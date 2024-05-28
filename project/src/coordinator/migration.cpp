#include "coordinator.h"

inline bool cmp_ascending(std::pair<unsigned int, double> &a,std::pair<unsigned int, double> &b) 
{
    return a.second < b.second;
};
inline bool cmp_descending(std::pair<unsigned int, double> &a,std::pair<unsigned int, double> &b) 
{
    return a.second > b.second;
};

namespace ECProject
{
    void Coordinator::do_migration_on_rack_level(double storage_bias_threshold, double network_bias_threshold)
    {
        double rack_avg_storage_cost, rack_avg_network_cost;
        double rack_storage_bias, rack_network_bias;
        compute_avg_cost_and_bias_on_rack_level(rack_avg_storage_cost, rack_avg_network_cost,
                                                rack_storage_bias, rack_network_bias);
        while(rack_storage_bias > storage_bias_threshold 
              && rack_network_bias > network_bias_threshold)
        {
            double node_avg_storage_cost, node_avg_network_cost;
            double node_storage_bias, node_network_bias;
            compute_avg_cost_and_bias_on_node_level(node_avg_storage_cost, node_avg_network_cost,
                                                    node_storage_bias, node_network_bias);
            // sort the racks by load in ascending order
            std::vector<std::pair<unsigned int, double>> sorted_racks;
            for (auto &rack : rack_table_) {
                double rack_storage_cost, rack_network_cost;
                compute_total_cost_for_rack(rack.second, rack_storage_cost, rack_network_cost);
                // to improve metric
                if(IF_NEW_LOAD_METRIC)
                {
                    alpha_ = beta_ + (rack_network_cost - rack_storage_cost) * (1 - beta_);
                }
                double combined_cost = (rack_storage_cost / rack_avg_storage_cost) * (1 - alpha_) + 
                                        (rack_network_cost / rack_avg_network_cost) * alpha_;
                sorted_racks.push_back({rack.first, combined_cost});
            }
            std::sort(sorted_racks.begin(), sorted_racks.end(), cmp_descending);
            
            int migration_times = gama_ * num_of_racks_;
            int rack_idx = 0;
            while(rack_idx < migration_times)
            {
                // in the source rack
                unsigned int src_rack_id = sorted_racks[rack_idx++].first;
                // sort the partitions by predicted cost in descending order, find out the partition with max cost
                std::unordered_map<unsigned int, std::vector<int>> blocks_in_stripes;
                find_out_blocks_in_rack(src_rack_id, blocks_in_stripes);
                int num_of_partitions = int(blocks_in_stripes.size());
                std::vector<int> num_of_data_blocks_each_par;
                std::vector<int> num_of_blocks_each_par;
                double avg_blocks = 0;
                double avg_data_blocks = 0;
                for(auto it = blocks_in_stripes.begin(); it != blocks_in_stripes.end(); it++)
                {
                    std::vector<int> &partition = it->second;
                    int num_of_blocks_in_partition = int(partition.size());
                    num_of_blocks_each_par.push_back(num_of_blocks_in_partition);
                    int cnt = 0;
                    for(int j = 0; j < num_of_blocks_in_partition; j++)
                    {
                        if(partition[j] < ec_schema_.k)
                            cnt++;
                    }
                    num_of_data_blocks_each_par.push_back(cnt);
                    avg_blocks += (double)num_of_blocks_in_partition;
                    avg_data_blocks += (double)cnt;
                }
                avg_blocks = avg_blocks / (double)num_of_partitions;
                avg_data_blocks = avg_data_blocks / (double)num_of_partitions;
                std::vector<std::pair<unsigned int, double>> prediction_cost_each_par;
                int index = 0;
                for (auto it = blocks_in_stripes.begin(); it != blocks_in_stripes.end(); it++, index++) {
                    double storage_cost = (double)num_of_blocks_each_par[index] / avg_blocks;
                    double network_cost = (double)num_of_data_blocks_each_par[index] / avg_data_blocks;
                    if(IF_NEW_LOAD_METRIC)
                    {
                        alpha_ = beta_;
                    }
                    double prediction_cost = storage_cost * (1 - alpha_) + network_cost * alpha_;   // to improve metric
                    prediction_cost_each_par.push_back({it->first, prediction_cost});
                }
                std::sort(prediction_cost_each_par.begin(), prediction_cost_each_par.end(), cmp_descending);
                int alternative_num = int(prediction_cost_each_par.size());
                int par_idx = 0;
                unsigned int stripe_id = prediction_cost_each_par[par_idx++].first;
                unsigned int dst_rack_id = num_of_racks_;
                while(true)
                {
                    // select a rack that does not place any blocks of this stripe, with lowest load as possible
                    for(int i = num_of_racks_ - 1; i > num_of_racks_ / 2; i--)
                    {
                        unsigned int rack_id = sorted_racks[i].first;
                        if(!if_stripe_in_rack(stripe_id, rack_id))
                        {
                            dst_rack_id = rack_id;
                            break;
                        }
                    }
                    if(dst_rack_id != num_of_racks_ || par_idx == alternative_num)
                        break;
                    stripe_id = prediction_cost_each_par[par_idx++].first;
                }
                if(dst_rack_id == num_of_racks_)
                {
                    continue;
                }
                // in the destination rack
                Rack &dst_rack = rack_table_[dst_rack_id];
                // sort the nodes by load in ascending order
                std::vector<std::pair<unsigned int, double>> sorted_nodes_in_each_rack;
                for (auto &node_id : dst_rack.nodes) {
                    Node &node = node_table_[node_id];
                    double node_storage_cost = node.storage_cost / node.storage;
                    double node_network_cost = node.network_cost / node.bandwidth;
                    // to improve metric
                    if(IF_NEW_LOAD_METRIC)
                    {
                        alpha_ = beta_ + (node_network_cost - node_storage_cost) * (1 - beta_);
                    }
                    double combined_cost = (node_storage_cost / node_avg_storage_cost) * (1 - alpha_) +
                                        (node_network_cost / node_avg_network_cost) * alpha_;
                    sorted_nodes_in_each_rack.push_back({node_id, combined_cost});
                }
                std::sort(sorted_nodes_in_each_rack.begin(), sorted_nodes_in_each_rack.end(), cmp_ascending);
                stripe_id = prediction_cost_each_par[0].first;
                std::vector<int> &chosen_partition = blocks_in_stripes[stripe_id];
                Stripe &stripe = stripe_table_[stripe_id];
                int k = (int)stripe.k;
                int g = (int)stripe.g;
                int num_of_blocks = int(chosen_partition.size());
                migration_plan migration_info;
                std::vector<int> migrated_blocks;
                std::vector<unsigned int> src_node_ids;
                std::vector<unsigned int> dst_node_ids;
                int node_idx = 0;
                // data block
                for(int i = 0; i < num_of_blocks; i++)
                {
                    int block_idx = chosen_partition[i];
                    if(block_idx < k)
                    {
                        int src_node_id = stripe.blocks2nodes[block_idx];
                        int dst_node_id = sorted_nodes_in_each_rack[node_idx++].first;
                        migrated_blocks.push_back(block_idx);
                        src_node_ids.push_back(src_node_id);
                        dst_node_ids.push_back(dst_node_id);
                        migration_info.block_sizes.push_back(stripe.block_size);
                        migration_info.block_ids.push_back(std::to_string(stripe_id) + "_" + std::to_string(block_idx));
                        migration_info.src_nodes.push_back({node_table_[src_node_id].node_ip, node_table_[src_node_id].node_port});
                        migration_info.dst_nodes.push_back({node_table_[dst_node_id].node_ip, node_table_[dst_node_id].node_port});
                    }
                }
                // global block
                for(int i = 0; i < num_of_blocks; i++)
                {
                    int block_idx = chosen_partition[i];
                    if(block_idx >= k && block_idx < k + g)
                    {
                        int src_node_id = stripe.blocks2nodes[block_idx];
                        int dst_node_id = sorted_nodes_in_each_rack[node_idx++].first;
                        migrated_blocks.push_back(block_idx);
                        src_node_ids.push_back(src_node_id);
                        dst_node_ids.push_back(dst_node_id);
                        migration_info.block_sizes.push_back(stripe.block_size);
                        migration_info.block_ids.push_back(std::to_string(stripe_id) + "_" + std::to_string(block_idx));
                        migration_info.src_nodes.push_back({node_table_[src_node_id].node_ip, node_table_[src_node_id].node_port});
                        migration_info.dst_nodes.push_back({node_table_[dst_node_id].node_ip, node_table_[dst_node_id].node_port});
                    }
                }
                // local block
                for(int i = 0; i < num_of_blocks; i++)
                {
                    int block_idx = chosen_partition[i];
                    if(block_idx >= k + g)
                    {
                        int src_node_id = stripe.blocks2nodes[block_idx];
                        int dst_node_id = sorted_nodes_in_each_rack[node_idx++].first;
                        migrated_blocks.push_back(block_idx);
                        src_node_ids.push_back(src_node_id);
                        dst_node_ids.push_back(dst_node_id);
                        migration_info.block_sizes.push_back(stripe.block_size);
                        migration_info.block_ids.push_back(std::to_string(stripe_id) + "_" + std::to_string(block_idx));
                        migration_info.src_nodes.push_back({node_table_[src_node_id].node_ip, node_table_[src_node_id].node_port});
                        migration_info.dst_nodes.push_back({node_table_[dst_node_id].node_ip, node_table_[dst_node_id].node_port});
                    }
                }

                // migration
                if(!IF_SIMULATION)
                {
                    std::string dst_proxy_ip = rack_table_[dst_rack_id].proxy_ip;
                    int dst_proxy_port = rack_table_[dst_rack_id].proxy_port;
                    async_simple::coro::syncAwait(proxies_[dst_proxy_ip + std::to_string(dst_proxy_port)]->call<&Proxy::migrate_blocks>(migration_info));
                }
                if(IF_DEBUG)
                {
                    std::cout << "Move partition: \n";
                    for(int i = 0; i < int(migrated_blocks.size()); i++)
                    {
                        std::cout << stripe_id << "_" << migrated_blocks[i] << " : N" 
                                << src_node_ids[i] << " -> N" << dst_node_ids[i] << std::endl;
                    }
                }
                // update meta data
                for(int i = 0; i < num_of_blocks; i++)
                {
                    int block_idx = migrated_blocks[i];
                    unsigned int src_node_id = src_node_ids[i];
                    unsigned int dst_node_id = dst_node_ids[i];
                    stripe.blocks2nodes[block_idx] = dst_node_id;
                    node_table_[src_node_id].stripes_blocks.erase(stripe_id);
                    node_table_[src_node_id].storage_cost -= 1;
                    node_table_[src_node_id].network_cost -= 1;
                    node_table_[dst_node_id].stripes_blocks[stripe_id] = block_idx;
                    node_table_[dst_node_id].storage_cost += 1;
                    node_table_[dst_node_id].network_cost += 1;
                }
            }
            double old_rack_storage_bias = rack_storage_bias;
            double old_rack_network_bias = rack_network_bias;
            compute_avg_cost_and_bias_on_rack_level(rack_avg_storage_cost, rack_avg_network_cost,
                                                    rack_storage_bias, rack_network_bias);
            if(old_rack_storage_bias < rack_storage_bias || old_rack_network_bias < rack_network_bias)
            {
                break;
            }
        }
    }

    void Coordinator::do_migration_on_node_level_inside_rack(double storage_bias_threshold, double network_bias_threshold)
    {
        double node_avg_storage_cost, node_avg_network_cost;
        double node_storage_bias, node_network_bias;
        compute_avg_cost_and_bias_on_node_level(node_avg_storage_cost, node_avg_network_cost,
                                                node_storage_bias, node_network_bias);
        while(node_storage_bias > storage_bias_threshold 
              && node_network_bias > network_bias_threshold)
        {
            // sort the nodes by load in descending order
            std::vector<std::pair<unsigned int, double>> sorted_nodes_in_each_rack;
            for (auto node : node_table_) {
                double node_storage_cost = node.second.storage_cost / node.second.storage;
                double node_network_cost = node.second.network_cost / node.second.bandwidth;
                // to improve metric
                if(IF_NEW_LOAD_METRIC)
                {
                    alpha_ = beta_ + (node_network_cost - node_storage_cost) * (1 - beta_);
                }
                double combined_cost = (node_storage_cost / node_avg_storage_cost) * (1 - alpha_) +
                                        (node_network_cost / node_avg_network_cost) * alpha_;
                sorted_nodes_in_each_rack.push_back({node.first, combined_cost});
                // std::cout << node.first << " " << node.second.storage_cost << " " << node.second.network_cost << " "
                //           << node_storage_cost << " " << node_network_cost << " " << node_avg_storage_cost << " "
                //           << node_avg_network_cost << " " << alpha_ << " " << combined_cost << std::endl;
            }
            std::sort(sorted_nodes_in_each_rack.begin(), sorted_nodes_in_each_rack.end(), cmp_descending);
            
            migration_plan migration_info;
            std::vector<int> migrated_blocks;
            std::vector<unsigned int> stripe_ids;
            std::vector<unsigned int> src_node_ids;
            std::vector<unsigned int> dst_node_ids;

            int block_nums = gama_ * (num_of_racks_ * num_of_nodes_per_rack_) / 5;
            int node_idx = 0;
            while(node_idx < block_nums)
            {
                unsigned int src_node_id = sorted_nodes_in_each_rack[node_idx++].first;
                // in the source node, randomly select a block
                size_t alternative_num = int(node_table_[src_node_id].stripes_blocks.size());
                my_assert(alternative_num > 0);
                std::vector<std::pair<unsigned int, double>> alternative_blocks;
                for(auto it = node_table_[src_node_id].stripes_blocks.begin(); 
                    it != node_table_[src_node_id].stripes_blocks.end(); it++)
                {
                    alternative_blocks.push_back(std::make_pair(it->first, (double)it->second));
                }
                sort(alternative_blocks.begin(), alternative_blocks.end(), cmp_ascending);
                auto iterator = alternative_blocks.begin();
                unsigned int stripe_id = iterator->first;
                int block_idx = (int)iterator->second;
                // find a destination node in the same rack
                unsigned int num_of_nodes = int(sorted_nodes_in_each_rack.size());
                unsigned int dst_node_id = num_of_nodes;
                while(true)
                {
                    // select a node that does not place any blocks of this stripe, with lowest load as possible
                    for(int i = num_of_nodes - 1; i > num_of_nodes / 2; i--)
                    {
                        unsigned int node_id = sorted_nodes_in_each_rack[i].first;
                        if(node_table_[src_node_id].map2rack == node_table_[node_id].map2rack)
                        {
                            if(!if_stripe_in_node(stripe_id, node_id))
                            {
                                dst_node_id = node_id;
                                break;
                            }
                        }
                    }
                    if(dst_node_id != num_of_nodes || --alternative_num)
                        break;
                    iterator++;
                    stripe_id = iterator->first;
                    block_idx = (int)iterator->second;
                }
                if(dst_node_id == num_of_nodes)
                {
                    continue;
                }
                migrated_blocks.push_back(block_idx);
                stripe_ids.push_back(stripe_id);
                src_node_ids.push_back(src_node_id);
                dst_node_ids.push_back(dst_node_id);
                migration_info.block_sizes.push_back(stripe_table_[stripe_id].block_size);
                migration_info.block_ids.push_back(std::to_string(stripe_id) + "_" + std::to_string(block_idx));
                migration_info.src_nodes.push_back(std::make_pair(node_table_[src_node_id].node_ip, node_table_[src_node_id].node_port));
                migration_info.dst_nodes.push_back(std::make_pair(node_table_[dst_node_id].node_ip, node_table_[dst_node_id].node_port));
            }

            if(migrated_blocks.size() > 0)
            {
                if(!IF_SIMULATION)
                {
                    // migration
                    unsigned int rack_id = node_table_[src_node_ids[0]].map2rack;
                    std::string chosen_proxy_ip = rack_table_[rack_id].proxy_ip;
                    int chosen_proxy_port = rack_table_[rack_id].proxy_port;
                    async_simple::coro::syncAwait(proxies_[chosen_proxy_ip + std::to_string(chosen_proxy_port)]->call<&Proxy::migrate_blocks>(migration_info));
                }
                if(IF_DEBUG)
                {
                    std::cout << "Move block: \n";
                    for(int i = 0; i < int(migrated_blocks.size()); i++)
                    {
                        std::cout << stripe_ids[i] << "_" << migrated_blocks[i] << " : N" 
                                << src_node_ids[i] << " -> N" << dst_node_ids[i] << std::endl;
                    }
                }

                // update meta data
                for(int i = 0; i < int(migrated_blocks.size()); i++)
                {
                    unsigned int stripe_id = stripe_ids[i];
                    int block_idx = migrated_blocks[i];
                    unsigned int src_node_id = src_node_ids[i];
                    unsigned int dst_node_id = dst_node_ids[i];
                    stripe_table_[stripe_id].blocks2nodes[block_idx] = dst_node_id;
                    node_table_[src_node_id].stripes_blocks.erase(stripe_id);
                    node_table_[src_node_id].storage_cost -= 1;
                    node_table_[src_node_id].network_cost -= 1;
                    node_table_[dst_node_id].stripes_blocks[stripe_id] = block_idx;
                    node_table_[dst_node_id].storage_cost += 1;
                    node_table_[dst_node_id].network_cost += 1;
                }
            }

            double old_node_storage_bias = node_storage_bias;
            double old_node_network_bias = node_network_bias;
            compute_avg_cost_and_bias_on_node_level(node_avg_storage_cost, node_avg_network_cost,
                                                node_storage_bias, node_network_bias);
            if(old_node_storage_bias <= node_storage_bias || old_node_network_bias <= node_network_bias)
            {
                break;
            }
        }
    }
}