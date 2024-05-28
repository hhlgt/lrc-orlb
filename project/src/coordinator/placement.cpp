#include "coordinator.h"

inline bool cmp_ascending(std::pair<unsigned int, double> &a,std::pair<unsigned int, double> &b) 
{
    return a.second < b.second;
};
inline bool cmp_ascending_v2(std::pair<int, int> &a,std::pair<int, int> &b) 
{
    return a.second < b.second;
};
inline bool cmp_descending(std::pair<int, int> &a,std::pair<int, int> &b) 
{
    return a.second > b.second;
};
inline bool cmp_descending_v2(std::pair<std::vector<int>, double> &a,
                              std::pair<std::vector<int>, double> &b) {
  return a.second > b.second;
};

namespace ECProject
{
    void Coordinator::generate_placement_for_LRC(unsigned int stripe_id)
    {
        Stripe &stripe = stripe_table_[stripe_id];
        int k = stripe.k;
        int g = stripe.g;
        int l = stripe.l;
        
        std::vector<std::vector<int>> partition_plan;
        if(ec_schema_.placement_type == PlacementType::flat)
        {
            partition_plan = lrc_partition_strategy_flat(k, g, l);
        }
        else if(ec_schema_.placement_type == PlacementType::ran)
        {
            if(ec_schema_.ft_level == FaultToleranceLevel::random_region)
            {
                partition_plan = lrc_partition_strategy_random_random(k, g, l);
            }
            else if(ec_schema_.ft_level == FaultToleranceLevel::single_region)
            {
                partition_plan = lrc_partition_strategy_random_single_region(k, g, l);
            }
            else if(ec_schema_.ft_level == FaultToleranceLevel::two_regions)
            {
                partition_plan = lrc_partition_strategy_random_t_regions(k, g, l, 2);
            }
            else if(ec_schema_.ft_level == FaultToleranceLevel::three_regions)
            {
                partition_plan = lrc_partition_strategy_random_t_regions(k, g, l, 3);
            }
            else if(ec_schema_.ft_level == FaultToleranceLevel::four_regions)
            {
                partition_plan = lrc_partition_strategy_random_t_regions(k, g, l, 4);
            }
            else
            {
                my_assert(false);
            }
        }
        else if(ec_schema_.placement_type == PlacementType::ecwide) // only for Azu_LRC
        {
            if(ec_schema_.ft_level == FaultToleranceLevel::single_region)
            {
                partition_plan = lrc_partition_strategy_ecwide_single_region(k, g, l);
            }
            else
            {
                my_assert(false);
            }
        }
        else if(ec_schema_.placement_type == PlacementType::opt)
        {
            if(ec_schema_.ft_level == FaultToleranceLevel::single_region)
            {
                partition_plan = lrc_partition_strategy_opt_single_region(stripe.ec_type, k, g, l);
            }
            else if(ec_schema_.ft_level == FaultToleranceLevel::two_regions)
            {
                partition_plan = lrc_partition_strategy_opt_t_regions(stripe.ec_type, k, g, l, 2);
            }
            else if(ec_schema_.ft_level == FaultToleranceLevel::three_regions)
            {
                partition_plan = lrc_partition_strategy_opt_t_regions(stripe.ec_type, k, g, l, 3);
            }
            else if(ec_schema_.ft_level == FaultToleranceLevel::four_regions)
            {
                partition_plan = lrc_partition_strategy_opt_t_regions(stripe.ec_type, k, g, l, 4);
            }
            else
            {
                my_assert(false);
            }
        }
        else
        {
            my_assert(false);
        }

        if(IF_DEBUG)
        {
            std::cout << "Partitions: " << std::endl;
            for(auto i = 0; i < partition_plan.size(); i++)
            {
                std::cout << "[" << i << "] ";
                for(auto j = 0; j < partition_plan[i].size(); j++)
                {
                    std::cout << partition_plan[i][j] << " ";
                }
                std::cout << std::endl;
            }
        }

        if(ec_schema_.ns_type == NodeSelectionType::random)
        {
            select_by_random(partition_plan, stripe_id);
        }
        else if(ec_schema_.ns_type == NodeSelectionType::load_balance)
        {
            select_by_load(partition_plan, stripe_id);
        }
        else
        {
            my_assert(false);
        }

        for (auto node_id : stripe.blocks2nodes) {
            node_table_[node_id].network_cost += 1;
            node_table_[node_id].storage_cost += 1;
        }
        
        if(IF_DEBUG)
        {
            std::unordered_map<unsigned int, std::vector<int>> blocks_in_racks;
            find_out_stripe_placement(stripe_id, blocks_in_racks);
        }
    }

    std::vector<std::vector<int>> Coordinator::lrc_partition_strategy_flat(int k, int g, int l)
    {
        int n = k + g + l;
        std::vector<std::vector<int>> partition_plan;
        for(int i = 0; i < n; i++)
        {
            partition_plan.push_back({i});  // place a block in a seperate region
        }
        return partition_plan;
    }

    std::vector<std::vector<int>> Coordinator::lrc_partition_strategy_random_random(int k, int g, int l)
    {
        int n = k + g + l;
        std::vector<int> blocks;
        for(int i = 0; i < n; i++)
        {
            blocks.push_back(i);
        } 
        std::vector<std::vector<int>> partition_plan;
        int cumulative_size = 0;
        int num_of_left_blocks = n;
        while(cumulative_size < n)
        {
            int random_partition_size = random_range(1, g + 1);  // at least subject to single-region fault tolerance
            int partition_size = std::min(random_partition_size, n - cumulative_size);
            std::vector<int> partition;
            for(int i = 0; i < partition_size; i++)
            {
                int ran_idx = random_index(num_of_left_blocks);
                int block_idx = blocks[ran_idx];
                partition.push_back(block_idx);
                auto it = std::find(blocks.begin(), blocks.end(), block_idx);
                blocks.erase(it);
            }
            partition_plan.push_back(partition);
            cumulative_size += partition_size;
        }
        return partition_plan;
    }
    std::vector<std::vector<int>> Coordinator::lrc_partition_strategy_random_single_region(int k, int g, int l)
    {
        int n = k + g + l;
        std::vector<int> blocks;
        for(int i = 0; i < n; i++)
        {
            blocks.push_back(i);
        } 
        std::vector<std::vector<int>> partition_plan;
        int cumulative_size = 0;
        int num_of_left_blocks = n;
        while(cumulative_size < n)
        {
            int partition_size = g + 1;  // place random g + 1 blocks in a seperate region
            partition_size = std::min(partition_size, n - cumulative_size);
            std::vector<int> partition;
            for(int i = 0; i < partition_size; i++)
            {
                int ran_idx = random_index(num_of_left_blocks);
                int block_idx = blocks[ran_idx];
                partition.push_back(block_idx);
                auto it = std::find(blocks.begin(), blocks.end(), block_idx);
                blocks.erase(it);
                num_of_left_blocks -= 1;
            }
            partition_plan.push_back(partition);
            cumulative_size += partition_size;
        }
        return partition_plan;
    }
    std::vector<std::vector<int>> Coordinator::lrc_partition_strategy_random_t_regions(int k, int g, int l, int t)
    {
        my_assert(t <= g + 1);
        std::vector<std::vector<int>> partition_plan;
        int base_size = (g + 1) / t;
        std::vector<int> stripe_blocks;
        int n = k + g + l;
        for(int i = 0; i < n; i++)
        {
            stripe_blocks.push_back(i);
        }
        std::random_shuffle(stripe_blocks.begin(), stripe_blocks.end());
        int index = 0;
        while(index < n)
        {
            std::vector<int> partition;
            int partition_size = std::min(base_size, n - index);
            for(int i = 0; i < partition_size; i++)
            {
                partition.push_back(stripe_blocks[index++]);
            }
            partition_plan.push_back(partition);
        }
        return partition_plan;
    }

    // ecwide only for Azu_LRC
    std::vector<std::vector<int>> Coordinator::lrc_partition_strategy_ecwide_single_region(int k, int g, int l)
    {
        std::vector<std::vector<int>> partition_plan;
        int r = return_group_size(Azu_LRC, k, g, l);

        std::vector<std::vector<int>> stripe_groups;
        generate_stripe_information_for_Azu_LRC(k, l, g, stripe_groups);

        std::vector<std::vector<int>> remaining_groups;
        for(int i = 0; i < l; i++)  // partition blocks in every local group
        {
            std::vector<int> local_group = stripe_groups[i];
            int group_size = int(local_group.size());
            // every g + 1 blocks a partition
            for(int j = 0; j < group_size; j += g + 1)
            {
                if(j + g + 1 > group_size) // derive the remain group
                {
                    std::vector<int> remain_group;
                    for(int ii = j; ii < group_size; ii++)
                    {
                        remain_group.push_back(local_group[ii]);
                    }
                    remaining_groups.push_back(remain_group);
                    break;
                }
                std::vector<int> partition;
                for(int ii = j; ii < j + g + 1; ii++)
                {
                    partition.push_back(local_group[ii]);
                }
                partition_plan.push_back(partition);
            }
        }

        int theta = l;
        if((r + 1) % (g + 1) > 1)
        {
            theta = g / ((r + 1) % (g + 1) - 1);
        }
        int remaining_groups_num = int(remaining_groups.size());
        for(int i = 0; i < remaining_groups_num; i += theta) // organize every θ remaining groups
        {
            std::vector<int> partition;
            for(int j = i; j < i + theta && j < remaining_groups_num; j++)
            {
                std::vector<int> remain_group = remaining_groups[j];
                int remain_block_num = int(remain_group.size());
                for(int ii = 0; ii < remain_block_num; ii++)
                {
                    partition.push_back(remain_group[ii]);
                }
            }
            partition_plan.push_back(partition);
        }

        // organize the global parity blocks in a single partition
        std::vector<int> partition;
        for(int i = k; i < k + g; i++)
        {
            partition.push_back(i);
        }
        partition_plan.push_back(partition);

        return partition_plan;
    }

    std::vector<std::vector<int>> Coordinator::lrc_partition_strategy_opt_single_region(ECTYPE lrc_type, int k, int g, int l)
    {
        std::vector<std::vector<int>> partition_plan;
        if(lrc_type == ECTYPE::Azu_LRC)
        {
            int r = return_group_size(Azu_LRC, k, g, l);
            std::vector<std::vector<int>> stripe_groups;
            generate_stripe_information_for_Azu_LRC(k, l, g, stripe_groups);
            
            std::vector<std::vector<int>> remaining_groups;
            for(int i = 0; i < l; i++)  // partition blocks in every local group
            {
                std::vector<int> local_group = stripe_groups[i];
                int group_size = int(local_group.size());
                // every g + 1 blocks a partition
                for(int j = 0; j < group_size; j += g + 1)
                {
                    if(j + g + 1 > group_size) // derive the remain group
                    {
                        std::vector<int> remain_group;
                        for(int ii = j; ii < group_size; ii++)
                        {
                            remain_group.push_back(local_group[ii]);
                        }
                        remaining_groups.push_back(remain_group);
                        break;
                    }
                    std::vector<int> partition;
                    for(int ii = j; ii < j + g + 1; ii++)
                    {
                        partition.push_back(local_group[ii]);
                    }
                    partition_plan.push_back(partition);
                }
            }
            int theta = l;
            if((r + 1) % (g + 1) > 1)
            {
                theta = g / ((r + 1) % (g + 1) - 1);
            }
            int remaining_groups_num = int(remaining_groups.size());
            for(int i = 0; i < remaining_groups_num; i += theta) // organize every θ remaining groups
            {
                std::vector<int> partition;
                for(int j = i; j < i + theta && j < remaining_groups_num; j++)
                {
                    std::vector<int> remain_group = remaining_groups[j];
                    int remain_block_num = int(remain_group.size());
                    for(int ii = 0; ii < remain_block_num; ii++)
                    {
                        partition.push_back(remain_group[ii]);
                    }
                }
                partition_plan.push_back(partition);
            }
            // calculate free space
            std::vector<std::pair<int, int>> space_left_in_each_partition;
            int sum_left_space = 0;
            for(int i = 0; i < (int)partition_plan.size(); i++)
            {
                int num_of_blocks = (int)partition_plan[i].size();
                int num_of_group = 0;
                for(int j = 0; j < num_of_blocks; j++)
                {
                    if(partition_plan[i][j] >= k + g)
                    {
                        num_of_group += 1;
                    }
                }
                if(num_of_group == 0)
                {
                    num_of_group = 1;
                }
                int max_space = g + num_of_group;
                int left_space = max_space - num_of_blocks;
                space_left_in_each_partition.push_back({i, left_space});
                sum_left_space += left_space;
            }
            // place the global parity blocks
            int left_g = g;
            int global_idx = k;
            if(sum_left_space >= g) // insert to partitions with free space
            {
                std::sort(space_left_in_each_partition.begin(), space_left_in_each_partition.end(), cmp_descending);
                for (auto i = 0; i < space_left_in_each_partition.size() && left_g > 0; i++) {
                    if (space_left_in_each_partition[i].second > 0) {
                        int j = space_left_in_each_partition[i].first;
                        int left_space = space_left_in_each_partition[i].second;
                        if (left_g >= left_space) {
                            left_g -= left_space;
                        } else {
                            left_space = left_g;
                            left_g -= left_g;
                        }
                        while(left_space--)
                        {
                            partition_plan[j].push_back(global_idx++);
                        }
                    }
                }
                my_assert(left_g == 0);
            }
            else    // a seperate new partition
            {
                std::vector<int> partition;
                while(global_idx < k + g)
                {
                    partition.push_back(global_idx++);
                }
                partition_plan.push_back(partition);
            }
        }
        return partition_plan;
    }

    std::vector<std::vector<int>> Coordinator::lrc_partition_strategy_opt_t_regions(ECTYPE lrc_type, int k, int g, int l, int t)
    {
        my_assert(t <= g + 1);
        std::vector<std::vector<int>> partition_plan;
        if(lrc_type == ECTYPE::Azu_LRC)
        {
            int r = return_group_size(Azu_LRC, k, g, l);
            std::vector<std::vector<int>> stripe_groups;
            generate_stripe_information_for_Azu_LRC(k, l, g, stripe_groups);
            std::vector<std::vector<int>> remaining_groups;
            for(int i = 0; i < l; i++)  // partition blocks in every local group
            {
                std::vector<int> local_group = stripe_groups[i];
                int group_size = int(local_group.size());
                // every g + 1 blocks a partition
                for(int j = 0; j < group_size; j += (g + 1) / t)
                {
                    if(j + (g + 1) / t > group_size) // derive the remain group
                    {
                        std::vector<int> remain_group;
                        for(int ii = j; ii < group_size; ii++)
                        {
                            remain_group.push_back(local_group[ii]);
                        }
                        remaining_groups.push_back(remain_group);
                        break;
                    }
                    std::vector<int> partition;
                    for(int ii = j; ii < j + (g + 1) / t; ii++)
                    {
                        partition.push_back(local_group[ii]);
                    }
                    partition_plan.push_back(partition);
                }
            }
            
            int theta = l;
            if((r + 1) % ((g + 1) / t) > 1)
            {
                theta = g / (t * ((r + 1) % ((g + 1) / t) - 1));
            }
            int remaining_groups_num = int(remaining_groups.size()); 
            for(int i = 0; i < remaining_groups_num; i += theta) // organize every θ remaining groups
            {
                std::vector<int> partition;
                for(int j = i; j < i + theta && j < remaining_groups_num; j++)
                {
                    std::vector<int> remain_group = remaining_groups[j];
                    int remain_block_num = int(remain_group.size());
                    for(int ii = 0; ii < remain_block_num; ii++)
                    {
                        partition.push_back(remain_group[ii]);
                    }
                }
                partition_plan.push_back(partition);
            }
            // calculate free space
            std::vector<std::pair<int, int>> space_left_in_each_partition;
            int sum_left_space = 0;
            for(auto i = 0; i < partition_plan.size(); i++)
            {
                int num_of_blocks = (int)partition_plan[i].size();
                int num_of_group = 0;
                for(auto j = 0; j < num_of_blocks; j++)
                {
                    if(partition_plan[i][j] >= k + g)
                    {
                        num_of_group += 1;
                    }
                }
                if(num_of_group == 0)
                {
                    num_of_group = 1;
                }
                int max_space = g / t + num_of_group;
                int left_space = max_space - num_of_blocks;
                space_left_in_each_partition.push_back({i, left_space});
                sum_left_space += left_space;
            }
            // place the global parity blocks
            int left_g = g;
            int cond_g = g % ((g + 1) / t);
            int global_idx = k;
            if(sum_left_space >= cond_g) // insert to partitions with free space
            {
                std::sort(space_left_in_each_partition.begin(), space_left_in_each_partition.end(), cmp_descending);
                for (auto i = 0; i < space_left_in_each_partition.size() && left_g > 0; i++) {
                    if (space_left_in_each_partition[i].second > 0) {
                        int j = space_left_in_each_partition[i].first;
                        int left_space = space_left_in_each_partition[i].second;
                        if (left_g >= left_space) {
                            left_g -= left_space;
                        } else {
                            left_space = left_g;
                            left_g -= left_g;
                        }
                        while(left_space--)
                        {
                            partition_plan[j].push_back(global_idx++);
                        }
                    }
                }
            }
            if(left_g > 0)
            {
                while(global_idx < k + g)
                {
                    std::vector<int> partition;
                    int cnt = 0;
                    while(global_idx < k + g && cnt < (g + 1) / t)
                    {
                        partition.push_back(global_idx++);
                        cnt++;
                    }
                    partition_plan.push_back(partition);
                }
            }
        }
        return partition_plan;
    }

    // on rack level
    void Coordinator::select_by_random(std::vector<std::vector<int>> &partition_plan, unsigned int stripe_id)
    {
        Stripe &stripe = stripe_table_[stripe_id];
        int n = stripe.k + stripe.g + stripe.l;
        for(unsigned int i = 0; i < n; i++)
        {
            stripe.blocks2nodes.push_back(i);
        }

        std::vector<unsigned int> free_racks;
        for(unsigned int i = 0; i < num_of_racks_; i++)
        {
            free_racks.push_back(i);
        }

        // place each partition into a seperate rack
        size_t free_racks_len = free_racks.size();
        int num_of_partitions = int(partition_plan.size());
        for(int i = 0; i < num_of_partitions; i++)
        {
            my_assert(free_racks_len);
            // randomly select a rack
            int rack_idx = random_index(free_racks_len);
            unsigned int rack_id = free_racks[rack_idx];
            Rack &rack = rack_table_[rack_id];
            int num_of_nodes_in_rack = int(rack.nodes.size());
            std::vector<unsigned int> free_nodes;
            for(int j = 0; j < num_of_nodes_in_rack; j++)
            {
                free_nodes.push_back(rack.nodes[j]);
            }
            int num_of_block_in_partition = int(partition_plan[i].size());
            size_t free_nodes_len = int(free_nodes.size());
            for(int j = 0; j < num_of_block_in_partition; j++)
            {
                my_assert(free_nodes_len);
                // randomly select a node
                int node_idx = random_index(free_nodes_len);
                unsigned int node_id = free_nodes[node_idx];
                int block_idx = partition_plan[i][j];
                stripe.blocks2nodes[block_idx] = node_id;
                node_table_[node_id].stripes_blocks[stripe_id] = block_idx;
                // remove the chosen node from the free list
                auto it_n = std::find(free_nodes.begin(), free_nodes.end(), node_id);
                free_nodes.erase(it_n);
                free_nodes_len--;
            }
            // remove the chosen rack from the free list
            auto it_r = std::find(free_racks.begin(), free_racks.end(), rack_id);
            free_racks.erase(it_r);
            free_racks_len--;
        }
    }

    void Coordinator::select_by_load(std::vector<std::vector<int>> &partition_plan, unsigned int stripe_id)
    {
        Stripe &stripe = stripe_table_[stripe_id];
        int k = stripe.k;
        int g = stripe.g;
        int n = stripe.k + stripe.g + stripe.l;
        for(unsigned int i = 0; i < n; i++)
        {
            stripe.blocks2nodes.push_back(i);
        }
        std::vector<int> num_of_data_blocks_each_par;
        std::vector<int> num_of_blocks_each_par;
        int num_of_partitions = int(partition_plan.size());
        for(int i = 0; i < num_of_partitions; i++)
        {
            int num_of_blocks_in_partition = int(partition_plan[i].size());
            num_of_blocks_each_par.push_back(num_of_blocks_in_partition);
            int cnt = 0;
            for(int j = 0; j < num_of_blocks_in_partition; j++)
            {
                if(partition_plan[i][j] < k)
                    cnt++;
            }
            num_of_data_blocks_each_par.push_back(cnt);
        }

        double avg_blocks = 0;
        double avg_data_blocks = 0;
        for (auto i = 0; i < num_of_partitions; i++) 
        {
            avg_blocks += (double)num_of_blocks_each_par[i];
            avg_data_blocks += (double)num_of_data_blocks_each_par[i];
        }
        avg_blocks = avg_blocks / (double)num_of_partitions;
        avg_data_blocks = avg_data_blocks / (double)num_of_partitions;

        std::vector<std::pair<std::vector<int>, double>> prediction_cost_each_par;
        for (auto i = 0; i < num_of_partitions; i++) {
            double storage_cost = (double)num_of_blocks_each_par[i] / avg_blocks;
            double network_cost = (double)num_of_data_blocks_each_par[i] / avg_data_blocks;
            double prediction_cost = storage_cost * (1 - beta_) + network_cost * beta_;   
            prediction_cost_each_par.push_back({partition_plan[i], prediction_cost});
        }

        // sort the partition_plan by predicted cost in descending order
        std::sort(prediction_cost_each_par.begin(), prediction_cost_each_par.end(), cmp_descending_v2);

        double node_avg_storage_cost, node_avg_network_cost;
        double rack_avg_storage_cost, rack_avg_network_cost;
        compute_avg_cost_for_each_node_and_rack(node_avg_storage_cost, node_avg_network_cost, rack_avg_storage_cost, rack_avg_network_cost);
        // sort the racks by load in ascending order
        std::vector<std::pair<unsigned int, double>> sorted_racks;
        for (auto &rack : rack_table_) {
            double rack_storage_cost, rack_network_cost;
            compute_total_cost_for_rack(rack.second, rack_storage_cost, rack_network_cost);
            // to optimize
            if(IF_NEW_LOAD_METRIC)
            {
                alpha_ = beta_ + (rack_network_cost - rack_storage_cost) * (1 - beta_);
            }
            double combined_cost = (rack_storage_cost / rack_avg_storage_cost) * (1 - alpha_) + 
                                    (rack_network_cost / rack_avg_network_cost) * alpha_;
            sorted_racks.push_back({rack.first, combined_cost});
        }
        std::sort(sorted_racks.begin(), sorted_racks.end(), cmp_ascending);

        // for each partition in a seperate rack
        int rack_idx = 0;
        for(int i = 0; i < num_of_partitions; i++)
        {
            Rack &rack = rack_table_[sorted_racks[rack_idx++].first];

            // sort the nodes by load in ascending order
            std::vector<std::pair<unsigned int, double>> sorted_nodes_in_each_rack;
            for (auto &node_id : rack.nodes) {
                Node &node = node_table_[node_id];
                double node_storage_cost = node.storage_cost / node.storage;
                double node_network_cost = node.network_cost / node.bandwidth;
                // to optimize
                if(IF_NEW_LOAD_METRIC)
                {
                    alpha_ = beta_ + (node_network_cost - node_storage_cost) * (1 - beta_);
                }
                double combined_cost = (node_storage_cost / node_avg_storage_cost) * (1 - alpha_) +
                                       (node_network_cost / node_avg_network_cost) * alpha_;
                sorted_nodes_in_each_rack.push_back({node_id, combined_cost});
            }
            std::sort(sorted_nodes_in_each_rack.begin(), sorted_nodes_in_each_rack.end(), cmp_ascending);

            // select and place
            int node_idx = 0;
            std::vector<int> &partition = prediction_cost_each_par[i].first;
            int num_of_blocks = int(partition.size());
            // data block
            for(int j = 0; j < num_of_blocks; j++)
            {
                if(partition[j] < k)
                {
                    unsigned int node_id = sorted_nodes_in_each_rack[node_idx++].first;
                    stripe.blocks2nodes[partition[j]] = node_id;
                    node_table_[node_id].stripes_blocks[stripe_id] = partition[j];
                }
            }
            // global block
            // data block
            for(int j = 0; j < num_of_blocks; j++)
            {
                if(partition[j] >= k && partition[j] < k + g)
                {
                    unsigned int node_id = sorted_nodes_in_each_rack[node_idx++].first;
                    stripe.blocks2nodes[partition[j]] = node_id;
                    node_table_[node_id].stripes_blocks[stripe_id] = partition[j];
                }
            }
            // local block
            for(int j = 0; j < num_of_blocks; j++)
            {
                if(partition[j] >= k + g)
                {
                    unsigned int node_id = sorted_nodes_in_each_rack[node_idx++].first;
                    stripe.blocks2nodes[partition[j]] = node_id;
                    node_table_[node_id].stripes_blocks[stripe_id] = partition[j];
                }
            }
        }
    }

    unsigned int Coordinator::select_a_node_by_load(std::vector<unsigned int> node_lists)
    {
        double node_avg_storage_cost, node_avg_network_cost;
        double node_storage_bias, node_network_bias;
        compute_avg_cost_and_bias_on_node_level(node_avg_storage_cost, node_avg_network_cost,
                                                node_storage_bias, node_network_bias);
        // sort the nodes by load in ascending order
        std::vector<std::pair<unsigned int, double>> sorted_nodes_in_each_rack;
        for (auto &node_id : node_lists) {
            Node &node = node_table_[node_id];
            double node_storage_cost = node.storage_cost / node.storage;
            double node_network_cost = node.network_cost / node.bandwidth;
            // to optimize
            if(IF_NEW_LOAD_METRIC)
            {
                alpha_ = beta_ + (node_network_cost - node_storage_cost) * (1 - beta_);
            }
            double combined_cost = (node_storage_cost / node_avg_storage_cost) * (1 - alpha_) +
                                       (node_network_cost / node_avg_network_cost) * alpha_;
            sorted_nodes_in_each_rack.push_back({node_id, combined_cost});
        }
        std::sort(sorted_nodes_in_each_rack.begin(), sorted_nodes_in_each_rack.end(), cmp_ascending);
        return sorted_nodes_in_each_rack[0].first;
    }

    void Coordinator::compute_avg_cost_for_each_node_and_rack(
                        double &node_avg_storage_cost, double &node_avg_network_cost,
                        double &rack_avg_storage_cost, double &rack_avg_network_cost)
    {
        for (auto &node : node_table_) {
            double storage_cost = node.second.storage_cost / node.second.storage;
            double network_cost = node.second.network_cost / node.second.bandwidth;
            node_avg_storage_cost += storage_cost;
            node_avg_network_cost += network_cost;
        }
        node_avg_storage_cost /= (double)node_table_.size();
        node_avg_network_cost /= (double)node_table_.size();

        for (auto &rack : rack_table_) {
            double storage_cost = 0, network_cost = 0;
            compute_total_cost_for_rack(rack.second, storage_cost, network_cost);
            rack_avg_storage_cost += storage_cost;
            rack_avg_network_cost += network_cost;
        }
        rack_avg_storage_cost /= (double)rack_table_.size();
        rack_avg_network_cost /= (double)rack_table_.size();
    }

    void Coordinator::compute_total_cost_for_rack(Rack &rack, double &storage_cost, double &network_cost)
    {
        double all_storage = 0, all_bandwidth = 0;
        double all_storage_cost = 0, all_network_cost = 0;
        for (auto i = 0; i < rack.nodes.size(); i++) {
            unsigned int node_id = rack.nodes[i];
            Node &node = node_table_[node_id];
            all_storage += node.storage;
            all_bandwidth += node.bandwidth;
            all_storage_cost += node.storage_cost;
            all_network_cost += node.network_cost;
        }
        storage_cost = all_storage_cost / all_storage;
        network_cost = all_network_cost / all_bandwidth;
    }
    
    void Coordinator::compute_avg_cost_and_bias_on_node_level(
                        double &node_avg_storage_cost, double &node_avg_network_cost,
                        double &node_storage_bias, double &node_network_bias)
    {
        double node_max_storage_cost = 0, node_max_network_cost = 0;
        double node_min_storage_cost = 10000, node_min_network_cost = 10000;
        for (auto &node : node_table_) {
            double storage_cost = node.second.storage_cost / node.second.storage;
            double network_cost = node.second.network_cost / node.second.bandwidth;
            node_avg_storage_cost += storage_cost;
            node_avg_network_cost += network_cost;
            if(node_max_storage_cost < storage_cost)
                node_max_storage_cost = storage_cost;
            if(node_max_network_cost < network_cost)
                node_max_network_cost = network_cost;
            if(node_min_storage_cost > storage_cost)
                node_min_storage_cost = storage_cost;
            if(node_min_network_cost > network_cost)
                node_min_network_cost = network_cost;
        }
        node_avg_storage_cost /= (double)node_table_.size();
        node_avg_network_cost /= (double)node_table_.size();
        node_storage_bias = std::max(node_max_storage_cost - node_avg_storage_cost, 
                            node_avg_storage_cost - node_min_storage_cost) / node_avg_storage_cost;
        node_network_bias = std::max(node_max_network_cost - node_avg_network_cost, 
                            node_avg_network_cost - node_min_network_cost) / node_avg_network_cost;
    }
    void Coordinator::compute_avg_cost_and_bias_on_rack_level(
                        double &rack_avg_storage_cost, double &rack_avg_network_cost,
                        double &rack_storage_bias, double &rack_network_bias)
    {
        double rack_max_storage_cost = 0, rack_max_network_cost = 0;
        double rack_min_storage_cost = 10000, rack_min_network_cost = 10000;
        for (auto &rack : rack_table_) {
            double storage_cost = 0, network_cost = 0;
            compute_total_cost_for_rack(rack.second, storage_cost, network_cost);
            rack_avg_storage_cost += storage_cost;
            rack_avg_network_cost += network_cost;
            if(rack_max_storage_cost < storage_cost)
                rack_max_storage_cost = storage_cost;
            if(rack_max_network_cost < network_cost)
                rack_max_network_cost = network_cost;
            if(rack_min_storage_cost > storage_cost)
                rack_min_storage_cost = storage_cost;
            if(rack_min_network_cost > network_cost)
                rack_min_network_cost = network_cost;
        }
        rack_avg_storage_cost /= (double)rack_table_.size();
        rack_avg_network_cost /= (double)rack_table_.size();
        rack_storage_bias = std::max(rack_max_storage_cost - rack_avg_storage_cost, 
                            rack_avg_storage_cost - rack_min_storage_cost) / rack_avg_storage_cost;
        rack_network_bias = std::max(rack_max_network_cost - rack_avg_network_cost, 
                            rack_avg_network_cost - rack_min_network_cost) / rack_avg_network_cost;
    }
}
