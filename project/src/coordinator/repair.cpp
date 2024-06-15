#include "coordinator.h"

namespace ECProject
{
    void Coordinator::do_repair(std::vector<unsigned int> failed_ids, int stripe_id, 
                                double &repair_time, double &decoding_time, int &cross_rack_num)
    {
        struct timeval start_time, end_time;

        std::unordered_map<unsigned int, std::vector<int>> failures_map;
        std::unordered_map<unsigned int, ECProject::FailureType> failures_type;
        check_out_failures(stripe_id, failed_ids, failures_map, failures_type);

        for(auto it = failures_map.begin(); it != failures_map.end(); it++)
        {
            int t_stripe_id = it->first;
            auto it_ = failures_type.find(t_stripe_id);
            ECProject::FailureType ft = it_->second;
            if(IF_DEBUG)
            {
                std::cout << std::endl;
                std::cout << "[Type " << ft << "] Failed Stripe " << t_stripe_id  << " (f" << (it->second).size() << ") : ";
                for(auto t_it = (it->second).begin(); t_it != (it->second).end(); t_it++)
                {
                    std::cout << *t_it << " ";
                }
                std::cout << std::endl;
            }
            std::vector<main_repair_plan> main_repairs;
            std::vector<std::vector<help_repair_plan>> help_repairs;
            bool flag = true;
            if(ec_schema_.ec_type == ECTYPE::Azu_LRC)
            {
                if(ft == FailureType::single_block)
                {
                    int failed_block_idx = (it->second)[0];
                    if(failed_block_idx >= ec_schema_.k && failed_block_idx < ec_schema_.k + ec_schema_.g)
                    {
                        flag = generate_plan_for_a_local_or_global_repair_LRC(main_repairs, help_repairs, t_stripe_id, it->second, false);
                    }
                    else
                    {
                        flag = generate_plan_for_a_local_or_global_repair_LRC(main_repairs, help_repairs, t_stripe_id, it->second, true);
                    }
                }
                else
                {
                    flag = generate_repair_plan_for_multi_blocks_LRC(main_repairs, help_repairs, t_stripe_id, it->second);
                }
            }

            if(IF_DEBUG)
            {
                std::cout << "Finish generate repair plan." << std::endl;
            }
            
            
            auto lock_ptr = std::make_shared<std::mutex>();
            auto main_decoding_time_ptr = std::make_shared<std::vector<double>>();
            auto help_decoding_time_ptr = std::make_shared<std::vector<double>>();

            auto send_main_repair_plan = [this, main_repairs, lock_ptr, main_decoding_time_ptr](int i, int main_rack_id) mutable
            {
                std::string chosen_proxy = rack_table_[main_rack_id].proxy_ip + std::to_string(rack_table_[main_rack_id].proxy_port);
                auto main_decoding_time = async_simple::coro::syncAwait(proxies_[chosen_proxy]->call<&Proxy::main_repair>(main_repairs[i])).value();
                lock_ptr->lock();
                main_decoding_time_ptr->push_back(main_decoding_time);
                lock_ptr->unlock();
                if (IF_DEBUG)
                {
                    std::cout << "Selected main proxy " << chosen_proxy << " of Rack" << main_rack_id 
                              << ". Decoding time : " << main_decoding_time << std::endl;
                }
            };

            auto send_help_repair_plan = [this, help_repairs, lock_ptr, help_decoding_time_ptr](int i, int j, std::string proxy_ip, int proxy_port) mutable
            {
                std::string chosen_proxy = proxy_ip + std::to_string(proxy_port);
                auto help_decoding_time = async_simple::coro::syncAwait(proxies_[chosen_proxy]->call<&Proxy::help_repair>(help_repairs[i][j])).value();
                lock_ptr->lock();
                help_decoding_time_ptr->push_back(help_decoding_time);
                lock_ptr->unlock();
                if (IF_DEBUG)
                {
                    std::cout << "Selected help proxy " << chosen_proxy 
                              << ". Decoding time : " << help_decoding_time << std::endl;
                }
            };

            // simulation
            if(flag)
            {
                if(IF_DEBUG)
                {
                    for(int i = 0; i < int(main_repairs.size()); i++)
                    {
                        std::cout << "> Failed Blocks: ";
                        for(int j = 0; j < int(main_repairs[i].failed_blocks_index.size()); j++)
                        {
                            std::cout << main_repairs[i].failed_blocks_index[j] << " ";
                        }
                        std::cout << std::endl;
                        std::cout << "> Repair by Blocks: ";
                        for(int jj = 0; jj < int(main_repairs[i].inner_rack_help_blocks_info.size()); jj++)
                        {
                            std::cout << main_repairs[i].inner_rack_help_blocks_info[jj].first << " ";
                        }
                        for(int j = 0; j < int(main_repairs[i].help_racks_blocks_info.size()); j++)
                        {
                            for(int jj = 0; jj < int(main_repairs[i].help_racks_blocks_info[j].size()); jj++)
                            {
                                std::cout << main_repairs[i].help_racks_blocks_info[j][jj].first << " ";
                            }
                        }
                        std::cout << std::endl;
                    }
                }
                simulation_repair(main_repairs, cross_rack_num);
                if(IF_DEBUG)
                {
                    std::cout << "Finish simulation! " << cross_rack_num << std::endl;
                }
            }

            gettimeofday(&start_time, NULL);
            if(!flag)
            {
                std::cout << "Undecodable!" << std::endl;
                my_assert(false);
            }
            else if(!IF_SIMULATION)
            {
                for(int i = 0; i < int(main_repairs.size()); i++)
                {
                    try
                    {
                        int num_of_failed_blocks = int(main_repairs[i].failed_blocks_index.size());
                        unsigned int main_rack_id = main_repairs[i].rack_id;
                        std::thread my_main_thread(send_main_repair_plan, i, main_rack_id);
                        std::vector<std::thread> senders;
                        int index = 0;
                        for(int j = 0; j < int(main_repairs[i].help_racks_blocks_info.size()); j++)
                        {
                            int num_of_blocks_in_help_rack = int(main_repairs[i].help_racks_blocks_info[j].size());
                            my_assert(num_of_blocks_in_help_rack == int(help_repairs[i][j].inner_rack_help_blocks_info.size()));
                            if((IF_DIRECT_FROM_NODE && ec_schema_.partial_decoding && num_of_failed_blocks < num_of_blocks_in_help_rack) ||
                                !IF_DIRECT_FROM_NODE)
                            {
                                Rack &rack = rack_table_[help_repairs[i][j].rack_id];
                                senders.push_back(std::thread(send_help_repair_plan, i, j, rack.proxy_ip, rack.proxy_port));
                            }
                        }
                        for (int j = 0; j < int(senders.size()); j++)
                        {
                            senders[j].join();
                        }
                        my_main_thread.join();
                    }
                    catch(const std::exception& e)
                    {
                        std::cerr << e.what() << '\n';
                    }
                    for(auto it = help_decoding_time_ptr->begin(); it != help_decoding_time_ptr->end(); it++)
                    {
                        decoding_time += *it;
                    }
                    for(auto it = main_decoding_time_ptr->begin(); it != main_decoding_time_ptr->end(); it++)
                    {
                        decoding_time += *it;
                    }
                    main_decoding_time_ptr->clear();
                    help_decoding_time_ptr->clear();
                }
            }
            gettimeofday(&end_time, NULL);
            repair_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
            if(IF_DEBUG)
            {
                std::cout << "Repair time = " << repair_time << "s, decoding_time = " << decoding_time << "s." << std::endl;
            }
        }
    }

    void Coordinator::check_out_failures(int stripe_id, std::vector<unsigned int> failed_ids,
                                        std::unordered_map<unsigned int, std::vector<int>> &failures_map,
                                        std::unordered_map<unsigned int, ECProject::FailureType> &failures_type)
    {
        if(stripe_id >= 0)  // block failures
        {
            int num_of_failed_blocks = (int)failed_ids.size();
            std::vector<int> failed_blocks;
            for(int i = 0; i < num_of_failed_blocks; i++)
            {
                failed_blocks.push_back(failed_ids[i]);
            }
            failures_map[stripe_id] = failed_blocks;
            if(num_of_failed_blocks == 1)
            {
                failures_type[stripe_id] = FailureType::single_block;
            }
            else
            {
                failures_type[stripe_id] = FailureType::multi_blocks;
            }
        }
        else    // node failures
        {
            int num_of_failed_nodes = int(failed_ids.size());
            for(int i = 0; i < num_of_failed_nodes; i++)
            {
                Node &node = node_table_[failed_ids[i]];
                for(auto it = node.stripes_blocks.begin(); it != node.stripes_blocks.end(); it++)
                {
                    unsigned int t_stripe_id = it->first;
                    Stripe &stripe = stripe_table_[t_stripe_id];
                    int failed_block_idx = -1;
                    for(int j = 0; j < stripe.k + stripe.l + stripe.g; j++)
                    {
                        if(stripe.blocks2nodes[j] == failed_ids[i])
                        {
                            failed_block_idx = j;
                            break;
                        }
                    }
                    if(failures_map.find(t_stripe_id) != failures_map.end())
                    {
                        failures_map[t_stripe_id].push_back(failed_block_idx);
                    }
                    else
                    {
                        std::vector<int> failed_blocks;
                        failed_blocks.push_back(failed_block_idx);
                        failures_map[t_stripe_id] = failed_blocks;
                    }
                }
            }

            for(auto it = failures_map.begin(); it != failures_map.end(); it++)
            {
                int t_stripe_id = it->first;
                if(int((it->second).size()) == 1)
                {
                    failures_type[t_stripe_id] = ECProject::single_block;
                }
                else
                {
                    failures_type[t_stripe_id] = ECProject::multi_blocks;
                }
            }
        }
    }

    bool Coordinator::generate_plan_for_a_local_or_global_repair_LRC(std::vector<main_repair_plan> &main_repair, 
            std::vector<std::vector<help_repair_plan>> &help_repair, unsigned int stripe_id, std::vector<int> &failed_blocks, bool local_repair)
    {
        Stripe &stripe = stripe_table_[stripe_id];
        int k = stripe.k;
        int l = stripe.l;
        int g = stripe.g;
        ECTYPE ec_type = stripe.ec_type;

        main_repair_plan main_plan;
        int num_of_failed_blocks = int(failed_blocks.size());
        
        std::unordered_map<unsigned int, std::vector<int>> help_info;
        
        if(local_repair)
        {
            my_assert(num_of_failed_blocks == 1);
            main_plan.is_local_repair = true;
            local_repair_plan(failed_blocks[0], stripe_id, help_info);
        }
        else
        {
            main_plan.is_local_repair = false;
            global_repair_plan(failed_blocks, stripe_id, help_info);
        }

        if(IF_DEBUG)
        {
            for(auto it = help_info.begin(); it != help_info.end(); it++)
            {
                std::vector<int> &help_blocks = it->second;
                std::cout << "Rack " << it->first << " : ";
                for(auto i = 0; i < help_blocks.size(); ++i)
                {
                    std::cout << help_blocks[i] << " ";
                }
                std::cout << std::endl;
            }
        }

        unsigned int main_rack_id = node_table_[stripe.blocks2nodes[failed_blocks[0]]].map2rack;

        // for new locations, to optimize
        std::unordered_map<unsigned int, std::vector<unsigned int>> free_nodes_in_racks;
        for(auto it = failed_blocks.begin(); it != failed_blocks.end(); it++)
        {
            unsigned int node_id = stripe.blocks2nodes[*it];
            // node_table_[node_id].storage_cost -= 1;
            // node_table_[node_id].network_cost -= 1;
            unsigned int rack_id = node_table_[node_id].map2rack;
            if(free_nodes_in_racks.find(rack_id) == free_nodes_in_racks.end())
            {
                std::vector<unsigned int> free_nodes;
                Rack &rack = rack_table_[rack_id];
                for(int i = 0; i < num_of_nodes_per_rack_; i++)
                {
                    free_nodes.push_back(rack.nodes[i]);
                }
                free_nodes_in_racks[rack_id] = free_nodes;
            }
            auto iter = std::find(free_nodes_in_racks[rack_id].begin(), 
                                  free_nodes_in_racks[rack_id].end(), node_id);
            free_nodes_in_racks[rack_id].erase(iter);
        }

        auto inner_help_blocks = help_info[main_rack_id];
        main_plan.rack_id = main_rack_id;
        main_plan.stripe_id = stripe_id;
        main_plan.k = stripe.k;
        main_plan.l = stripe.l;
        main_plan.g = stripe.g;
        main_plan.block_size = stripe.block_size;
        main_plan.ec_type = stripe.ec_type;
        main_plan.partial_decoding = ec_schema_.partial_decoding;
        for(auto it = failed_blocks.begin(); it != failed_blocks.end(); it++)
        {
            main_plan.failed_blocks_index.push_back(*it);
        }
        for(int i = 0; i < int(inner_help_blocks.size()); i++)
        {
            int block_idx = inner_help_blocks[i];
            main_plan.live_blocks_index.push_back(block_idx);
            unsigned int node_id = stripe.blocks2nodes[block_idx];
            // for each help block from one node
            node_table_[node_id].network_cost += 1;
            std::string node_ip = node_table_[node_id].node_ip;
            int node_port = node_table_[node_id].node_port;
            main_plan.inner_rack_help_blocks_info.push_back(std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
            // for new locations
            unsigned int rack_id = node_table_[node_id].map2rack;
            auto iter = std::find(free_nodes_in_racks[rack_id].begin(), 
                                  free_nodes_in_racks[rack_id].end(), node_id);
            free_nodes_in_racks[rack_id].erase(iter);
        }
        std::vector<help_repair_plan> help_plans;
        int num_of_help_racks = int(help_info.size()) - 1;
        for(int i = 0; i < num_of_help_racks; i++)
        {
            help_repair_plan help_plan;
            help_plan.stripe_id = stripe_id;
            help_plan.k = stripe.k;
            help_plan.l = stripe.l;
            help_plan.g = stripe.g;
            help_plan.block_size = stripe.block_size;
            help_plan.ec_type = stripe.ec_type;
            help_plan.partial_decoding = ec_schema_.partial_decoding;
            help_plan.is_local_repair = main_plan.is_local_repair;
            for(auto it = failed_blocks.begin(); it != failed_blocks.end(); it++)
            {
                help_plan.failed_blocks_index.push_back(*it);
            }
            for(auto it = main_plan.live_blocks_index.begin(); 
                    it != main_plan.live_blocks_index.end(); it++)
            {
                help_plan.live_blocks_index.push_back(*it);
            }
            help_plan.main_proxy_ip = rack_table_[main_rack_id].proxy_ip;
            help_plan.main_proxy_port = rack_table_[main_rack_id].proxy_port + 500; // add bias
            help_plans.push_back(help_plan);
        }
        int idx = 0;
        for(auto it = help_info.begin(); it != help_info.end(); it++)
        {
            if(it->first != main_rack_id)
            {
                std::vector<int> help_blocks = it->second;
                int num_of_help_blocks = int(help_blocks.size());
                for(int i = 0; i < num_of_help_blocks; i++)
                {
                    int block_idx = help_blocks[i];
                    unsigned int node_id = stripe.blocks2nodes[block_idx];
                    // for each help block from one node
                    node_table_[node_id].network_cost += 1;
                    std::string node_ip = node_table_[node_id].node_ip;
                    int node_port = node_table_[node_id].node_port;
                    help_plans[idx].inner_rack_help_blocks_info.push_back(std::make_pair(block_idx, std::make_pair(node_ip, node_port)));
                }
                main_plan.help_racks_blocks_info.push_back(help_plans[idx].inner_rack_help_blocks_info);
                help_plans[idx].rack_id = it->first;
                // update all the live_blocks_index
                for(int i = 0; i < num_of_help_racks; i++)
                {
                    for(int j = 0; j < num_of_help_blocks; j++)
                    {
                        int block_idx = help_blocks[j];
                        help_plans[i].live_blocks_index.push_back(block_idx);
                    }
                }
                for(int i = 0; i < num_of_help_blocks; i++)
                {
                    int block_idx = help_blocks[i];
                    main_plan.live_blocks_index.push_back(block_idx);
                }
                idx++;
            }
        }
        for(auto it = failed_blocks.begin(); it != failed_blocks.end(); it++)
        {
            unsigned int node_id = stripe.blocks2nodes[*it];
            unsigned int rack_id = node_table_[node_id].map2rack;
            std::vector<unsigned int> &free_nodes = free_nodes_in_racks[rack_id];
            int ran_node_idx = -1;
            unsigned int new_node_id = 0;
            if(NodeSelectionType::random == ec_schema_.ns_type) // randomly select a node
            {
                ran_node_idx = random_index(free_nodes.size());
                new_node_id = free_nodes[ran_node_idx];
            }
            else    // select by load
            {
                new_node_id = select_a_node_by_load(free_nodes);
            }
            auto iter = std::find(free_nodes.begin(), free_nodes.end(), new_node_id);
            free_nodes.erase(iter);
            
            std::string node_ip = node_table_[new_node_id].node_ip;
            int node_port = node_table_[new_node_id].node_port;
            // node_table_[new_node_id].storage_cost += 1;
            // node_table_[new_node_id].network_cost += 1;
            main_plan.new_locations.push_back(std::make_pair(rack_id, std::make_pair(node_ip, node_port)));
        }
        main_repair.push_back(main_plan);
        help_repair.push_back(help_plans);
        return true;
    }

    bool Coordinator::generate_repair_plan_for_multi_blocks_LRC(std::vector<main_repair_plan> &main_repair, 
            std::vector<std::vector<help_repair_plan>> &help_repair, unsigned int stripe_id, std::vector<int> &failed_blocks)
    {
        Stripe &stripe = stripe_table_[stripe_id];
        int k = stripe.k;
        int l = stripe.l;
        int g = stripe.g;
        ECTYPE ec_type = stripe.ec_type;

        std::vector<int> b2g(k + g + l, 0);
        return_block2group_Azu_LRC(k, l, g, b2g); // block to group id map
        std::vector<int> failed_map(k + g + l, 0);
        std::vector<int> fb_group_cnt(l, 0);
        int num_of_failed_data_or_global = 0;
        int num_of_failed_blocks = int(failed_blocks.size());
        for(int i = 0; i < num_of_failed_blocks; i++)
        {
            int failed_idx = failed_blocks[i];
            failed_map[failed_idx] = 1;
            fb_group_cnt[b2g[failed_idx]] += 1;
            if(failed_idx < k + g)
            {
                num_of_failed_data_or_global += 1;
            }
        }

        int iter_cnt = 0;
        while(num_of_failed_blocks > 0)
        {
            for(int group_id = 0; group_id < l; group_id++)
            {
                // local repair
                if(fb_group_cnt[group_id] == 1)
                {
                    int failed_idx = -1;
                    for(int i = 0; i < k + g + l; i++)
                    {
                        if(failed_map[i] && b2g[i] == group_id)
                        {
                            failed_idx = i;
                            break;
                        }
                    }

                    std::vector<int> temp_failed_blocks;
                    temp_failed_blocks.push_back(failed_idx);
                    generate_plan_for_a_local_or_global_repair_LRC(main_repair, help_repair, stripe_id, temp_failed_blocks, true);

                    // update
                    failed_map[failed_idx] = 0;
                    fb_group_cnt[group_id] = 0;
                    num_of_failed_blocks -= 1;
                    if(failed_idx < k + g)
                    {
                        num_of_failed_data_or_global -= 1;
                    }
                }
            }
            // 1 <= data_or_global_failed_num <= g, global repair
            if(num_of_failed_data_or_global > 0 && num_of_failed_data_or_global <= g)
            {
                std::vector<int> failed_indexs;
                for(int i = 0; i < k + g; i++)
                {
                    if(failed_map[i])
                    {
                        failed_indexs.push_back(i);
                    }
                }

                generate_plan_for_a_local_or_global_repair_LRC(main_repair, help_repair, stripe_id, failed_indexs, false);

                // update 
                for(int i = 0; i < k + g; i++)
                {
                    if(failed_map[i])
                    {
                        failed_map[i] = 0;
                        num_of_failed_blocks -= 1;
                        fb_group_cnt[b2g[i]] -= 1;
                    }
                }
                num_of_failed_data_or_global = 0;
            }

            if(iter_cnt > 0 && num_of_failed_blocks > 0)
            {

                bool if_decodable = true;
                if_decodable = check_if_decodable_Azu_LRC(k, g, l, failed_blocks);
                if(if_decodable)    // if decodable, repair in one go
                {
                    std::vector<int> failed_indexs;
                    for(int i = 0; i < k + g + l; i++)
                    {
                        if(failed_map[i])
                        {
                            failed_indexs.push_back(i);
                        }
                    }

                    generate_plan_for_a_local_or_global_repair_LRC(main_repair, help_repair, stripe_id, failed_indexs, false);

                    // update 
                    for(int i = 0; i < k + g + l; i++)
                    {
                        if(failed_map[i])
                        {
                            failed_map[i] = 0;
                            num_of_failed_blocks -= 1;
                            fb_group_cnt[b2g[i]] -= 1;
                        }
                    }
                    num_of_failed_data_or_global = 0;
                }
                else
                {
                    std::cout << "Undecodable!!!" << std::endl;
                    return false;
                }
            }
            iter_cnt++;
        }
        return true;
    }
    
    void Coordinator::local_repair_plan(int failed_index, unsigned int stripe_id, 
                                        std::unordered_map<unsigned int, std::vector<int>> &help_info)
    {
        Stripe &stripe = stripe_table_[stripe_id];
        int k = stripe.k;
        int l = stripe.l;
        int g = stripe.g;
    
        std::vector<int> b2g(k + g + l, 0);
        return_block2group_Azu_LRC(k, l, g, b2g); // block to group id map
        for(int i = 0; i < k + g + l; i++)
        {
            if(i != failed_index && b2g[i] == b2g[failed_index])
            {
                unsigned int rack_id = node_table_[stripe.blocks2nodes[i]].map2rack;
                if(help_info.find(rack_id) == help_info.end())
                {
                    std::vector<int> tmp({i});
                    help_info[rack_id] = tmp;
                }
                else
                {
                    help_info[rack_id].push_back(i);
                }
            }
        }
    }

    void Coordinator::global_repair_plan(std::vector<int> failed_indexs, unsigned int stripe_id, 
                                         std::unordered_map<unsigned int, std::vector<int>> &help_info)
    {
        std::unordered_map<unsigned int, std::vector<int>> rack_placement;
        std::unordered_map<unsigned int, int> data_global_cnt;

        Stripe &stripe = stripe_table_[stripe_id];
        int k = stripe.k;
        int l = stripe.l;
        int g = stripe.g;
        // find out the placement information, the blocks placed in racks
        for(int i = 0; i < k + g + l; i++)
        {
            unsigned int node_id = stripe.blocks2nodes[i];
            unsigned int rack_id = node_table_[node_id].map2rack;
            if(rack_placement.find(rack_id) == rack_placement.end())
            {
                std::vector<int> blocks({i});
                rack_placement[rack_id] = blocks;
                if(i >= k + g)
                    data_global_cnt[rack_id] = 0;
                else
                    data_global_cnt[rack_id] = 1;
            }
            else
            {
                rack_placement[rack_id].push_back(i);
                if(i < k + g)
                    data_global_cnt[rack_id]++;
            }
        }

        std::vector<int> b2g(k + g + l, 0);
        return_block2group_Azu_LRC(k, l, g, b2g); // block to group id map
        std::vector<int> failure_in_group_cnt(l + 1, 0);
        // find out the racks that contain failed blocks
        std::unordered_set<unsigned int> failed_racks;
        int num_of_failed_blocks = int(failed_indexs.size());
        for(int i = 0; i < num_of_failed_blocks; i++)
        {
            unsigned int node_id = stripe.blocks2nodes[failed_indexs[i]];
            unsigned int rack_id = node_table_[node_id].map2rack;
            failed_racks.insert(rack_id);
            failure_in_group_cnt[b2g[failed_indexs[i]]]++;
        }
        
        // find help blocks for repair
        int num_of_help_blocks = k;
        std::unordered_set<int> failed_blocks(failed_indexs.begin(), failed_indexs.end());
        for(auto it = failed_racks.begin(); it != failed_racks.end(); it++) // first in the failed racks
        {
            unsigned int rack_id = *it;
            std::vector<int> tmp;
            help_info[rack_id] = tmp;
            int num_of_blocks_in_rack = int(rack_placement[rack_id].size());
            for(int i = 0; i < num_of_blocks_in_rack; i++)
            {
                int block_idx = rack_placement[rack_id][i];
                if(block_idx < k + g && failed_blocks.find(block_idx) == failed_blocks.end())
                {
                    help_info[rack_id].push_back(block_idx);
                    num_of_help_blocks--;
                    if(!num_of_help_blocks)
                        break;
                }
            }
            if(!num_of_help_blocks)
                break;
            auto it1 = rack_placement.find(rack_id);
            rack_placement.erase(it1);
            auto it2 = data_global_cnt.find(rack_id);
            data_global_cnt.erase(it2);
        }
        while(num_of_help_blocks)   // rack with max to min num of data and global parity blocks
        {
            int max_val = 0;
            int max_rack_id = 0;
            for(auto it = data_global_cnt.begin(); it != data_global_cnt.end(); it++)
            {
                if(max_val < it->second)
                {
                    max_val = it->second;
                    max_rack_id = it->first;
                }
            }
            std::vector<int> tmp;
            help_info[max_rack_id] = tmp;
            int num_of_blocks_in_rack = int(rack_placement[max_rack_id].size());
            for(int i = 0; i < num_of_blocks_in_rack; i++)
            {
                int block_idx = rack_placement[max_rack_id][i];
                if(block_idx < k + g)
                {
                    help_info[max_rack_id].push_back(block_idx);
                    num_of_help_blocks--;
                    if(!num_of_help_blocks)
                        break;
                }
            }
            auto it1 = rack_placement.find(max_rack_id);
            rack_placement.erase(it1);
            auto it2 = data_global_cnt.find(max_rack_id);
            data_global_cnt.erase(it2);
        }    

        if(num_of_failed_blocks > g)    // repair needs both local and global parity blocks
        {
            for(int i = 0; i < l; i++)
            {
                if(failure_in_group_cnt[i] > 1)
                {
                    unsigned int rack_id = node_table_[stripe.blocks2nodes[k + g + i]].map2rack;
                    if(help_info.find(rack_id) == help_info.end())
                    {
                        std::vector<int> tmp({k + g + i});
                        help_info[rack_id] = tmp;
                    }
                    else
                    {
                        help_info[rack_id].push_back(k + g + i);
                    }
                }
            }
        }
    }

    // two hierarchies
    void Coordinator::simulation_repair(std::vector<main_repair_plan> &main_repair, int &cross_rack_num)
    {
        for(int i = 0; i < int(main_repair.size()); i++)
        {
            int num_of_failed_blocks = int(main_repair[i].failed_blocks_index.size());
            for(int j = 0; j < int(main_repair[i].help_racks_blocks_info.size()); j++)
            {
                int num_of_help_blocks = int(main_repair[i].help_racks_blocks_info[j].size());
                if(num_of_help_blocks > num_of_failed_blocks && ec_schema_.partial_decoding)
                {
                    cross_rack_num += num_of_failed_blocks;
                }
                else
                {
                    cross_rack_num += num_of_help_blocks;
                }
            }
        }
    }
}