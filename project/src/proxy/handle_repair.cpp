#include "proxy.h"

namespace ECProject
{
    double Proxy::main_repair(main_repair_plan repair_plan)
    {
        struct timeval start_time, end_time;
        double decoding_time = 0;

        bool if_partial_decoding = repair_plan.partial_decoding;
        int stripe_id = repair_plan.stripe_id;
        size_t block_size = repair_plan.block_size;
        int k = repair_plan.k;
        int g = repair_plan.g;
        int l = repair_plan.l;
        int num_of_failed_blocks = (int)repair_plan.failed_blocks_index.size();
        if(repair_plan.is_local_repair)
        {
            my_assert(num_of_failed_blocks == 1);
        }
        int *erasures = new int[num_of_failed_blocks + 1];
        for(int i = 0; i < num_of_failed_blocks; i++)
        {
            erasures[i] = repair_plan.failed_blocks_index[i];
        }
        erasures[num_of_failed_blocks] = -1;

        if(IF_DEBUG)
        {
            std::cout << "[Main Proxy " << self_rack_id_ << "] To repair ";
            for(int i = 0; i < num_of_failed_blocks; i++)
            {
                std::cout << erasures[i] << " ";
            }
            std::cout << std::endl;
        }

        auto fls_idx_ptr = std::make_shared<std::vector<int>>(repair_plan.failed_blocks_index);
        auto svrs_idx_ptr = std::make_shared<std::vector<int>>(repair_plan.live_blocks_index);

        auto original_lock_ptr = std::make_shared<std::mutex>();
        auto original_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
        auto original_blocks_idx_ptr = std::make_shared<std::vector<int>>();

        auto partial_lock_ptr = std::make_shared<std::mutex>();
        auto partial_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();

        auto get_from_node = [this, original_lock_ptr, original_blocks_ptr, original_blocks_idx_ptr, block_size] 
                             (int stripe_id, int block_idx, std::string node_ip, int node_port) mutable
        {
            std::string block_id = std::to_string(stripe_id) + "_" + std::to_string(block_idx);
            std::vector<char> tmp_val(block_size);
            read_from_datanode(block_id.c_str(), block_id.size(), 
                                tmp_val.data(), block_size, node_ip.c_str(), node_port);
            original_lock_ptr->lock();
            original_blocks_ptr->push_back(tmp_val);
            original_blocks_idx_ptr->push_back(block_idx);
            original_lock_ptr->unlock();
        };

        auto get_from_proxy = [this, original_lock_ptr, original_blocks_ptr, original_blocks_idx_ptr,
                               partial_lock_ptr, partial_blocks_ptr, block_size, num_of_failed_blocks]
                              (std::shared_ptr<asio::ip::tcp::socket> socket_ptr) mutable
        {
            asio::error_code ec;
            std::vector<unsigned char> int_buf(sizeof(int));
            asio::read(*socket_ptr, asio::buffer(int_buf, int_buf.size()), ec);
            int t_rack_id = ECProject::bytes_to_int(int_buf);
            std::vector<unsigned char> int_flag_buf(sizeof(int));
            asio::read(*socket_ptr, asio::buffer(int_flag_buf, int_flag_buf.size()), ec);
            int t_flag = ECProject::bytes_to_int(int_flag_buf);
            std::string msg = "data";
            if(t_flag)
                msg = "partial";
            if (IF_DEBUG)
            {
                std::cout << "[Main Proxy " << self_rack_id_ << "] Try to get " << msg << " blocks from the proxy in rack " << t_rack_id << ". " << std::endl;
            }
            if(t_flag) // receive partial blocks from help proxies
            {
                std::vector<unsigned char> int_buf_num_of_blocks(sizeof(int));
                asio::read(*socket_ptr, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), ec);
                int partial_block_num = ECProject::bytes_to_int(int_buf_num_of_blocks);
                if(partial_block_num != num_of_failed_blocks)
                {
                    std::cout << "Error! num_of_partial_blocks != num_of_failed_blocks" << std::endl;
                }
                partial_lock_ptr->lock();
                for (int i = 0; i < num_of_failed_blocks; i++)
                {
                    std::vector<char> tmp_val(block_size);
                    asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
                    partial_blocks_ptr->push_back(tmp_val);
                }
                partial_lock_ptr->unlock();
            }
            else  // receive data blocks from help proxies
            {
                std::vector<unsigned char> int_buf_num_of_blocks(sizeof(int));
                asio::read(*socket_ptr, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()), ec);
                int block_num = ECProject::bytes_to_int(int_buf_num_of_blocks);
                for (int i = 0; i < block_num; i++)
                {
                    std::vector<char> tmp_val(block_size);
                    std::vector<unsigned char> block_idx_buf(sizeof(int));
                    asio::read(*socket_ptr, asio::buffer(block_idx_buf, block_idx_buf.size()), ec);
                    int block_idx = ECProject::bytes_to_int(block_idx_buf);
                    asio::read(*socket_ptr, asio::buffer(tmp_val.data(), block_size), ec);
                    original_lock_ptr->lock();
                    original_blocks_ptr->push_back(tmp_val);
                    original_blocks_idx_ptr->push_back(block_idx);
                    original_lock_ptr->unlock();
                }
            }

            if (IF_DEBUG)
            {
                std::cout << "[Main Proxy " << self_rack_id_ << "] Finish getting data from the proxy in rack " << t_rack_id << std::endl;
            }
        };

        auto send_to_datanode = [this, block_size](std::string block_id, char *data, std::string node_ip, int node_port)
        {
            write_to_datanode(block_id.c_str(), block_id.size(), data, block_size, node_ip.c_str(), node_port);
        };

        // get original blocks inside rack
        int num_of_original_blocks = (int)repair_plan.inner_rack_help_blocks_info.size();
        if(num_of_original_blocks > 0)
        {
            std::vector<std::thread> readers;
            for (int i = 0; i < num_of_original_blocks; i++)
            {
                readers.push_back(std::thread(get_from_node, stripe_id,
                                              repair_plan.inner_rack_help_blocks_info[i].first, 
                                              repair_plan.inner_rack_help_blocks_info[i].second.first,
                                              repair_plan.inner_rack_help_blocks_info[i].second.second));
            }
            for (int i = 0; i < num_of_original_blocks; i++)
            {
                readers[i].join();
            }

            if(IF_DEBUG)
            {
                std::cout << "[Main Proxy " << self_rack_id_ << "] Finish getting blocks inside main rack." << std::endl;
            }
        }

        // get blocks or partial blocks from other racks
        int num_of_help_racks = (int)repair_plan.help_racks_blocks_info.size();
        int partial_cnt = 0;  // num of partial-block sets
        if(num_of_help_racks > 0)
        {
            std::vector<std::thread> readers;
            for(int i = 0; i < num_of_help_racks; i++)
            {
                int num_of_blocks_help_rack = (int)repair_plan.help_racks_blocks_info[i].size();
                bool t_flag = true;
                if(num_of_blocks_help_rack <= num_of_failed_blocks)
                {
                    t_flag = false;
                }
                t_flag = (if_partial_decoding && t_flag);
                if(!t_flag && IF_DIRECT_FROM_NODE)
                {
                    num_of_original_blocks += num_of_blocks_help_rack;
                    for(int j = 0; j < num_of_blocks_help_rack; j++)
                    {
                        readers.push_back(std::thread(get_from_node, stripe_id,
                                            repair_plan.help_racks_blocks_info[i][j].first, 
                                            repair_plan.help_racks_blocks_info[i][j].second.first,
                                            repair_plan.help_racks_blocks_info[i][j].second.second));
                    }
                }
                else
                {
                    if(t_flag)  // encode partial blocks and transfer through proxies
                        partial_cnt++;
                    else    // direct transfer original blocks through proxies
                        num_of_original_blocks += num_of_blocks_help_rack;
                    std::shared_ptr<asio::ip::tcp::socket> socket_ptr = std::make_shared<asio::ip::tcp::socket>(io_context_);
                    acceptor_.accept(*socket_ptr);
                    readers.push_back(std::thread(get_from_proxy, socket_ptr));
                }
            }
            int num_of_readers = (int)readers.size();
            for(int i = 0; i < num_of_readers; i++)
            {
                readers[i].join();
            }

            // simulate cross-rack transfer
            if(IF_SIMULATE_CROSS_RACK)
            {
                for(int i = 0; i < num_of_help_racks; i++)
                {
                    int num_of_blocks = (int)repair_plan.help_racks_blocks_info[i].size();
                    bool t_flag = true;
                    if(num_of_blocks <= num_of_failed_blocks)
                    {
                        t_flag = false;
                    }
                    t_flag = (if_partial_decoding && t_flag);
                    if(t_flag)
                    {
                        num_of_blocks = num_of_failed_blocks;
                    }
                    size_t t_val_len = (int)block_size * num_of_blocks;
                    std::string t_value = generate_random_string((int)t_val_len);
                    transfer_to_networkcore(t_value.c_str(), t_val_len);
                }
            }
        }

        my_assert(num_of_original_blocks == (int)original_blocks_ptr->size());
        if(num_of_original_blocks > 0 && if_partial_decoding)   // encode-and-transfer
        {
            std::vector<char *> v_data(num_of_original_blocks);
            std::vector<char *> v_coding(num_of_failed_blocks);
            char **data = (char **)v_data.data();
            char **coding = (char **)v_coding.data();
            for (int j = 0; j < num_of_original_blocks; j++)
            {
                data[j] = (*original_blocks_ptr)[j].data();
            }
            std::vector<std::vector<char>> v_coding_area(num_of_failed_blocks, std::vector<char>(block_size));
            for (int j = 0; j < num_of_failed_blocks; j++)
            {
                coding[j] = v_coding_area[j].data();
            }
            
            gettimeofday(&start_time, NULL);
            if(repair_plan.is_local_repair)  // local repair with encode-and-transfer
            {
                encode_partial_blocks_for_decoding_LRC_local(repair_plan.ec_type, k, repair_plan.g, repair_plan.l, data,
                                                             coding, block_size, original_blocks_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
            }
            else    // global repair with encode-and-transfer
            {
                if(check_ec_family(repair_plan.ec_type) == LRCs)
                {
                    encode_partial_blocks_for_decoding_LRC_global(repair_plan.ec_type, k, repair_plan.g, repair_plan.l, data,
                                                                  coding, block_size, original_blocks_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
                }
            }
            gettimeofday(&end_time, NULL);
            decoding_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
            
            partial_lock_ptr->lock();
            for (int j = 0; j < num_of_failed_blocks; j++)
            {
                partial_blocks_ptr->push_back(v_coding_area[j]);
            }
            partial_lock_ptr->unlock();
            partial_cnt++;
        }

        if (IF_DEBUG)
        {
            std::cout << "[Main Proxy" << self_rack_id_ << "] ready to decode! " << num_of_original_blocks << " - " << partial_cnt << std::endl;
        }

        // decode
        int data_num = partial_cnt * num_of_failed_blocks;
        int coding_num = num_of_failed_blocks;
        if(!if_partial_decoding)
        {
            if(repair_plan.is_local_repair)
            {
                data_num = (int)svrs_idx_ptr->size();
                coding_num = 1;
            }
            else
            {
                data_num = k;
                coding_num = g + l;
            }
            
        }
        else
        {
            my_assert(data_num == (int)partial_blocks_ptr->size());
        }

        if (IF_DEBUG)
        {
            std::cout << "[Main Proxy" << self_rack_id_ << "] " << data_num << " " << partial_blocks_ptr->size() << " " << original_blocks_idx_ptr->size() << std::endl;
        }

        std::vector<char *> vt_data(data_num);
        std::vector<char *> vt_coding(coding_num);
        char **t_data = (char **)vt_data.data();
        char **t_coding = (char **)vt_coding.data();
        std::vector<std::vector<char>> vt_data_area(data_num, std::vector<char>(block_size));
        std::vector<std::vector<char>> vt_coding_area(coding_num, std::vector<char>(block_size));
        for (int i = 0; i < coding_num; i++)
        {
            t_coding[i] = vt_coding_area[i].data();
        }
        for (int j = 0; j < coding_num; j++)
        {
            t_coding[j] = vt_coding_area[j].data();
        }

        // for local repair
        int min_idx = k + g + l;
        int group_id = -1;
        int group_size = -1;

        if(if_partial_decoding)
        {
            for(int i = 0; i < data_num; i++)
            {
                t_data[i] = (*partial_blocks_ptr)[i].data();
            }
        }
        else
        {
            if(repair_plan.is_local_repair)
            {
                group_size = (int)svrs_idx_ptr->size();
                for(auto it = svrs_idx_ptr->begin(); it != svrs_idx_ptr->end(); it++)
                {
                    int idx = *it;
                    if(idx >= k + g)
                        group_id = idx - k - g;
                    if(idx < min_idx)
                        min_idx = idx;
                }
                for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++)
                {
                    int idx = *it;
                    if(idx >= k + g)
                        group_id = idx - k - g;
                    if(idx < min_idx)
                        min_idx = idx;
                }
                for(int i = 0; i < data_num; i++)
                {
                    int idx = (*original_blocks_idx_ptr)[i];
                    if(idx >= k + g)
                        idx = group_size;
                    else
                        idx = idx - min_idx;
                    if(idx >= group_size)
                        t_coding[idx - group_size] = (*original_blocks_ptr)[i].data();
                    else
                        t_data[idx] = (*original_blocks_ptr)[i].data();
                }
            }
            else
            {
                for(int i = 0; i < data_num; i++)
                {
                    int idx = (*original_blocks_idx_ptr)[i];
                    if(idx >= k)
                        t_coding[idx - k] = (*original_blocks_ptr)[i].data();
                    else
                        t_data[idx] = (*original_blocks_ptr)[i].data();
                }
            }
        }

        if (IF_DEBUG)
        {
            std::cout << "[Main Proxy" << self_rack_id_ << "] decoding!" << std::endl;
        }

        gettimeofday(&start_time, NULL);
        if(if_partial_decoding)
        {
            if(data_num == coding_num)
                t_coding = t_data;
            else
                perform_addition(t_data, t_coding, block_size, data_num, coding_num);
        }
        else
        {
            if(repair_plan.is_local_repair)
            {
                int g = repair_plan.g;
                int l = repair_plan.l;
                if(erasures[0] >= k + g)
                    erasures[0] = group_size;
                else
                    erasures[0] -= min_idx;
                decode_LRC_local(repair_plan.ec_type, k, g, l, group_id, group_size, t_data, t_coding, block_size, erasures);
            }
            else
            {
                if(check_ec_family(repair_plan.ec_type) == LRCs)
                {
                    int g = repair_plan.g;
                    int l = repair_plan.l;
                    decode_LRC(repair_plan.ec_type, k, g, l, t_data, t_coding, block_size, erasures, num_of_failed_blocks);
                }
                
            }
        }
        gettimeofday(&end_time, NULL);
        decoding_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

        std::vector<std::thread> writers;
        for(int i = 0; i < num_of_failed_blocks; i++)
        {
            int index = repair_plan.failed_blocks_index[i];
            std::string failed_block_id = std::to_string(stripe_id) + "_" + std::to_string(index);
            if(if_partial_decoding)
            {
                writers.push_back(std::thread(send_to_datanode, failed_block_id, t_coding[i], 
                                                repair_plan.new_locations[i].second.first, repair_plan.new_locations[i].second.second));
            }
            else
            {
                if(repair_plan.is_local_repair)
                {
                    int g = repair_plan.g;
                    if(index >= k + g)
                        index = group_size;
                    else
                        index = index - min_idx;
                    if(index >= group_size)
                    {
                        writers.push_back(std::thread(send_to_datanode, failed_block_id, t_coding[index - group_size], 
                                                      repair_plan.new_locations[i].second.first, repair_plan.new_locations[i].second.second));
                    }
                    else
                    {
                        writers.push_back(std::thread(send_to_datanode, failed_block_id, t_data[index], 
                                                      repair_plan.new_locations[i].second.first, repair_plan.new_locations[i].second.second));
                    }
                }
                else
                {
                    if(index >= k)
                    {
                        writers.push_back(std::thread(send_to_datanode, failed_block_id, t_coding[index - k], 
                                                      repair_plan.new_locations[i].second.first, repair_plan.new_locations[i].second.second));
                    }
                    else
                    {
                        writers.push_back(std::thread(send_to_datanode, failed_block_id, t_data[index], 
                                                      repair_plan.new_locations[i].second.first, repair_plan.new_locations[i].second.second));
                    }
                }
            }
        }
        for(int i = 0; i < num_of_failed_blocks; i++)
        {
            writers[i].join();
        }

        if(IF_SIMULATE_CROSS_RACK)
        {
            for(int i = 0; i < (int)repair_plan.new_locations.size(); i++)
            {
                if(repair_plan.new_locations[i].first != self_rack_id_)
                {
                    std::string t_value = generate_random_string((int)block_size);
                    transfer_to_networkcore(t_value.c_str(), block_size);
                }
            }
        }

        if(IF_DEBUG)
        {
            std::cout << "[Main Proxy" << self_rack_id_ << "] finish repair " << num_of_failed_blocks << " blocks! Decoding time : " << decoding_time << std::endl;
        }

        return decoding_time;
    }

    double Proxy::help_repair(help_repair_plan repair_plan)
    {
        struct timeval start_time, end_time;
        double decoding_time = 0;

        bool if_partial_decoding = repair_plan.partial_decoding;
        bool t_flag = true;
        int num_of_original_blocks = (int)repair_plan.inner_rack_help_blocks_info.size();
        int num_of_failed_blocks = (int)repair_plan.failed_blocks_index.size();
        if(num_of_original_blocks <= num_of_failed_blocks)
        {
            t_flag = false;
        }
        t_flag = (if_partial_decoding && t_flag);
        if(!t_flag && IF_DIRECT_FROM_NODE)
        {
            return 0;
        }

        int stripe_id = repair_plan.stripe_id;
        int k = repair_plan.k;
        int l = repair_plan.l;
        int g = repair_plan.g;
        size_t block_size = repair_plan.block_size;

        if(repair_plan.is_local_repair)
        {
            my_assert(1 == num_of_failed_blocks);
        }
        auto fls_idx_ptr = std::make_shared<std::vector<int>>(repair_plan.failed_blocks_index);
        auto svrs_idx_ptr = std::make_shared<std::vector<int>>(repair_plan.live_blocks_index);

        auto original_lock_ptr = std::make_shared<std::mutex>();
        auto original_blocks_ptr = std::make_shared<std::vector<std::vector<char>>>();
        auto original_blocks_idx_ptr = std::make_shared<std::vector<int>>();

        auto get_from_node = [this, original_lock_ptr, original_blocks_ptr, original_blocks_idx_ptr, block_size] 
                             (int stripe_id, int block_idx, std::string node_ip, int node_port) mutable
        {
            std::string block_id = std::to_string(stripe_id) + "_" + std::to_string(block_idx);
            std::vector<char> tmp_val(block_size);
            read_from_datanode(block_id.c_str(), block_id.size(), 
                                tmp_val.data(), block_size, node_ip.c_str(), node_port);
            original_lock_ptr->lock();
            original_blocks_ptr->push_back(tmp_val);
            original_blocks_idx_ptr->push_back(block_idx);
            original_lock_ptr->unlock();
        };
        if (IF_DEBUG)
        {
            std::cout << "[Helper Proxy" << self_rack_id_ << "] Ready to read blocks from data node!" << std::endl;
        }

        if(num_of_original_blocks > 0)
        {
            std::vector<std::thread> readers;
            for(int i = 0; i < num_of_original_blocks; i++)
            {
                readers.push_back(std::thread(get_from_node, stripe_id, 
                                              repair_plan.inner_rack_help_blocks_info[i].first,
                                              repair_plan.inner_rack_help_blocks_info[i].second.first,
                                              repair_plan.inner_rack_help_blocks_info[i].second.second));
            }
            for(int i = 0; i < num_of_original_blocks; i++)
            {
                readers[i].join();
            }
        }

        my_assert(num_of_original_blocks == (int)original_blocks_ptr->size());

        int value_size = 0;

        if(t_flag)
        {
            if (IF_DEBUG)
            {
                std::cout << "[Helper Proxy" << self_rack_id_ << "] partial encoding with blocks " << std::endl;
                for(auto it = original_blocks_idx_ptr->begin(); it != original_blocks_idx_ptr->end(); it++)
                {
                    std::cout << (*it) << " ";
                }
                std::cout << std::endl;
            }
            // encode partial blocks
            std::vector<char *> v_data(num_of_original_blocks);
            std::vector<char *> v_coding(num_of_failed_blocks);
            char **data = (char **)v_data.data();
            char **coding = (char **)v_coding.data();
            std::vector<std::vector<char>> v_coding_area(num_of_failed_blocks, std::vector<char>(block_size));
            for (int j = 0; j < num_of_failed_blocks; j++)
            {
                coding[j] = v_coding_area[j].data();
            }
            for (int j = 0; j < num_of_original_blocks; j++)
            {
                data[j] = (*original_blocks_ptr)[j].data();
            }
            gettimeofday(&start_time, NULL);
            if(repair_plan.is_local_repair)
            {
                encode_partial_blocks_for_decoding_LRC_local(repair_plan.ec_type, k, g, l, data, coding, block_size, original_blocks_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
            }
            else
            {
                if(check_ec_family(repair_plan.ec_type) == LRCs)
                {
                    encode_partial_blocks_for_decoding_LRC_global(repair_plan.ec_type, k, g, l, data, coding, block_size, original_blocks_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
                }
                
            }
            gettimeofday(&end_time, NULL);
            decoding_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

            // send to main proxy
            asio::ip::tcp::socket socket_(io_context_);
            asio::ip::tcp::resolver resolver(io_context_);
            asio::error_code con_error;
            if (IF_DEBUG)
            {
                std::cout << "[Helper Proxy" << self_rack_id_ << "] Try to connect main proxy port " << repair_plan.main_proxy_port << std::endl;
            }
            asio::connect(socket_, resolver.resolve({repair_plan.main_proxy_ip, std::to_string(repair_plan.main_proxy_port)}), con_error);
            if (!con_error && IF_DEBUG)
            {
                std::cout << "[Helper Proxy" << self_rack_id_ << "] Connect to " << repair_plan.main_proxy_ip<< ":" << repair_plan.main_proxy_port << " success!" << std::endl;
            }
            std::vector<unsigned char> int_buf_self_rack_id = ECProject::int_to_bytes(self_rack_id_);
            asio::write(socket_, asio::buffer(int_buf_self_rack_id, int_buf_self_rack_id.size()));
            std::vector<unsigned char> t_flag = ECProject::int_to_bytes(1);
            asio::write(socket_, asio::buffer(t_flag, t_flag.size()));
            std::vector<unsigned char> int_buf_num_of_blocks = ECProject::int_to_bytes(num_of_failed_blocks);
            asio::write(socket_, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()));
            for (int j = 0; j < num_of_failed_blocks; j++)
            {
                asio::write(socket_, asio::buffer(coding[j], block_size));
                value_size += block_size;
            }
        }
        else if(!IF_DIRECT_FROM_NODE)
        {
            // send to main proxy
            asio::ip::tcp::socket socket_(io_context_);
            asio::ip::tcp::resolver resolver(io_context_);
            asio::error_code con_error;
            if (IF_DEBUG)
            {
                std::cout << "[Helper Proxy" << self_rack_id_ << "] Try to connect main proxy port " << repair_plan.main_proxy_port << std::endl;
            }
            asio::connect(socket_, resolver.resolve({repair_plan.main_proxy_ip, std::to_string(repair_plan.main_proxy_port)}), con_error);
            if (!con_error && IF_DEBUG)
            {
                std::cout << "[Helper Proxy" << self_rack_id_ << "] Connect to " << repair_plan.main_proxy_ip<< ":" << repair_plan.main_proxy_port << " success!" << std::endl;
            }
            std::vector<unsigned char> int_buf_self_rack_id = ECProject::int_to_bytes(self_rack_id_);
            asio::write(socket_, asio::buffer(int_buf_self_rack_id, int_buf_self_rack_id.size()));
            std::vector<unsigned char> t_flag = ECProject::int_to_bytes(0);
            asio::write(socket_, asio::buffer(t_flag, t_flag.size()));
            std::vector<unsigned char> int_buf_num_of_blocks = ECProject::int_to_bytes(num_of_original_blocks);
            asio::write(socket_, asio::buffer(int_buf_num_of_blocks, int_buf_num_of_blocks.size()));
            int j = 0;
            for(auto it = original_blocks_idx_ptr->begin(); it != original_blocks_idx_ptr->end(); it++, j++)
            { 
                // send index and value
                int block_idx = *it;
                std::vector<unsigned char> byte_block_idx = ECProject::int_to_bytes(block_idx);
                asio::write(socket_, asio::buffer(byte_block_idx, byte_block_idx.size()));
                asio::write(socket_, asio::buffer((*original_blocks_ptr)[j], block_size));
                value_size += block_size;
            }
        }
        
        if (IF_DEBUG)
        {
            std::cout << "[Helper Proxy" << self_rack_id_ << "] Send value to proxy" 
                      << repair_plan.main_proxy_port << "! With length of " << value_size 
                      << ". Decoding time : " << decoding_time << std::endl;
        }

        return decoding_time;
    } 
}