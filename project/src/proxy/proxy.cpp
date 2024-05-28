#include "proxy.h"

namespace ECProject
{
    Proxy::Proxy(std::string ip, int port, std::string networkcore, std::string config_path)
        : ip_(ip), port_(port), networkcore_(networkcore), config_path_(config_path), port_for_transfer_data_(port + 500),
        acceptor_(io_context_, asio::ip::tcp::endpoint(asio::ip::address::from_string(ip.c_str()), port + 500)) 
    {
        // port is for rpc, port + 500 is for socket
        rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, port_);
        rpc_server_->register_handler<&Proxy::checkalive>(this);
        rpc_server_->register_handler<&Proxy::encode_and_store_object>(this);
        rpc_server_->register_handler<&Proxy::decode_and_get_object>(this);
        rpc_server_->register_handler<&Proxy::delete_blocks>(this);
        rpc_server_->register_handler<&Proxy::main_repair>(this);
        rpc_server_->register_handler<&Proxy::help_repair>(this);
        rpc_server_->register_handler<&Proxy::migrate_blocks>(this);

        init_datanodes();
    }

    Proxy::~Proxy() {
        acceptor_.close();
        rpc_server_->stop();
    }

    void Proxy::run() { auto err = rpc_server_->start(); }

    std::string Proxy::checkalive(std::string msg) 
    { 
        return msg; 
    }

    void Proxy::init_datanodes()
    {
        tinyxml2::XMLDocument xml;
        xml.LoadFile(config_path_.c_str());
        tinyxml2::XMLElement *root = xml.RootElement();
        for (tinyxml2::XMLElement *rack = root->FirstChildElement(); rack != nullptr; rack = rack->NextSiblingElement())
        {
            std::string rack_id(rack->Attribute("id"));
            std::string proxy(rack->Attribute("proxy"));
            if (proxy == ip_ + ":" + std::to_string(port_))
            {
                self_rack_id_ = std::stoi(rack_id);
            }
            for (tinyxml2::XMLElement *node = rack->FirstChildElement()->FirstChildElement(); node != nullptr; node = node->NextSiblingElement())
            {
                std::string node_uri(node->Attribute("uri"));
                datanodes_[node_uri] = std::make_unique<coro_rpc::coro_rpc_client>();
                std::string ip = node_uri.substr(0, node_uri.find(':'));
                int port = std::stoi(node_uri.substr(node_uri.find(':') + 1, node_uri.size()));
                async_simple::coro::syncAwait(datanodes_[node_uri]->connect(ip, std::to_string(port)));
            }
        }
        // init networkcore
        datanodes_[networkcore_] = std::make_unique<coro_rpc::coro_rpc_client>();
        std::string ip = networkcore_.substr(0, networkcore_.find(':'));
        int port = std::stoi(networkcore_.substr(networkcore_.find(':') + 1, networkcore_.size()));
        async_simple::coro::syncAwait(datanodes_[networkcore_]->connect(ip, std::to_string(port)));
    }

    void Proxy::write_to_datanode(const char *key, size_t key_len, const char *value, size_t value_len, const char *ip, int port)
    {
        try
        {
            std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
            async_simple::coro::syncAwait(datanodes_[node_ip_port]->call<&Datanode::handle_set>(ip_, port_for_transfer_data_, false));

            asio::error_code error;
            asio::ip::tcp::socket socket_(io_context_);
            asio::ip::tcp::resolver resolver(io_context_);
            asio::error_code con_error;
            asio::connect(socket_, resolver.resolve({std::string(ip), std::to_string(port + 500)}), con_error);
            if (!con_error && IF_DEBUG)
            {
                std::cout << "Connect to " << ip << ":" << port + 500 << " success!" << std::endl;
            }


            std::vector<unsigned char> key_size_buf = int_to_bytes(key_len);
            asio::write(socket_, asio::buffer(key_size_buf, key_size_buf.size()));

            std::vector<unsigned char> value_size_buf = int_to_bytes(value_len);
            asio::write(socket_, asio::buffer(value_size_buf, value_size_buf.size()));

            asio::write(socket_, asio::buffer(key, key_len));
            asio::write(socket_, asio::buffer(value, value_len));

            std::vector<char> finish(1);
            asio::read(socket_, asio::buffer(finish, finish.size()));

            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);
            if (IF_DEBUG)
            {
                std::cout << "[Proxy" << self_rack_id_ << "][SET]"
                        << "Set " << key << " success! With length of " << value_len << std::endl;
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }
    }

    void Proxy::read_from_datanode(const char *key, size_t key_len, char *value, size_t value_len, const char *ip, int port)
    {
        try
        {
            std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
            async_simple::coro::syncAwait(datanodes_[node_ip_port]->call<&Datanode::handle_get>(std::string(key), key_len, value_len, true));
            if (IF_DEBUG)
            {
                std::cout << "[Proxy" << self_rack_id_ << "][GET]"
                        << "Call datanode to handle get " << key << std::endl;
            }

            asio::error_code error;
            asio::ip::tcp::socket socket_(io_context_);
            asio::ip::tcp::resolver resolver(io_context_);
            asio::error_code con_error;
            asio::connect(socket_, resolver.resolve({std::string(ip), std::to_string(port + 500)}), con_error);

            asio::read(socket_, asio::buffer(value, value_len));

            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);
            if (IF_DEBUG)
            {
                std::cout << "[Proxy" << self_rack_id_ << "][GET]"
                        << "Read data from socket with length of " << value_len << std::endl;
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }
    }

    void Proxy::delete_in_datanode(std::string block_id, const char *ip, int port)
    {
        try
        {
            std::string node_ip_port = std::string(ip) + ":" + std::to_string(port);
            async_simple::coro::syncAwait(datanodes_[node_ip_port]->call<&Datanode::handle_delete>(block_id));
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        
    }

    // source datanode -> destination datanode
    void Proxy::block_migration(const char *key, size_t key_len, size_t value_len, const char *src_ip, int src_port, const char *dsn_ip, int dsn_port)
    {
        try
        {
            std::string s_node_ip_port = std::string(src_ip) + ":" + std::to_string(src_port);
            async_simple::coro::syncAwait(datanodes_[s_node_ip_port]->call<&Datanode::handle_get>(std::string(key), key_len, value_len, false));
            if (IF_DEBUG)
            {
                std::cout << "[Proxy" << self_rack_id_ << "][Migration]"
                        << " Call datanode" << src_port << " to handle get " << key << std::endl;
            }

            std::string d_node_ip_port = std::string(dsn_ip) + ":" + std::to_string(dsn_port);
            async_simple::coro::syncAwait(datanodes_[d_node_ip_port]->call<&Datanode::handle_set>(src_ip, src_port + 500, true));
            if (IF_DEBUG)
            {
                std::cout << "[Proxy" << self_rack_id_ << "][Migration]"
                        << " Call datanode" << dsn_port << " to handle set " << key << std::endl;
            }
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        
    }

    void Proxy::transfer_to_networkcore(const char *value, size_t value_len)
    {
        try
        {
            async_simple::coro::syncAwait(datanodes_[networkcore_]->call<&Datanode::handle_transfer>());

            std::string ip;
            int port;
            std::stringstream ss(networkcore_);
            std::getline(ss, ip, ':');
            ss >> port;

            asio::error_code error;
            asio::ip::tcp::socket socket_(io_context_);
            asio::ip::tcp::resolver resolver(io_context_);
            asio::error_code con_error;
            asio::connect(socket_, resolver.resolve({std::string(ip), std::to_string(port + 500)}), con_error);
            if (!con_error && IF_DEBUG)
            {
                std::cout << "Connect to " << ip << ":" << port + 500 << " success!" << std::endl;
            }

            std::vector<unsigned char> value_size_buf = int_to_bytes(value_len);
            asio::write(socket_, asio::buffer(value_size_buf, value_size_buf.size()));

            asio::write(socket_, asio::buffer(value, value_len));

            std::vector<char> finish(1);
            asio::read(socket_, asio::buffer(finish, finish.size()));

            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);
            if (IF_DEBUG)
            {
                std::cout << "[Proxy" << self_rack_id_ << "][Cross-rack Transfer]"
                        << "Transfer success! With length of " << value_len << std::endl;
            }
        }
        catch (const std::exception &e)
        {
            std::cerr << e.what() << '\n';
        }
    }

    // non-blocked
    void Proxy::encode_and_store_object(placement_info placement)
    {
        auto encode_and_store = [this, placement]() mutable {
            struct timeval start_time, end_time;
            asio::ip::tcp::socket socket_(io_context_);
            acceptor_.accept(socket_);

            size_t value_buf_size = placement.k * placement.block_size * placement.stripe_ids.size();
            std::string key_buf(placement.key.size(), 0);
            std::string value_buf(value_buf_size, 0);
            my_assert(key_buf.size() == placement.key.size());
            my_assert(value_buf.size() == value_buf_size);

            if(IF_DEBUG)
            {
                std::cout << "[Proxy" << self_rack_id_ << "] [SET] Ready to receive value of " 
                          << placement.key << " with length of " << value_buf_size << std::endl;
            }

            size_t read_len_of_key = asio::read(socket_, asio::buffer(key_buf.data(), key_buf.size()));
            my_assert(read_len_of_key == key_buf.size());

            size_t read_len_of_value = asio::read(socket_, asio::buffer(value_buf.data(), placement.value_len));
            my_assert(read_len_of_value == placement.value_len);

            double encoding_time = 0;
            char *object_value = value_buf.data();
            for (auto i = 0; i < placement.stripe_ids.size(); i++) {
                std::vector<char *> data_v(placement.k);
                std::vector<char *> coding_v(placement.g + placement.l);
                char **data = (char **)data_v.data();
                char **coding = (char **)coding_v.data();

                size_t cur_block_size;
                if ((i == placement.stripe_ids.size() - 1) && placement.tail_block_size != -1) {
                    cur_block_size = placement.tail_block_size;
                } else {
                    cur_block_size = placement.block_size;
                }
                my_assert(cur_block_size > 0);

                if (IF_DEBUG)
                {
                    std::cout << "[Proxy" << self_rack_id_ << "] [SET]"
                            << "Encode value with size of " << placement.k * cur_block_size << std::endl;
                }

                std::vector<std::vector<char>> space_for_parity_blocks(placement.g + placement.l, std::vector<char>(cur_block_size));
                for (int j = 0; j < placement.k; j++) {
                    data[j] = &object_value[j * cur_block_size];
                }
                for (int j = 0; j < placement.g + placement.l; j++) {
                    coding[j] = space_for_parity_blocks[j].data();
                }

                gettimeofday(&start_time, NULL);
                if(check_ec_family(placement.ec_type) == LRCs)
                {
                    encode_LRC(placement.ec_type, placement.k, placement.g, placement.l, data, coding, cur_block_size);
                }
                gettimeofday(&end_time, NULL);
                encoding_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;

                int num_of_datanodes_involved = placement.k + placement.g + placement.l;
                int num_of_blocks_each_stripe = num_of_datanodes_involved;

                if (IF_DEBUG)
                {
                    std::cout << "[Proxy" << self_rack_id_ << "] [SET]"
                                << "Distribute blocks to datanodes." << std::endl;
                }

                std::vector<std::thread> writers;
                int k = placement.k;
                for (int j = 0; j < num_of_datanodes_involved; j++) {
                    std::string block_id = std::to_string(placement.stripe_ids[i]) + "_" + std::to_string(j);
                    std::pair<std::string, int> ip_and_port_of_datanode = placement.datanode_ip_port[i * num_of_blocks_each_stripe + j];
                    writers.push_back(
                        std::thread([this, j, k, block_id, data, coding, cur_block_size, ip_and_port_of_datanode]() {
                            if (j < k) {
                                write_to_datanode(block_id.c_str(), block_id.size(), 
                                                data[j], cur_block_size,
                                                ip_and_port_of_datanode.first.c_str(),
                                                ip_and_port_of_datanode.second);
                            } else {
                                write_to_datanode(block_id.c_str(), block_id.size(),
                                                coding[j - k], cur_block_size,
                                                ip_and_port_of_datanode.first.c_str(),
                                                ip_and_port_of_datanode.second);
                            }
                        }));
                }
                for (auto j = 0; j < writers.size(); j++) {
                    writers[j].join();
                }

                object_value += (placement.k * cur_block_size);
            }

            if (IF_DEBUG)
            {
                std::cout << "[Proxy" << self_rack_id_ << "] [SET]"
                          << "Finish encode and set." << std::endl;
            }

            std::vector<unsigned char> encoding_time_buf = double_to_bytes(encoding_time);
            asio::write(socket_, asio::buffer(encoding_time_buf, encoding_time_buf.size()));

            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);
        };
        try
        {
            std::thread new_thread(encode_and_store);
            new_thread.detach();
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }
    
    // non-blocked
    void Proxy::decode_and_get_object(placement_info placement)
    {
        auto decode_and_transfer = [this, placement]() mutable {
            std::string object_value;
            int left_value_len = placement.value_len;   // int
            for (auto i = 0; i < placement.stripe_ids.size(); i++) {
                unsigned int stripe_id = placement.stripe_ids[i];
                auto blocks_ptr = std::make_shared<std::unordered_map<int, std::string>>();

                size_t cur_block_size;
                if ((i == placement.stripe_ids.size() - 1) && placement.tail_block_size != -1) {
                    cur_block_size = placement.tail_block_size;
                } else {
                    cur_block_size = placement.block_size;
                }
                my_assert(cur_block_size > 0);

                if(IF_DEBUG)
                {
                    std::cout << "[Proxy" << self_rack_id_ << "] [SET]"
                              << "Ready to read data from datanode. The block size is " << cur_block_size << std::endl;
                }

                // read the k data blocks
                int num_of_datanodes_involved = placement.k;
                int offset = placement.offsets[i];
                left_value_len -= (placement.k - offset) * cur_block_size;
                if(left_value_len < 0)
                {
                    num_of_datanodes_involved = std::ceil(static_cast<double>(left_value_len) / static_cast<double>(cur_block_size));
                }
                else
                {
                    num_of_datanodes_involved = placement.k - offset;
                }
                int num_of_blocks_each_stripe = placement.k + placement.g + placement.l;
                std::vector<std::thread> readers;
                for (int j = 0; j < num_of_datanodes_involved; j++) {
                    std::pair<std::string, int> ip_and_port_of_datanode =
                        placement.datanode_ip_port[i * num_of_blocks_each_stripe + j + offset];
                    readers.push_back(
                        std::thread([this, i, j, stripe_id, blocks_ptr, cur_block_size, ip_and_port_of_datanode, placement]() {
                            std::string block_id = std::to_string(placement.stripe_ids[i]) + "_" + std::to_string(j);
                            std::string block(cur_block_size, 0);
                            read_from_datanode(block_id.c_str(), block_id.size(),
                                                block.data(), cur_block_size,
                                                ip_and_port_of_datanode.first.c_str(),
                                                ip_and_port_of_datanode.second);

                            mutex_.lock();
                            (*blocks_ptr)[j] = block;
                            mutex_.unlock();
                        }));
                }
                for (auto j = 0; j < readers.size(); j++) {
                    readers[j].join();
                }

                my_assert(blocks_ptr->size() == num_of_datanodes_involved);

                for (int j = 0; j < placement.k; j++) {
                    object_value += (*blocks_ptr)[j];
                }
            }

            asio::ip::tcp::socket socket_(io_context_);
            asio::ip::tcp::endpoint endpoint(asio::ip::make_address(placement.client_ip), placement.client_port);
            socket_.connect(endpoint);

            asio::write(socket_, asio::buffer(placement.key, placement.key.size()));
            asio::write(socket_, asio::buffer(object_value, object_value.size()));

            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);
        };
        try
        {
            std::thread new_thread(decode_and_transfer);
            new_thread.detach();
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }

    bool Proxy::delete_blocks(delete_plan delete_info)
    {
        auto delete_blocks_in_stripe = [this, delete_info]() {
            my_assert(delete_info.block_ids.size() == delete_info.datanode_ip_port.size());
            int num_of_blocks_to_delete = delete_info.block_ids.size(); 
            std::vector<std::thread> deleters;
            for(int i = 0; i < num_of_blocks_to_delete; i++)
            {
                std::pair<std::string, int> ip_and_port_of_datanode = delete_info.datanode_ip_port[i];
                std::string block_id = delete_info.block_ids[i];
                deleters.push_back(
                    std::thread([this, block_id, ip_and_port_of_datanode](){
                        delete_in_datanode(block_id, ip_and_port_of_datanode.first.c_str(), ip_and_port_of_datanode.second);
                }));
            }
            for(int i = 0; i < num_of_blocks_to_delete; i++)
            {
                deleters[i].join();
            }
        };
        try
        {
            std::thread new_thread(delete_blocks_in_stripe);
            new_thread.join();
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        
        return true;
    }

    void Proxy::migrate_blocks(migration_plan migration_info)
    {
        auto migrate_a_block = [this, migration_info](int i) mutable
        {
            block_migration(migration_info.block_ids[i].c_str(), 
                            migration_info.block_ids[i].size(),
                            migration_info.block_sizes[i],
                            migration_info.src_nodes[i].first.c_str(),
                            migration_info.src_nodes[i].second,
                            migration_info.dst_nodes[i].first.c_str(),
                            migration_info.dst_nodes[i].second);
            delete_in_datanode(migration_info.block_ids[i].c_str(),
                               migration_info.src_nodes[i].first.c_str(),
                               migration_info.src_nodes[i].second);
        };
        try
        {
            std::vector<std::thread> migrators;
            int num_of_blocks = int(migration_info.block_ids.size());
            for(int i = 0; i < num_of_blocks; i++)
            {
                migrators.push_back(std::thread(migrate_a_block, i));
            }
            for(int i = 0; i < num_of_blocks; i++)
            {
                migrators[i].join();
            }
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
    }
}