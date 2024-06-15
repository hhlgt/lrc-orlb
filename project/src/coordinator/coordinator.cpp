#include "coordinator.h"
#include "tinyxml2.h"

namespace ECProject
{
    Coordinator::Coordinator(std::string ip, int port, std::string config_file_path, double beta)
        : ip_(ip), port_(port), config_path_(config_file_path), alpha_(beta), beta_(beta) 
    {
        rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(4, port_);
        rpc_server_->register_handler<&Coordinator::checkalive>(this);
        rpc_server_->register_handler<&Coordinator::set_erasure_coding_parameters>(this);
        rpc_server_->register_handler<&Coordinator::request_set>(this);
        rpc_server_->register_handler<&Coordinator::commit_object>(this);
        rpc_server_->register_handler<&Coordinator::request_get>(this);
        rpc_server_->register_handler<&Coordinator::request_delete_by_stripe>(this);
        rpc_server_->register_handler<&Coordinator::request_repair>(this);
        rpc_server_->register_handler<&Coordinator::check_load_balance>(this);
        rpc_server_->register_handler<&Coordinator::list_stripes>(this);
        rpc_server_->register_handler<&Coordinator::compute_biases>(this);
        rpc_server_->register_handler<&Coordinator::check_migration>(this);

        gama_ = 0.1;
        cur_stripe_id_ = 0;
        time_ = 0;
        
        init_rack_info();
        init_proxy_info();
        if(IF_THREE_HIERARCHY)
        {
            num_of_racks_per_cluster_ = 3;
            init_cluster_info();
        }
    }
    Coordinator::~Coordinator() { rpc_server_->stop(); }

    void Coordinator::run() { auto err = rpc_server_->start(); }

    std::string Coordinator::checkalive(std::string msg) 
    { 
        return msg + " Hello, it's me. The coordinator!"; 
    }

    void Coordinator::set_erasure_coding_parameters(ECSchema ec_schema) {
        ec_schema_ = ec_schema;
        init_load_info();
        reset_metadata();
    }

    set_resp Coordinator::request_set(std::vector<std::pair<std::string, size_t>> objects)
    {
        int num_of_objects = (int)objects.size();
        my_assert(num_of_objects > 0);
        mutex_.lock();
        for(int i = 0; i < num_of_objects; i++)
        {
            std::string key = objects[i].first;
            if (commited_object_table_.contains(key)) {
                mutex_.unlock();
                my_assert(false);
            }
        }
        mutex_.unlock();

        size_t total_value_len = 0;
        std::vector<object_info> new_objects;
        for(int i = 0; i < num_of_objects; i++)
        {
            // currently only support small objects (size < stripe size) with regular size, for cross-object striping
            my_assert(objects[i].second % ec_schema_.block_size == 0); 
            object_info new_object;
            new_object.value_len = objects[i].second;
            total_value_len += new_object.value_len;
            new_objects.push_back(new_object);
        }

        if(IF_DEBUG)
        {
            std::cout << "[SET] Ready to process " << num_of_objects << " objects. Each with length of " << 
                      total_value_len / num_of_objects << std::endl;
        }

        placement_info placement;

        size_t stripe_len = ec_schema_.k * ec_schema_.block_size;
        if(stripe_len >= total_value_len)
        {
            init_placement_info(placement, objects[0].first, total_value_len, ec_schema_.block_size, -1);

            auto &stripe = new_stripe(ec_schema_.block_size);
            for(int i = 0; i < num_of_objects; i++)
            {
                stripe.objects.push_back(objects[i].first);
                new_objects[i].stripes.push_back(stripe.stripe_id);
            }

            if(IF_DEBUG)
            {
                std::cout << "[SET] Ready to data placement for stripe " << stripe.stripe_id << std::endl;
            }

            if(check_ec_family(placement.ec_type) == LRCs)
            {
                generate_placement_for_LRC(stripe.stripe_id);
            }

            if(IF_DEBUG)
            {
                std::cout << "[SET] Finish data placement for stripe " << stripe.stripe_id << std::endl;
            }

            placement.stripe_ids.push_back(stripe.stripe_id);
            for (auto &node_id : stripe.blocks2nodes) {
                auto &node = node_table_[node_id];
                placement.datanode_ip_port.push_back({node.node_ip, node.node_port});
            }
        }
        else
        {
            init_placement_info(placement, objects[0].first, total_value_len, ec_schema_.block_size, -1);
            
            int num_of_stripes = total_value_len / stripe_len;
            size_t left_value_len = total_value_len;
            int cumulative_len = 0, obj_idx = 0;
            for(int i = 0; i < num_of_stripes; i++)
            {
                left_value_len -= stripe_len;
                auto &stripe = new_stripe(ec_schema_.block_size);
                while(cumulative_len < stripe_len)
                {
                    stripe.objects.push_back(objects[obj_idx].first);
                    new_objects[obj_idx].stripes.push_back(stripe.stripe_id);
                    cumulative_len += objects[obj_idx].second;
                    obj_idx++;
                }

                if(IF_DEBUG)
                {
                    std::cout << "[SET] Ready to data placement for stripe " << stripe.stripe_id << std::endl;
                }

                if(check_ec_family(placement.ec_type) == LRCs)
                {
                    generate_placement_for_LRC(stripe.stripe_id);
                }

                if(IF_DEBUG)
                {
                    std::cout << "[SET] Finish data placement for stripe " << stripe.stripe_id << std::endl;
                }

                placement.stripe_ids.push_back(stripe.stripe_id);
                for (auto &node_id : stripe.blocks2nodes) {
                    auto &node = node_table_[node_id];
                    placement.datanode_ip_port.push_back({node.node_ip, node.node_port});
                }
            }
            if(left_value_len > 0)
            {
                size_t tail_block_size = std::ceil(static_cast<double>(left_value_len) /
                                         static_cast<double>(ec_schema_.k));
                tail_block_size = 64 * std::ceil(static_cast<double>(tail_block_size) / 64.0);
                placement.tail_block_size = tail_block_size;
                auto &stripe = new_stripe(tail_block_size);
                if(cumulative_len > stripe_len) // for object cross stripe
                {
                    stripe.objects.push_back(objects[obj_idx - 1].first);
                    new_objects[obj_idx - 1].stripes.push_back(stripe.stripe_id);
                }
                while(obj_idx < num_of_objects)
                {
                    stripe.objects.push_back(objects[obj_idx].first);
                    new_objects[obj_idx].stripes.push_back(stripe.stripe_id);
                    cumulative_len += objects[obj_idx].second;
                    obj_idx++;
                }
                my_assert(cumulative_len == total_value_len);

                if(IF_DEBUG)
                {
                    std::cout << "[SET] Ready to data placement for stripe " << stripe.stripe_id << std::endl;
                }

                if(check_ec_family(placement.ec_type) == LRCs)
                {
                    generate_placement_for_LRC(stripe.stripe_id);
                }

                if(IF_DEBUG)
                {
                    std::cout << "[SET] Finish data placement for stripe " << stripe.stripe_id << std::endl;
                }
                
                placement.stripe_ids.push_back(stripe.stripe_id);
                for (auto &node_id : stripe.blocks2nodes) {
                    auto &node = node_table_[node_id];
                    placement.datanode_ip_port.push_back(std::make_pair(node.node_ip, node.node_port));
                }
            }
        }

        mutex_.lock();
        for(int i = 0; i < num_of_objects; i++)
        {
            updating_object_table_[objects[i].first] = new_objects[i];
        }
        mutex_.unlock();

        unsigned int node_id = stripe_table_[cur_stripe_id_ - 1].blocks2nodes[0];
        unsigned int selected_rack_id = node_table_[node_id].map2rack;
        std::string selected_proxy_ip = rack_table_[selected_rack_id].proxy_ip;
        int selected_proxy_port = rack_table_[selected_rack_id].proxy_port;

        if(IF_DEBUG)
        {
            std::cout << "[SET] Select proxy " << selected_proxy_ip << ":" << selected_proxy_port << " in rack "
                      << selected_rack_id << " to handle encode and set!" << std::endl;
        }

        if(IF_SIMULATION) // simulation, commit object
        {
            mutex_.lock();
            for(int i = 0; i < num_of_objects; i++)
            {
                my_assert(commited_object_table_.contains(objects[i].first) == false &&
                            updating_object_table_.contains(objects[i].first) == true);
                commited_object_table_[objects[i].first] = updating_object_table_[objects[i].first];
                updating_object_table_.erase(objects[i].first);
            }
            mutex_.unlock();
        }
        else
        {
            async_simple::coro::syncAwait(proxies_[selected_proxy_ip + std::to_string(selected_proxy_port)]
                                                           ->call<&Proxy::encode_and_store_object>(placement));
        }

        set_resp response;
        response.proxy_ip = selected_proxy_ip;
        response.proxy_port = selected_proxy_port + 500; // port for transfer data

        return response;
    }

    void Coordinator::commit_object(std::vector<std::string> keys)
    {
        int num = (int)keys.size();
        mutex_.lock();
        for(int i = 0; i < num; i++)
        {
            my_assert(commited_object_table_.contains(keys[i]) == false &&
                        updating_object_table_.contains(keys[i]) == true);
            commited_object_table_[keys[i]] = updating_object_table_[keys[i]];
            updating_object_table_.erase(keys[i]);
        }
        mutex_.unlock();
    }

    size_t Coordinator::request_get(std::string key, std::string client_ip, int client_port)
    {
        mutex_.lock();
        if (commited_object_table_.contains(key) == false) {
            mutex_.unlock();
            my_assert(false);
        }
        object_info &object = commited_object_table_[key];
        mutex_.unlock();

        placement_info placement;
        if (ec_schema_.block_size * ec_schema_.k >= object.value_len) {
            init_placement_info(placement, key, object.value_len, ec_schema_.block_size, -1);
        } else {
            size_t tail_block_size = -1;
            if (object.value_len % (ec_schema_.k * ec_schema_.block_size) != 0) {
                size_t tail_stripe_size = object.value_len % (ec_schema_.k * ec_schema_.block_size);
                tail_block_size = std::ceil(static_cast<double>(tail_stripe_size) / static_cast<double>(ec_schema_.k));
                tail_block_size = 64 * std::ceil(static_cast<double>(tail_block_size) / 64.0);
            }
            init_placement_info(placement, key, object.value_len, ec_schema_.block_size, tail_block_size);
        }

        for(auto stripe_id : object.stripes)   
        {
            Stripe &stripe = stripe_table_[stripe_id];
            placement.stripe_ids.push_back(stripe_id);

            int num_of_object_in_a_stripe = (int)stripe.objects.size();
            int offset = 0;
            for(int i = 0; i < num_of_object_in_a_stripe; i++)
            {
                if(stripe.objects[i] != key)
                {
                    int t_object_len = commited_object_table_[stripe.objects[i]].value_len;
                    offset += t_object_len / stripe.block_size;     // must be block_size of stripe
                }
                else
                {
                    break;
                }
            }
            placement.offsets.push_back(offset);

            for (auto node_id : stripe.blocks2nodes) {
                Node &node = node_table_[node_id];
                node_table_[node_id].network_cost += 1;
                placement.datanode_ip_port.push_back({node.node_ip, node.node_port});
            }
        }

        if(!IF_SIMULATION)
        {
            placement.client_ip = client_ip;
            placement.client_port = client_port;
            int selected_proxy_id = random_index(rack_table_.size());
            std::string location = rack_table_[selected_proxy_id].proxy_ip + std::to_string(rack_table_[selected_proxy_id].proxy_port);
            async_simple::coro::syncAwait(proxies_[location]->call<&Proxy::decode_and_get_object>(placement));
        }

        return object.value_len;
    }

    void Coordinator::request_delete_by_stripe(std::vector<unsigned int> stripe_ids)
    {
        std::unordered_set<std::string> objects_key;
        int num_of_stripes = (int)stripe_ids.size();
        delete_plan delete_info;
        for(int i = 0; i < num_of_stripes; i++)
        {
            auto &stripe = stripe_table_[stripe_ids[i]];
            for(auto key : stripe.objects)
            {
                objects_key.insert(key);
            }
            int num_of_nodes = stripe.k + stripe.g + stripe.l;
            my_assert(num_of_nodes == (int)stripe.blocks2nodes.size());
            for(int j = 0; j < num_of_nodes; j++)
            {
                std::string key = std::to_string(stripe.stripe_id) + "_" + std::to_string(j);
                delete_info.block_ids.push_back(key);
                Node &node = node_table_[stripe.blocks2nodes[j]];
                node.storage_cost -= 1;
                node.network_cost -= 1;
                delete_info.datanode_ip_port.push_back({node.node_ip, node.node_port});
            }
        }

        if(!IF_SIMULATION)
        {
            int selected_proxy_id = random_index(rack_table_.size());
            std::string location = rack_table_[selected_proxy_id].proxy_ip + std::to_string(rack_table_[selected_proxy_id].proxy_port);
            async_simple::coro::syncAwait(proxies_[location]->call<&Proxy::delete_blocks>(delete_info));
        }

        // update meta data
        for(int i = 0; i < num_of_stripes; i++)
        {
            stripe_table_.erase(stripe_ids[i]);
        }
        mutex_.lock();
        for(auto key : objects_key)
        {
            if(commited_object_table_.contains(key))
                commited_object_table_.erase(key);
        }
        mutex_.unlock();
    }

    std::vector<unsigned int> Coordinator::list_stripes()
    {
        std::vector<unsigned int> stripe_ids;
        for(auto it = stripe_table_.begin(); it != stripe_table_.end(); it++)
        {
            stripe_ids.push_back(it->first);
        }
        return stripe_ids;
    }

    bias_info Coordinator::compute_biases()
    {
        double rack_avg_storage_cost = 0, rack_avg_network_cost = 0;
        double rack_storage_bias = 0, rack_network_bias = 0;
        compute_avg_cost_and_bias_on_rack_level(rack_avg_storage_cost, rack_avg_network_cost,
                                                rack_storage_bias, rack_network_bias);
        double node_avg_storage_cost = 0, node_avg_network_cost = 0;
        double node_storage_bias = 0, node_network_bias = 0;
        compute_avg_cost_and_bias_on_node_level(node_avg_storage_cost, node_avg_network_cost,
                                                node_storage_bias, node_network_bias);
        bias_info biases;
        biases.rack_storage_bias = rack_storage_bias;
        biases.rack_network_bias = rack_network_bias;
        biases.node_storage_bias = node_storage_bias;
        biases.node_network_bias = node_network_bias;
        return biases;
    }

    repair_resp Coordinator::request_repair(std::vector<unsigned int> failed_ids, int stripe_id)
    {
        double decoding_time = 0, repair_time = 0;
        int cross_rack_num = 0;
        do_repair(failed_ids, stripe_id, repair_time, decoding_time, cross_rack_num);
        
        repair_resp response;
        response.decoding_time = decoding_time;
        response.repair_time = repair_time;
        response.num_of_blocks_cross_rack = cross_rack_num;
        return response;
    }

    void Coordinator::check_load_balance(double new_beta, 
                        double rack_storage_bias_threshold, double rack_network_bias_threshold,
                        double node_storage_bias_threshold, double node_network_bias_threshold)
    {
        if(new_beta >= 0 && new_beta <= 1)
        {
            beta_ = new_beta;
        }
        auto check_and_migration = [this, rack_storage_bias_threshold, rack_network_bias_threshold, 
                                    node_storage_bias_threshold, node_network_bias_threshold] () mutable
        {
            struct timeval start_time, end_time;
            double migration_time = 0;
            gettimeofday(&start_time, NULL);
            do_migration_on_rack_level(rack_storage_bias_threshold, rack_network_bias_threshold);
            do_migration_on_node_level_inside_rack(node_storage_bias_threshold, node_network_bias_threshold);
            gettimeofday(&end_time, NULL);
            migration_time = end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
            mutex_.lock();
            time_ = migration_time;
            mutex_.unlock();
        };
        try
        {
            std::thread my_thread(check_and_migration);
            my_thread.detach();
        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        
    }

    double Coordinator::check_migration()
    {
        double migration_time = 0;
        mutex_.lock();
        migration_time = time_;
        if(time_ > 0)
        {
            time_ = 0;
        }
        mutex_.unlock();
        return migration_time;
    }
}