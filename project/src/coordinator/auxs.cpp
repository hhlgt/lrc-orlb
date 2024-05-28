#include "coordinator.h"

namespace ECProject
{
    void Coordinator::init_cluster_info()
    {
        unsigned int cluster_id = 0;
        unsigned int rack_id = 0;
        for(int i = 0; i < num_of_racks_; i += num_of_racks_per_cluster_, cluster_id++)
        {
            cluster_table_[cluster_id].cluster_id = cluster_id;
            for(int j = i; j < num_of_racks_ && j < i + num_of_racks_per_cluster_; j++, rack_id++)
            {
                cluster_table_[cluster_id].racks.push_back(rack_id);
                rack_table_[rack_id].map2cluster = cluster_id;
            }
        }
        num_of_clusters_ = cluster_id;
    }

    void Coordinator::init_rack_info()
    {
        // std::vector<double> storages = {16, 16, 16, 16, 16, 16, 16, 16, 64, 64,

        //                                 16, 16, 16, 16, 16, 16, 16, 16, 64, 64,

        //                                 16, 16, 16, 16, 16, 16, 16, 16, 64, 64,

        //                                 16, 16, 16, 16, 16, 16, 16, 16, 64, 64};
        // std::vector<double> bandwidths = {
        //     1, 1, 1, 1, 1, 1, 1, 1, 10, 10,

        //     1, 1, 1, 1, 1, 1, 1, 1, 10, 10,

        //     1, 1, 1, 1, 1, 1, 1, 1, 10, 10,

        //     1, 1, 1, 1, 1, 1, 1, 1, 10, 10,
        // };

        tinyxml2::XMLDocument xml;
        xml.LoadFile(config_path_.c_str());
        tinyxml2::XMLElement *root = xml.RootElement();
        unsigned int node_id = 0;
        num_of_racks_ = 0;

        for (tinyxml2::XMLElement *rack = root->FirstChildElement(); 
             rack != nullptr; rack = rack->NextSiblingElement()) 
        {
            unsigned int rack_id(std::stoi(rack->Attribute("id")));
            std::string proxy(rack->Attribute("proxy"));

            rack_table_[rack_id].rack_id = rack_id;
            auto pos = proxy.find(':');
            rack_table_[rack_id].proxy_ip = proxy.substr(0, pos);
            rack_table_[rack_id].proxy_port = std::stoi(proxy.substr(pos + 1, proxy.size()));

            num_of_nodes_per_rack_ = 0;
            for (tinyxml2::XMLElement *node = rack->FirstChildElement()->FirstChildElement();
                node != nullptr; node = node->NextSiblingElement()) 
            {
                rack_table_[rack_id].nodes.push_back(node_id);

                std::string node_uri(node->Attribute("uri"));
                node_table_[node_id].node_id = node_id;
                auto pos = node_uri.find(':');
                node_table_[node_id].node_ip = node_uri.substr(0, pos);
                node_table_[node_id].node_port = std::stoi(node_uri.substr(pos + 1, node_uri.size()));
                node_table_[node_id].map2rack = rack_id;

                // node_table_[node_id].storage = storages[node_id];
                // node_table_[node_id].bandwidth = bandwidths[node_id];
                // node_table_[node_id].storage_cost = 0;
                // node_table_[node_id].network_cost = 0;

                node_id++;
                num_of_nodes_per_rack_++;
            }
            num_of_racks_++;
        }
    }
    
    void Coordinator::init_load_info()
    {
        unsigned int num_of_nodes = (unsigned int)(num_of_racks_ * num_of_nodes_per_rack_);
        for(unsigned int node_id = 0; node_id < num_of_nodes; node_id++)
        {
            double storage = 16;
            double bandwidth = 1;
            if(node_id % num_of_nodes_per_rack_ > 0.8 * num_of_nodes_per_rack_)
            {
                storage = 64;
                bandwidth = 10;
            }
            node_table_[node_id].storage = storage * 1024 * 1024 * 1024 * 1024 / (double)ec_schema_.block_size;
            node_table_[node_id].bandwidth = bandwidth * 1024 * 1024 * 128 / (double)ec_schema_.block_size;
            node_table_[node_id].storage_cost = 0;
            node_table_[node_id].network_cost = 0;
        }
    }

    void Coordinator::init_proxy_info()
    {
        for (auto cur = rack_table_.begin(); cur != rack_table_.end(); cur++) {
            std::string proxy_ip = cur->second.proxy_ip;
            int proxy_port = cur->second.proxy_port;
            std::string location = proxy_ip + std::to_string(proxy_port);
            my_assert(proxies_.contains(location) == false);

            proxies_[location] = std::make_unique<coro_rpc::coro_rpc_client>();
            if(!IF_SIMULATION)
            {
                async_simple::coro::syncAwait(proxies_[location]->connect(proxy_ip, std::to_string(proxy_port)));
                auto msg = async_simple::coro::syncAwait(proxies_[location]->call<&Proxy::checkalive>("hello"));
                if(msg != "hello")
                {
                    std::cout << "[Proxy Check] failed to connect " << location << std::endl;
                }
            }
        }
    }

    void Coordinator::reset_metadata()
    {
        cur_stripe_id_ = 0;
        commited_object_table_.clear();
        updating_object_table_.clear();
        stripe_table_.clear();
        for(auto it = node_table_.begin(); it != node_table_.end(); it++)
        {
            Node &node = it->second;
            node.stripes_blocks.clear();
        }
    }

    void Coordinator::init_placement_info(placement_info &placement, std::string key, 
                                          size_t value_len, size_t block_size, 
                                          size_t tail_block_size) 
    {
        placement.ec_type = ec_schema_.ec_type;
        placement.key = key;
        placement.value_len = value_len;
        placement.k = ec_schema_.k;
        placement.l = ec_schema_.l;
        placement.g = ec_schema_.g;
        placement.block_size = block_size;
        placement.tail_block_size = tail_block_size;
    }

    Stripe &Coordinator::new_stripe(size_t block_size)
    {
        Stripe temp;
        temp.stripe_id = cur_stripe_id_++;
        stripe_table_[temp.stripe_id] = temp;
        Stripe &stripe = stripe_table_[temp.stripe_id];
        stripe.ec_type = ec_schema_.ec_type;
        stripe.k = ec_schema_.k;
        stripe.l = ec_schema_.l;
        stripe.g = ec_schema_.g;
        stripe.block_size = block_size;

        return stripe_table_[temp.stripe_id];
    }

    void Coordinator::find_out_blocks_in_rack(unsigned int rack_id, 
                        std::unordered_map<unsigned int, std::vector<int>> &blocks_in_stripes)
    {
        for(auto node_id : rack_table_[rack_id].nodes) {
            Node &node = node_table_[node_id];
            for(auto it = node.stripes_blocks.begin(); it != node.stripes_blocks.end(); it++)
            {
                if(blocks_in_stripes.find(it->first) == blocks_in_stripes.end())
                {
                    blocks_in_stripes[it->first] = std::vector<int>({it->second});
                }
                else
                {
                    blocks_in_stripes[it->first].push_back(it->second);
                }
            }
        }
    }

    void Coordinator::find_out_stripe_placement(unsigned int stripe_id, 
                        std::unordered_map<unsigned int, std::vector<int>> &blocks_in_racks)
    {
        Stripe &stripe = stripe_table_[stripe_id];
        for(int i = 0; i < stripe.k + stripe.l + stripe.g; i++)
        {
            unsigned int node_id = stripe.blocks2nodes[i];
            unsigned int rack_id = node_table_[node_id].map2rack;
            if(blocks_in_racks.find(rack_id) == blocks_in_racks.end())
            {
                blocks_in_racks[rack_id] = std::vector<int>({i});
            }
            else
            {
                blocks_in_racks[rack_id].push_back(i);
            }
        }

        std::cout << "Data placement result of stripe " << stripe.stripe_id << ": " << std::endl;
        for(auto it = blocks_in_racks.begin(); it != blocks_in_racks.end(); it++)
        {
            std::cout << "Rack " << it->first << ": ";
            std::vector<int> &blocks = it->second;
            for(auto it_ = blocks.begin(); it_ != blocks.end(); it_++)
            {
                std::cout << *it_ << "[N" << stripe.blocks2nodes[*it_] << "] ";
            }
            std::cout << std::endl;
        }
    }

    bool Coordinator::if_stripe_in_rack(unsigned int stripe_id, unsigned int rack_id)
    {
        Rack &rack = rack_table_[rack_id];
        for(auto node_id : rack.nodes) {
            Node &node = node_table_[node_id];
            if(node.stripes_blocks.find(stripe_id) != node.stripes_blocks.end())
            {
                return true;
            }
        }
        return false;
    }

    bool Coordinator::if_stripe_in_node(unsigned int stripe_id, unsigned int node_id)
    {
        Node &node = node_table_[node_id];
        if(node.stripes_blocks.find(stripe_id) != node.stripes_blocks.end())
        {
            return true;
        }
        return false;
    }
}