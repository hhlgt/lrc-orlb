#ifndef COORDINATOR_H
#define COORDINATOR_H

#include "tinyxml2.h"
#include "utils.h"
#include "proxy.h"
#include "lrc.h"
#include <mutex>
#include <condition_variable>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace ECProject
{
    class Coordinator
    {
    public:
        Coordinator(std::string ip, int port, std::string config_path, double beta);
        ~Coordinator();

        void run();
        // rpc调用, coordinator.cpp
        std::string checkalive(std::string msg);
        // set parameters
        void set_erasure_coding_parameters(ECSchema ec_schema);
        // set, return proxy's ip and port
        set_resp request_set(std::vector<std::pair<std::string, size_t>> objects);
        void commit_object(std::vector<std::string> keys);
        // get, return size of value
        size_t request_get(std::string key, std::string client_ip, int client_port);
        // delete
        void request_delete_by_stripe(std::vector<unsigned int> stripe_ids);
        // repair, repair a list of blocks in specified stripes (stripe_id>=0) or nodes (stripe_id=-1)
        repair_resp request_repair(std::vector<unsigned int> failed_ids, int stripe_id);
        // migration for load balance
        void check_load_balance(double new_beta, double rack_storage_bias_threshold, double rack_network_bias_threshold,
                                  double node_storage_bias_threshold, double node_network_bias_threshold);
        // others
        std::vector<unsigned int> list_stripes();
        // rack_storage_bias, rack_network_bias, node_storage_bias, node_network_bias
        bias_info compute_biases();
        // check if migration finish
        double check_migration();

    private:
        // aux.cpp
        void init_cluster_info();
        void init_rack_info();
        void init_proxy_info();
        void init_load_info();
        void reset_metadata();

        void init_placement_info(placement_info &placement, std::string key, size_t value_len, size_t block_size, size_t tail_block_size);
        Stripe &new_stripe(size_t block_size);
        void find_out_blocks_in_rack(unsigned int rack_id, std::unordered_map<unsigned int, std::vector<int>> &blocks_in_stripes);
        void find_out_stripe_placement(unsigned int stripe_id, std::unordered_map<unsigned int, std::vector<int>> &blocks_in_racks);
        bool if_stripe_in_rack(unsigned int stripe_id, unsigned int rack_id);
        bool if_stripe_in_node(unsigned int stripe_id, unsigned int node_id);

        // placement.cpp
        // placement: partition -> place, a partition in a seperate region(rack/cluster)
        void generate_placement_for_LRC(unsigned int stripe_id);
        // partition strategy
        std::vector<std::vector<int>> lrc_partition_strategy_flat(int k, int g, int l);
        std::vector<std::vector<int>> lrc_partition_strategy_random_random(int k, int g, int l);
        std::vector<std::vector<int>> lrc_partition_strategy_random_single_region(int k, int g, int l);
        std::vector<std::vector<int>> lrc_partition_strategy_random_t_regions(int k, int g, int l, int t);
        std::vector<std::vector<int>> lrc_partition_strategy_ecwide_single_region(int k, int g, int l);
        std::vector<std::vector<int>> lrc_partition_strategy_opt_single_region(ECTYPE lrc_type, int k, int g, int l);
        std::vector<std::vector<int>> lrc_partition_strategy_opt_t_regions(ECTYPE lrc_type, int k, int g, int l, int t);
        // rack and node selection
        void select_by_random(std::vector<std::vector<int>> &partition_plan, unsigned int stripe_id);
        void select_by_load(std::vector<std::vector<int>> &partition_plan, unsigned int stripe_id);
        unsigned int select_a_node_by_load(std::vector<unsigned int> node_lists);

        void compute_avg_cost_for_each_node_and_rack(
                double &node_avg_storage_cost, double &node_avg_network_cost,
                double &rack_avg_storage_cost, double &rack_avg_network_cost);
        void compute_total_cost_for_rack(Rack &rack,
                                            double &storage_cost,
                                            double &network_cost);
        void compute_avg_cost_and_bias_on_node_level(
                double &node_avg_storage_cost, double &node_avg_network_cost,
                double &node_storage_bias, double &node_network_bias);
        void compute_avg_cost_and_bias_on_rack_level(
                double &rack_avg_storage_cost, double &rack_avg_network_cost,
                double &rack_storage_bias, double &rack_network_bias);

        // repair.cpp
        // for repair
        void check_out_failures(int stripe_id, std::vector<unsigned int> failed_ids,
                                std::unordered_map<unsigned int, std::vector<int>> &failure_map,
                                std::unordered_map<unsigned int, ECProject::FailureType> &failures_type);
        void do_repair(std::vector<unsigned int> failed_ids, int stripe_id, 
                       double &repair_time, double &decoding_time, int &cross_rack_num);
        void local_repair_plan(int failed_index, unsigned int stripe_id, 
                                std::unordered_map<unsigned int, std::vector<int>> &help_info);
        void global_repair_plan(std::vector<int> failed_indexs, unsigned int stripe_id, 
                                std::unordered_map<unsigned int, std::vector<int>> &help_info);
        bool generate_plan_for_a_local_or_global_repair_LRC(std::vector<main_repair_plan> &main_repair, std::vector<std::vector<help_repair_plan>> &help_repair, unsigned int stripe_id, std::vector<int> &failed_blocks, bool local_repair);
        bool generate_repair_plan_for_multi_blocks_LRC(std::vector<main_repair_plan> &main_repair, std::vector<std::vector<help_repair_plan>> &help_repair, unsigned int stripe_id, std::vector<int> &failed_blocks);
        void simulation_repair(std::vector<main_repair_plan> &main_repair, int &cross_rack_num);
        
        // migration.cpp
        void do_migration_on_rack_level(double storage_bias_threshold, double network_bias_threshold);
        void do_migration_on_node_level_inside_rack(double storage_bias_threshold, double network_bias_threshold);

        std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_{nullptr};
        std::unordered_map<std::string, std::unique_ptr<coro_rpc::coro_rpc_client>> proxies_;
        ECSchema ec_schema_;
        std::unordered_map<unsigned int, Cluster> cluster_table_;
        std::unordered_map<unsigned int, Rack> rack_table_;
        std::unordered_map<unsigned int, Node> node_table_;
        std::unordered_map<unsigned int, Stripe> stripe_table_;
        std::unordered_map<std::string, object_info> commited_object_table_;
        std::unordered_map<std::string, object_info> updating_object_table_;

        std::mutex mutex_;
        std::condition_variable cv_;
        unsigned int cur_stripe_id_;
        int num_of_clusters_;
        int num_of_racks_per_cluster_;
        int num_of_racks_;
        int num_of_nodes_per_rack_;
        std::string ip_;
        int port_;
        std::string config_path_;
        double alpha_;
        double beta_;
        double gama_;
        double time_;
    };
}

#endif