#ifndef CLIENT_H
#define CLIENT_H

#include "coordinator.h"
#include "utils.h"
#include <ylt/coro_rpc/coro_rpc_client.hpp>

namespace ECProject
{
    class Client {
    public:
        Client(std::string ip, int port, std::string coordinator_ip, int coordinator_port);
        ~Client();

        void set_ec_parameters(ECSchema ec_schema);
        // set
        double set(std::string key, std::string value);
        // get
        std::string get(std::string key);
        // delete
        void delete_stripe(unsigned int stripe_id);
        void delete_all_stripes();
        // repair
        repair_resp nodes_repair(std::vector<unsigned int> failed_node_ids);
        repair_resp blocks_repair(std::vector<unsigned int> failed_block_ids, int stripe_id);
        // get biases
        bias_info get_bias();
        // migration
        double check_load_balance_and_migration(double new_beta, double storage_bias_threshold, double network_bias_threshold);

        private:
        std::unique_ptr<coro_rpc::coro_rpc_client> rpc_coordinator_{nullptr};
        int port_;
        std::string ip_;
        std::string coordinator_ip_;
        int coordinator_port_;
        asio::io_context io_context_{};
        asio::ip::tcp::acceptor acceptor_;
    };
}

#endif