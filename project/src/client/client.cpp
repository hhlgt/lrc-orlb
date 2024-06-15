#include "client.h"
#include <unistd.h>

namespace ECProject
{
    Client::Client(std::string ip, int port, std::string coordinator_ip, int coordinator_port)
    : ip_(ip), port_(port), coordinator_ip_(coordinator_ip), coordinator_port_(coordinator_port),
      acceptor_(io_context_, asio::ip::tcp::endpoint( asio::ip::address::from_string(ip.c_str()), port_)) 
    {
        rpc_coordinator_ = std::make_unique<coro_rpc::coro_rpc_client>();
        async_simple::coro::syncAwait(rpc_coordinator_->connect(coordinator_ip_, std::to_string(coordinator_port_)));
    }

    Client::~Client() { acceptor_.close(); }

    void Client::set_ec_parameters(ECSchema ec_schema)
    {
        async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::set_erasure_coding_parameters>(ec_schema));
    }

    double Client::set(std::string key, std::string value)
    {
        std::vector<std::pair<std::string, size_t>> objects;
        objects.push_back({key, value.size()});
        auto response =
            async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::request_set>(objects)).value();
        
        std::cout << "[SET] Send " << key << " to proxy_address:" << response.proxy_ip << ":" << response.proxy_port << std::endl;

        double encoding_time = 0;
        if(!IF_SIMULATION)
        {
            asio::ip::tcp::socket socket_(io_context_);
            asio::ip::tcp::endpoint endpoint(asio::ip::make_address(response.proxy_ip), response.proxy_port);
            socket_.connect(endpoint);

            asio::write(socket_, asio::buffer(key, key.size()));
            asio::write(socket_, asio::buffer(value, value.size()));

            std::vector<unsigned char> encoding_time_buf(sizeof(double));
            asio::read(socket_, asio::buffer(encoding_time_buf, encoding_time_buf.size()));
            encoding_time = bytes_to_double(encoding_time_buf);

            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);

            std::vector<std::string> keys;
            keys.push_back(key);
            async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::commit_object>(keys));
        }

        return encoding_time;
    }

    std::string Client::get(std::string key)
    {
        size_t value_len = 
            async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::request_get>(key, ip_, port_)).value();

        std::string key_buf(key.size(), 0);
        std::string value_buf(value_len, 0);

        if(!IF_SIMULATION)
        {
            asio::ip::tcp::socket socket_(io_context_);
            acceptor_.accept(socket_);

            
            my_assert(key_buf.size() == key.size());
            my_assert(value_buf.size() == value_len);

            size_t read_len_of_key = asio::read(socket_, asio::buffer(key_buf.data(), key_buf.size()));
            my_assert(read_len_of_key == key.size() && key_buf == key);

            size_t read_len_of_value = asio::read(socket_, asio::buffer(value_buf.data(), value_buf.size()));
            my_assert(read_len_of_value == value_len);

            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);

            std::cout << "[GET] get key: " << key_buf.data() << ", valuesize: " << value_len << std::endl;
        }

        return value_buf;
    }

    // delete object

    void Client::delete_stripe(unsigned int stripe_id)
    {
        std::vector<unsigned int> stripe_ids;
        stripe_ids.push_back(stripe_id);
        async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::request_delete_by_stripe>(stripe_ids));
        std::cout << "[DEL] deleting Stripe " << stripe_id << std::endl;
    }

    void Client::delete_all_stripes()
    {
        auto stripe_ids = async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::list_stripes>()).value();
        async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::request_delete_by_stripe>(stripe_ids));
        for(auto it = stripe_ids.begin(); it != stripe_ids.end(); it++)
        {
            std::cout << "[DEL] deleting Stripe " << *it << std::endl;
        }
    }

    repair_resp Client::nodes_repair(std::vector<unsigned int> failed_node_ids)
    {
       auto response = async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::request_repair>(failed_node_ids, -1)).value();
       return response;
    }

    repair_resp Client::blocks_repair(std::vector<unsigned int> failed_block_ids, int stripe_id)
    {
        auto response = async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::request_repair>(failed_block_ids, stripe_id)).value();
        return response;
    }

    bias_info Client::get_bias()
    {
        auto response = async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::compute_biases>()).value();
        return response;
    }

    double Client::check_load_balance_and_migration(double new_beta, double rack_storage_bias_threshold, double rack_network_bias_threshold,
                                                    double node_storage_bias_threshold, double node_network_bias_threshold)
    {
        async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::check_load_balance>(new_beta, 
                                                            rack_storage_bias_threshold, rack_network_bias_threshold, 
                                                            node_storage_bias_threshold, node_network_bias_threshold));
        double migration_time = 0;
        while(migration_time == 0)
        {
            sleep(2);
            migration_time = async_simple::coro::syncAwait(rpc_coordinator_->call<&Coordinator::check_migration>()).value();
        }
        return migration_time;
    }
}