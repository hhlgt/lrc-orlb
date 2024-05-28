#ifndef DATANODE_H
#define DATANODE_H

#include "utils.h"
#include <asio.hpp>
#include <fstream>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <sw/redis++/redis++.h>

namespace ECProject
{
    class Datanode
    {
    public:
        Datanode(std::string ip, int port);
        ~Datanode();
        void run();

        // rpc调用
        bool checkalive();
        // set
        void handle_set(std::string src_ip, int src_port, bool ispull);
        // get
        void handle_get(std::string key, size_t key_len, size_t value_len, bool toproxy);
        // delete
        void handle_delete(std::string block_key);
        // simulate cross-cluster transfer
        void handle_transfer();

    private:
        std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_{nullptr};
        std::string ip_;
        int port_;
        int port_for_transfer_data_;
        asio::io_context io_context_{};
        asio::ip::tcp::acceptor acceptor_;
        std::unique_ptr<sw::redis::Redis> redis_{nullptr};
    };
}

#endif