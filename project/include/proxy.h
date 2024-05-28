#ifndef PROXY_H
#define PROXY_H

#include "tinyxml2.h"
#include "utils.h"
#include "datanode.h"
#include "lrc.h"
#include <mutex>
#include <condition_variable>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

namespace ECProject
{
    class Proxy
    {
    public:
        Proxy(std::string ip, int port, std::string networkcore, std::string config_path);
        ~Proxy();
        void run();

        // rpc调用
        std::string checkalive(std::string msg);
        // encode and set
        void encode_and_store_object(placement_info placement);
        // decode and get
        void decode_and_get_object(placement_info placement);
        // delete
        bool delete_blocks(delete_plan);
        // repair
        double main_repair(main_repair_plan repair_plan);
        double help_repair(help_repair_plan repair_plan);
        // migration
        void migrate_blocks(migration_plan migration_info);

    private:
        void init_datanodes();
        void write_to_datanode(const char *key, size_t key_len, const char *value, size_t value_len, const char *ip, int port);
        void read_from_datanode(const char *key, size_t key_len, char *value, size_t value_len, const char *ip, int port);
        void delete_in_datanode(std::string block_id, const char *ip, int port);
        void block_migration(const char *key, size_t key_len, size_t value_len, const char *src_ip, int src_port, const char *dsn_ip, int dsn_port);
        void transfer_to_networkcore(const char *value, size_t value_len);

        std::unordered_map<std::string, std::unique_ptr<coro_rpc::coro_rpc_client>> datanodes_;
        std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_{nullptr};
        int self_rack_id_;
        int port_;
        int port_for_transfer_data_;
        std::string ip_;
        std::string networkcore_;
        std::string config_path_;
        asio::io_context io_context_{};
        asio::ip::tcp::acceptor acceptor_;
        std::mutex mutex_;
        std::condition_variable cv;
    };  
}

#endif