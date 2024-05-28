#include "datanode.h"

namespace ECProject
{
    Datanode::Datanode(std::string ip, int port)
        : ip_(ip), port_(port), acceptor_(io_context_, asio::ip::tcp::endpoint(asio::ip::address::from_string(ip.c_str()), port + 500))
    {
        // port is for rpc, port + 500 is for socket, port + 1000 is for redis
        rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(1, port_);
        rpc_server_->register_handler<&Datanode::checkalive>(this);
        rpc_server_->register_handler<&Datanode::handle_set>(this);
        rpc_server_->register_handler<&Datanode::handle_get>(this);
        rpc_server_->register_handler<&Datanode::handle_delete>(this);
        rpc_server_->register_handler<&Datanode::handle_transfer>(this);
        port_for_transfer_data_ = port + 500;
        std::string url = "tcp://" + ip_ + ":" + std::to_string(port_ + 1000);
        redis_ = std::make_unique<sw::redis::Redis>(url);
        
    } 
    Datanode::~Datanode()
    {
        acceptor_.close();
        rpc_server_->stop();
    }

    void Datanode::run()
    {
        auto err = rpc_server_->start();
    }

    bool Datanode::checkalive()
    {
        return true;
    }

    void Datanode::handle_set(std::string src_ip, int src_port, bool ispull)
    {
        auto handler1 = [this]() mutable
        {
            try
            {
                asio::error_code ec;
                asio::ip::tcp::socket socket_(io_context_);
                acceptor_.accept(socket_);

                std::vector<unsigned char> value_or_key_size_buf(sizeof(int));
                asio::read(socket_, asio::buffer(value_or_key_size_buf, value_or_key_size_buf.size()), ec);
                int key_size = bytes_to_int(value_or_key_size_buf);
                asio::read(socket_, asio::buffer(value_or_key_size_buf, value_or_key_size_buf.size()), ec);
                int value_size = bytes_to_int(value_or_key_size_buf);

                std::string key_buf(key_size, 0);
                std::string value_buf(value_size, 0);
                asio::read(socket_, asio::buffer(key_buf.data(), key_buf.size()), ec);
                asio::read(socket_, asio::buffer(value_buf.data(), value_buf.size()), ec);

                if(IN_MEMORY)
                {
                    // on redis
                    redis_->set(key_buf, value_buf);
                }
                else
                {
                    // on disk
                    std::string targetdir = "./storage/" + std::to_string(port_) + "/";
                    std::string writepath = targetdir + key_buf;
                    if (access(targetdir.c_str(), 0) == -1)
                    {
                        mkdir(targetdir.c_str(), S_IRWXU);
                    }
                    std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                    ofs.write(value_buf.data(), value_size);
                    ofs.flush();
                    ofs.close();
                }

                // response
                std::vector<char> finish(1);
                asio::write(socket_, asio::buffer(finish, finish.size()));

                asio::error_code ignore_ec;
                socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket_.close(ignore_ec);

                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << port_ << "][Write] successfully write " << key_buf << " with " << value_size << "bytes" << std::endl;
                }
                
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };
        auto handler2 = [this, src_ip, src_port]() mutable
        {
            try
            {
                asio::ip::tcp::socket socket_(io_context_);
                asio::ip::tcp::resolver resolver(io_context_);
                asio::error_code con_error;
                asio::connect(socket_, resolver.resolve({std::string(src_ip), std::to_string(src_port)}), con_error);
                asio::error_code ec;
                if (!con_error && IF_DEBUG)
                {
                    std::cout << "[Datanode" << port_ << "] Connect to " << src_ip << ":" << src_port << " success!" << std::endl;
                }

                std::vector<unsigned char> value_or_key_size_buf(sizeof(int));
                asio::read(socket_, asio::buffer(value_or_key_size_buf, value_or_key_size_buf.size()), ec);
                int key_size = bytes_to_int(value_or_key_size_buf);
                asio::read(socket_, asio::buffer(value_or_key_size_buf, value_or_key_size_buf.size()), ec);
                int value_size = bytes_to_int(value_or_key_size_buf);

                std::string key_buf(key_size, 0);
                std::string value_buf(value_size, 0);
                asio::read(socket_, asio::buffer(key_buf.data(), key_buf.size()), ec);
                asio::read(socket_, asio::buffer(value_buf.data(), value_buf.size()), ec);

                if(IN_MEMORY)
                {
                    // on redis
                    redis_->set(key_buf, value_buf);
                }
                else
                {
                    // on disk
                    std::string targetdir = "./storage/" + std::to_string(port_) + "/";
                    std::string writepath = targetdir + key_buf;
                    if (access(targetdir.c_str(), 0) == -1)
                    {
                        mkdir(targetdir.c_str(), S_IRWXU);
                    }
                    std::ofstream ofs(writepath, std::ios::binary | std::ios::out | std::ios::trunc);
                    ofs.write(value_buf.data(), value_size);
                    ofs.flush();
                    ofs.close();
                }

                // response
                std::vector<char> finish(1);
                asio::write(socket_, asio::buffer(finish, finish.size()));

                asio::error_code ignore_ec;
                socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
                socket_.close(ignore_ec);

                if (IF_DEBUG)
                {
                    std::cout << "[Datanode" << port_ << "][Write] successfully write " << key_buf << " with " << value_size << "bytes" << std::endl;
                }
            }
            catch (const std::exception &e)
            {
                std::cerr << e.what() << '\n';
            }
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << port_ << "][SET] ready to handle set!" << std::endl;
            }
            if (ispull)
            {
                std::thread my_thread(handler2);
                my_thread.join();
            }
            else
            {
                std::thread my_thread(handler1);
                my_thread.detach();
            }
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
    }

    void Datanode::handle_get(std::string key, size_t key_len, size_t value_len, bool toproxy)
    {
        auto handler1 = [this, key, key_len, value_len]() mutable
        {
            asio::error_code ec;
            asio::ip::tcp::socket socket_(io_context_);
            acceptor_.accept(socket_);

            if(IN_MEMORY)
            {
                // on redis
                if(!redis_->exists(key))
                {
                    std::cout << "[GET] key not found!" << std::endl;
                }
                else
                {
                    auto value_returned = redis_->get(key);
                    my_assert(value_returned.has_value());
                    std::string value_buf = value_returned.value();
                    asio::write(socket_, asio::buffer(value_buf.data(), value_buf.length()));
                }
            }
            else
            {
                // on disk
                std::string targetdir = "./storage/" + std::to_string(port_) + "/";
                std::string readpath = targetdir + key;
                if (access(readpath.c_str(), 0) == -1)
                {
                    std::cout << "[Datanode" << port_ << "][Read] file does not exist!" << readpath << std::endl;
                }
                else
                {
                    if (IF_DEBUG)
                    {
                        std::cout << "[Datanode" << port_ << "][GET] read from the disk and write to socket with port " << port_for_transfer_data_ << std::endl;
                    }
                    std::string value_buf(value_len, 0);
                    std::ifstream ifs(readpath);
                    ifs.read(value_buf.data(), value_len);
                    ifs.close();
                    if (IF_DEBUG)
                    {
                        std::cout << "[Datanode" << port_ << "][GET] read " << readpath << " with length of " << value_buf.length() << std::endl;
                    }
                    asio::write(socket_, asio::buffer(value_buf.data(), value_buf.length()));
                }

            }

            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << port_ << "][GET] write to socket!" << std::endl;
            }
        };
        auto handler2 = [this, key, key_len, value_len]() mutable
        {
            asio::error_code ec;
            asio::ip::tcp::socket socket_(io_context_);
            acceptor_.accept(socket_);

            std::vector<unsigned char> key_size_buf = int_to_bytes(key_len);
            asio::write(socket_, asio::buffer(key_size_buf, key_size_buf.size()));

            std::vector<unsigned char> value_size_buf = int_to_bytes(value_len);
            asio::write(socket_, asio::buffer(value_size_buf, value_size_buf.size()));

            asio::write(socket_, asio::buffer(key, key_len));

            if(IN_MEMORY)
            {
                // on redis
                if(!redis_->exists(key))
                {
                    std::cout << "[GET] key not found!" << std::endl;
                }
                else
                {
                    auto value_returned = redis_->get(key);
                    my_assert(value_returned.has_value());
                    std::string value_buf = value_returned.value();
                    asio::write(socket_, asio::buffer(value_buf.data(), value_buf.length()));
                }
            }
            else
            {
                // on disk
                std::string targetdir = "./storage/" + std::to_string(port_) + "/";
                std::string readpath = targetdir + key;
                if (access(readpath.c_str(), 0) == -1)
                {
                    std::cout << "[Datanode" << port_ << "][Read] file does not exist!" << readpath << std::endl;
                }
                else
                {
                    if (IF_DEBUG)
                    {
                        std::cout << "[Datanode" << port_ << "][GET] read from the disk and write to socket with port " << port_for_transfer_data_ << std::endl;
                    }
                    std::string value_buf(value_len, 0);
                    std::ifstream ifs(readpath);
                    ifs.read(value_buf.data(), value_len);
                    ifs.close();
                    if (IF_DEBUG)
                    {
                        std::cout << "[Datanode" << port_ << "][GET] read " << readpath << " with length of " << value_buf.length() << std::endl;
                    }
                    asio::write(socket_, asio::buffer(value_buf.data(), value_buf.length()));
                }

            }

            std::vector<char> finish(1);
            asio::read(socket_, asio::buffer(finish, finish.size()));

            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << port_ << "][GET] write to socket!" << std::endl;
            }
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << port_ << "][GET] ready to handle get!" << std::endl;
            }
            if(toproxy)
            {
                std::thread my_thread(handler1);
                my_thread.detach();
            }
            else
            {
                std::thread my_thread(handler2);
                my_thread.detach();
            }
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }
    }

    void Datanode::handle_delete(std::string block_key)
    {
        if(IN_MEMORY)
        {
            if(!redis_->exists(block_key))
            {
                std::cout << "[DEL] key not found!" << std::endl;
            }
            else
            {
                redis_->del(block_key);
            }
        }
        else
        {
            std::string file_path = "./storage/" + std::to_string(port_) + "/" + block_key;
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << port_ << "] File path:" << file_path << std::endl;
            }
            if (remove(file_path.c_str()))
            {
                std::cout << "[DEL] delete error!" << std::endl;
            }
        }
    }

    void Datanode::handle_transfer()
    {
        auto handler = [this]() mutable
        {
            asio::error_code ec;
            asio::ip::tcp::socket socket_(io_context_);
            acceptor_.accept(socket_);

            std::vector<unsigned char> value_size_buf(sizeof(int));
            asio::read(socket_, asio::buffer(value_size_buf, value_size_buf.size()), ec);
            int value_size = bytes_to_int(value_size_buf);

            std::string value_buf(value_size, 0);
            asio::read(socket_, asio::buffer(value_buf.data(), value_buf.size()), ec);

            // response
            std::vector<char> finish(1);
            asio::write(socket_, asio::buffer(finish, finish.size()));
            
            asio::error_code ignore_ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignore_ec);
            socket_.close(ignore_ec);

            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << port_ << "][Transfer] successfully transfer with " << value_size << "bytes." << std::endl;
            }
        };
        try
        {
            if (IF_DEBUG)
            {
                std::cout << "[Datanode" << port_ << "][Transfer] ready to handle cross-cluster transfer!" << std::endl;
            }
            std::thread my_thread(handler);
            my_thread.detach();
        }
        catch (std::exception &e)
        {
            std::cout << "exception" << std::endl;
            std::cout << e.what() << std::endl;
        }

    }
}
