#include "client.h"
#include "zipf.h"
#include <chrono>
#include <fstream>

bool cmp(std::pair<int, int> &a, std::pair<int, int> &b) {
  return a.second > b.second;
}

int main(int argc, char **argv)
{
    if(!LOG_TO_FILE && argc != 3)
    {
        std::cout << "./sim_load_balance node_selection u\n";
        exit(-1);
    }
    else if(LOG_TO_FILE && argc != 4)
    {
        std::cout << "./sim_load_balance node_selection u logfile\n";
        exit(-1);
    }

    ECProject::ECSchema ec_schema;
    if(std::string(argv[1]) == "random")
    {
        ec_schema.ns_type = ECProject::NodeSelectionType::random;
    }
    else if(std::string(argv[1]) == "load_balance")
    {
        ec_schema.ns_type = ECProject::NodeSelectionType::load_balance;
    }
    else
    {
        std::cout << "error: unknown node_selection_type" << std::endl;
        exit(-1);
    }

    ec_schema.partial_decoding = true;
    ec_schema.ec_type = ECProject::Azu_LRC;
    ec_schema.placement_type = ECProject::PlacementType::opt;
    ec_schema.ft_level = ECProject::FaultToleranceLevel::single_region;
    ec_schema.k = 8;
    ec_schema.l = 4;
    ec_schema.g = 3;
    ec_schema.block_size = 1024;
    size_t value_len = ec_schema.k * ec_schema.block_size;
    double u = (double)std::stof(std::string(argv[2]));

    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);
    std::string log_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../res/test.txt";
    if(LOG_TO_FILE)
    {
        log_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../res/" + std::string(argv[3]);
    }
    std::ofstream outfile(log_path, std::ios::app);

    if(LOG_TO_FILE)
    {
        umask(0);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
    }

    if(LOG_TO_FILE)
    {
        outfile << "EC information:\n"
                << " Azu-LRC(" << ec_schema.k << "," << ec_schema.l << "," << ec_schema.g << "), BS=" << ec_schema.block_size << ", "
                << "Pt:opt, Ft:single-cluster, NS:" << std::string(argv[1]) << ", u=" << u << "\n";
    }
    else
    {
        std::cout << "EC information:\n"
                  << " Azu-LRC(" << ec_schema.k << "," << ec_schema.l << "," << ec_schema.g << "), BS=" << ec_schema.block_size << ", "
                  << "Pt:opt, Ft:single-cluster, NS:" << std::string(argv[1]) << ", u=" << u << "\n";
    }

    ECProject::Client client("0.0.0.0", CLIENT_PORT, "0.0.0.0", COORDINATOR_PORT);
    // set erasure coding parameters
    client.set_ec_parameters(ec_schema);

    int read_cnt = 95 * 1000 * 5;
    int write_cnt = 5 * 1000 * 5;
    std::unordered_map<int, std::string> all_keys_with_idx;
    std::unordered_set<std::string> all_keys;
    std::unordered_map<int, int> records;
    std::random_device rd;
    std::mt19937 gen(rd());
    
    int once_write = write_cnt / 1000;
    int iter_cnt = write_cnt / once_write;
    int cnt1 = 0;
    int cnt2 = 0;

    std::string value;
    for(int i = 0; i < value_len; i++)
    {
        value += "0";
    }
    ECProject::generate_unique_random_keys(5, write_cnt, all_keys);
    auto iter = all_keys.begin();

    int index = 0;
    for (int i = 0; i < iter_cnt; i++) {
        for (int j = 0; j < once_write; j++) {
            std::string key = *iter;
            iter++;
            all_keys_with_idx[index++] = key;
            client.set(key, value);
        }
        std::default_random_engine generator;
        zipfian_int_distribution<int> distribution(0, all_keys_with_idx.size() - 1, u);
        for (int j = 0; j < std::ceil(((double)read_cnt / (double)iter_cnt) * 0.8); j++) {
            cnt1++;
            int idx = distribution(generator);
            idx = all_keys_with_idx.size() - 1 - idx;
            records[idx]++;
            auto temp = client.get(all_keys_with_idx[idx]);
        }
        for (int j = 0; j < std::ceil(((double)read_cnt / (double)iter_cnt) * 0.2); j++) {
            cnt2++;
            int idx = distribution(generator);
            std::string temp;
            idx = all_keys_with_idx.size() - 1 - idx;
            records[idx]++;
            // simulate degraded read
            unsigned int ran_block_idx = (unsigned int)ECProject::random_index(ec_schema.k);
            auto response = client.blocks_repair({ran_block_idx}, idx);
        }
        if(((i + 1) * 10) % iter_cnt == 0)
        {
            // get bias
            ECProject::bias_info biases = client.get_bias();
            if(LOG_TO_FILE)
            {
                outfile << "iter " << i << ":\n";
                outfile << "rack_storage_bias=" << biases.rack_storage_bias << ", rack_network_bias=" 
                        << biases.rack_network_bias << ", node_storage_bias=" << biases.node_storage_bias 
                        << ", node_network_bias=" << biases.node_network_bias << "\n";
            }
            else
            {
                std::cout << "iter " << i << ":\n";
                std::cout << "rack_storage_bias=" << biases.rack_storage_bias << ", rack_network_bias=" 
                          << biases.rack_network_bias << ", node_storage_bias=" << biases.node_storage_bias 
                          << ", node_network_bias=" << biases.node_network_bias << "\n";
            }

            double rack_storage_threshold = 0.9 * biases.rack_storage_bias;
            double rack_network_threshold = 0.9 * biases.rack_network_bias;
            double node_storage_threshold = 0.9 * biases.node_storage_bias;
            double node_network_threshold = 0.9 * biases.node_network_bias;

            auto migration = client.check_load_balance_and_migration(-1, rack_storage_threshold, rack_network_threshold, 
                                                                     node_storage_threshold, node_network_threshold);

            biases = client.get_bias();
            if(LOG_TO_FILE)
            {
                outfile << "iter " << i << " after migration: " << migration << "s\n";
                outfile << "rack_storage_bias=" << biases.rack_storage_bias << ", rack_network_bias=" 
                        << biases.rack_network_bias << ", node_storage_bias=" << biases.node_storage_bias 
                        << ", node_network_bias=" << biases.node_network_bias << "\n";
            }
            else
            {
                std::cout << "iter " << i << " after migration: " << migration << "s\n";
                std::cout << "rack_storage_bias=" << biases.rack_storage_bias << ", rack_network_bias=" 
                          << biases.rack_network_bias << ", node_storage_bias=" << biases.node_storage_bias 
                          << ", node_network_bias=" << biases.node_network_bias << "\n";
            }
        }
    }
    unsigned int ran_failed_node = (unsigned int)ECProject::random_index(200);
    auto response = client.nodes_repair({ran_failed_node});

    // get bias
    ECProject::bias_info biases = client.get_bias();
    if(LOG_TO_FILE)
    {
        outfile << "After repair random node:\n";
        outfile << "rack_storage_bias=" << biases.rack_storage_bias << ", rack_network_bias=" 
                << biases.rack_network_bias << ", node_storage_bias=" << biases.node_storage_bias 
                << ", node_network_bias=" << biases.node_network_bias << "\n";
    }
    else
    {
        std::cout << "After repair random node:\n";
        std::cout << "rack_storage_bias=" << biases.rack_storage_bias << ", rack_network_bias=" 
                  << biases.rack_network_bias << ", node_storage_bias=" << biases.node_storage_bias 
                  << ", node_network_bias=" << biases.node_network_bias << "\n";
    }

    std::vector<std::pair<int, int>> help;
    for (auto p : records) {
        help.push_back({p.first, p.second});
    }
    std::sort(help.begin(), help.end(), cmp);
    int key_cnt = 0, key_read_cnt = 0, bound_frequency = 0;
    for(auto p : help)
    {
        key_cnt++;
        key_read_cnt += p.second;
        if(key_read_cnt > int((double)read_cnt * u))
        {
            bound_frequency = p.second;
            break;
        } 
    }
    if(LOG_TO_FILE)
    {
        outfile << "once_write=" << once_write
                << ", accessed objects:" << help.size() << "/" << all_keys.size() << ", " << u * 100 << "\% of reads on " 
                << key_cnt << " objects, max_read = " << help[0].second << ", bound_frequency = " << bound_frequency << "\n";
        outfile << "read_cnt:" << cnt1 << ", repair_cnt:" << cnt2 << "\n\n";
    }
    else
    {
        std::cout << "once_write=" << once_write
                  << ", accessed objects:" << help.size() << "/" << all_keys.size() << ", " << u * 100 << "\% of reads on " 
                  << key_cnt << " objects, max_read = " << help[0].second << ", bound_frequency = " << bound_frequency << "\n";
        std::cout << "read_cnt:" << cnt1 << ", repair_cnt:" << cnt2 << "\n\n";
    }

    client.delete_all_stripes();

    outfile.close();

    return 0;
}