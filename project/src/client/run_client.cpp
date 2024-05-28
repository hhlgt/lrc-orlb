#include "client.h"

int main(int argc, char **argv)
{
    if(argc != 11)
    {
        std::cout << "./run_client partial_decoding encode_type placement_type fault_tolerance_level node_selection_type k l g block_size(KB) stripe_num" << std::endl;
        std::cout << "./run_client true Azu_LRC random single_rack random 6 2 2 1024 4" << std::endl;
        exit(-1);
    }

    ECProject::ECSchema ec_schema;

    ec_schema.partial_decoding = (std::string(argv[1]) == "true");

    if(std::string(argv[2]) == "Azu_LRC")   // now only support Azure-LRC
    {
        ec_schema.ec_type = ECProject::Azu_LRC;
    }
    else
    {
        std::cout << "error: unknown encode_type" << std::endl;
        exit(-1);
    }


    if(std::string(argv[3]) == "flat")
    {
        ec_schema.placement_type = ECProject::PlacementType::flat;
    }
    else if(std::string(argv[3]) == "ran")
    {
        ec_schema.placement_type = ECProject::PlacementType::ran;
    }
    else if(std::string(argv[3]) == "opt")
    {
        ec_schema.placement_type = ECProject::PlacementType::opt;
    }
    else if(std::string(argv[3]) == "ecwide")
    {
        ec_schema.placement_type = ECProject::PlacementType::ecwide;
    }
    else
    {
        std::cout << "error: unknown placement_type" << std::endl;
        exit(-1);
    }

    if(std::string(argv[4]) == "single_rack")
    {
        ec_schema.ft_level = ECProject::FaultToleranceLevel::single_region;
    }
    else if(std::string(argv[4]) == "two_racks")
    {
        ec_schema.ft_level = ECProject::FaultToleranceLevel::two_regions;
    }
    else if(std::string(argv[4]) == "three_racks")
    {
        ec_schema.ft_level = ECProject::FaultToleranceLevel::three_regions;
    }
    else if(std::string(argv[4]) == "four_racks")
    {
        ec_schema.ft_level = ECProject::FaultToleranceLevel::four_regions;
    }
    else if(std::string(argv[4]) == "random")
    {
        ec_schema.ft_level = ECProject::FaultToleranceLevel::random_region;
    }
    else
    {
        std::cout << "error: unknown fault_tolerance_level" << std::endl;
        exit(-1);
    }

    if(std::string(argv[5]) == "random")
    {
        ec_schema.ns_type = ECProject::NodeSelectionType::random;
    }
    else if(std::string(argv[5]) == "load_balance")
    {
        ec_schema.ns_type = ECProject::NodeSelectionType::load_balance;
    }
    else
    {
        std::cout << "error: unknown node_selection_type" << std::endl;
        exit(-1);
    }

    ec_schema.k = std::stoi(std::string(argv[6]));
    ec_schema.l = std::stoi(std::string(argv[7]));
    ec_schema.g = std::stoi(std::string(argv[8]));
    ec_schema.block_size = std::stoi(std::string(argv[9])) * 1024;
    
    int stripe_num = std::stoi(std::string(argv[10]));
    size_t value_length = ec_schema.k * ec_schema.block_size;

    ECProject::Client client("0.0.0.0", CLIENT_PORT, "0.0.0.0", COORDINATOR_PORT);

    // set erasure coding parameters
    client.set_ec_parameters(ec_schema);

    struct timeval start_time, end_time;

    // generate key-value pair
    std::unordered_map<std::string, std::string> key_value;
    ECProject::generate_unique_random_strings(5, value_length, stripe_num, key_value);

    //set
    double set_time = 0;
    gettimeofday(&start_time, NULL);
    for(auto &kv : key_value)
    {
        double t_encoding_time = client.set(kv.first, kv.second);
        // get bias
        ECProject::bias_info biases = client.get_bias();
        std::cout << "[SET] encoding_time=" << t_encoding_time << "s, rack_storage_bias=" <<
                    biases.rack_storage_bias << ", rack_network_bias=" << biases.rack_network_bias <<
                    ", node_storage_bias=" << biases.node_storage_bias << ", node_network_bias=" <<
                    biases.node_network_bias << std::endl;
    }
    gettimeofday(&end_time, NULL);
    set_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    std::cout << "Total set time: " << set_time << ", average set time for each:" << set_time / stripe_num << std::endl;
/*
    double get_time = 0;
    gettimeofday(&start_time, NULL);
    for (auto &kv : key_value) {
        auto stored_value = client.get(kv.first);
        ECProject::my_assert(stored_value == kv.second);
    }
    gettimeofday(&end_time, NULL);
    get_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    std::cout << "Total get time: " << get_time << ", average get time for each:" << get_time / stripe_num << std::endl;
*/
    // degraded read repair
    int drc = 0;
    for(int i = 0; i < stripe_num; i++)
    {
        for(unsigned int j = 0; j < ec_schema.k; j++)
        {
            auto response = client.blocks_repair({j}, i);
            drc += response.num_of_blocks_cross_rack;
        }
    }
    std::cout << drc << ", DRC = " << (double)drc / (double)(stripe_num * ec_schema.k) << std::endl;

    // node repair
    int nrc = 0;
    for(int i = 0; i < stripe_num; i++)
    {
        for(unsigned int j = 0; j < ec_schema.k + ec_schema.l + ec_schema.g; j++)
        {
            auto response = client.blocks_repair({j}, i);
            // std::cout << response.decoding_time << " " << response.repair_time << " " << response.num_of_blocks_cross_rack << std::endl;
            nrc += response.num_of_blocks_cross_rack;
        }
    }
    std::cout << nrc << ", NRC = " << (double)nrc / (double)(stripe_num * ec_schema.k) << std::endl;

    // unsigned int num_of_nodes = 200;
    // for(unsigned int i = 0; i < num_of_nodes; i++)
    // {
    //     auto response = client.nodes_repair({i});
    // }
    
    // random multiple-block failures
    /*int run_time = 5;
    for(int i = 0; i < stripe_num; i++)
    {
        for(int j = 0; j < run_time; j++)
        {
            int failed_num = ECProject::random_range(2, 3);
            std::vector<int> random_numbers;
            ECProject::random_n_num(0, ec_schema.k + ec_schema.l + ec_schema.g - 1, failed_num, random_numbers);
            std::vector<unsigned int> failed_block_list;
            for(auto it = random_numbers.begin(); it != random_numbers.end(); it++)
            {
                failed_block_list.push_back(*it);
            }
            auto response = client.blocks_repair(failed_block_list, i);
            std::cout << response.decoding_time << " " << response.repair_time << " " << response.num_of_blocks_cross_rack << std::endl;
        }
    }*/

    

    // double get_time = 0;
    // gettimeofday(&start_time, NULL);
    // for (auto &kv : key_value) {
    //     auto stored_value = client.get(kv.first);
    //     // ECProject::my_assert(stored_value == kv.second);
    // }
    // gettimeofday(&end_time, NULL);
    // get_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    // std::cout << "Total get time: " << get_time << ", average get time for each:" << get_time / stripe_num << std::endl;

    // double migration_time = client.check_load_balance_and_migration(0.5, 0.5, 0.5);
    // ECProject::bias_info biases = client.get_bias();
    // std::cout << "[MIGRATION] time=" << migration_time << "s, rack_storage_bias=" 
    //           << biases.rack_storage_bias << ", rack_network_bias=" << biases.rack_network_bias 
    //           << ", node_storage_bias=" << biases.node_storage_bias << ", node_network_bias=" 
    //           << biases.node_network_bias << std::endl;

    client.delete_all_stripes();

    return 0;
}