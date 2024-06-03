#include "client.h"
#include <fstream>

int main(int argc, char **argv)
{
    if(!LOG_TO_FILE && argc != 11)
    {
        std::cout << "./run_client partial_decoding encode_type placement_type fault_tolerance_level node_selection_type k l g block_size(KB) stripe_num" << std::endl;
        std::cout << "./run_client true Azu_LRC random single_rack random 6 2 2 1024 4" << std::endl;
        exit(-1);
    }
    else if(LOG_TO_FILE && argc != 12)
    {
        std::cout << "./run_client partial_decoding encode_type placement_type fault_tolerance_level node_selection_type k l g block_size(KB) stripe_num log_file_name" << std::endl;
        std::cout << "./run_client true Azu_LRC random single_rack random 6 2 2 1024 4 test.txt" << std::endl;
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

    

    if(LOG_TO_FILE)
    {
        umask(0);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
    }

    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);
    std::string log_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../../res/" + std::string(argv[11]);
    std::ofstream outfile(log_path, std::ios::app);

    if(LOG_TO_FILE)
    {
        outfile << "EC information:\n"
                << stripe_num << " Azu-LRC(" << ec_schema.k << "," << ec_schema.l << "," << ec_schema.g << "), BS =" << ec_schema.block_size / 1024 << "KB\n"
                << "Pt:" << std::string(argv[3]) << ", Ft:" << std::string(argv[4]) << "NS:" << std::string(argv[5]) << "\n";
    }
    else
    {
        std::cout << "EC information:\n"
                  << stripe_num << " Azu-LRC(" << ec_schema.k << "," << ec_schema.l << "," << ec_schema.g << "), BS =" << ec_schema.block_size / 1024 << "KB\n"
                  << "Pt:" << std::string(argv[3]) << ", Ft:" << std::string(argv[4]) << ", NS:" << std::string(argv[5]) << "\n";
    }

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
        if(LOG_TO_FILE)
        {
            outfile << "[SET] encoding_time=" << t_encoding_time << "s, rack_storage_bias=" <<
                    biases.rack_storage_bias << ", rack_network_bias=" << biases.rack_network_bias <<
                    ", node_storage_bias=" << biases.node_storage_bias << ", node_network_bias=" <<
                    biases.node_network_bias << "\n";
        }
        else
        {
            std::cout << "[SET] encoding_time=" << t_encoding_time << "s, rack_storage_bias=" <<
                      biases.rack_storage_bias << ", rack_network_bias=" << biases.rack_network_bias <<
                      ", node_storage_bias=" << biases.node_storage_bias << ", node_network_bias=" <<
                      biases.node_network_bias << std::endl;
        }

        
    }
    gettimeofday(&end_time, NULL);
    set_time += end_time.tv_sec - start_time.tv_sec + (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
    if(LOG_TO_FILE)
    {
        outfile << "Total set time: " << set_time << ", average set time for each:" << set_time / stripe_num << "\n";
    }
    else
    {
        std::cout << "Total set time: " << set_time << ", average set time for each:" << set_time / stripe_num << std::endl;
    }
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
    double drc_rt = 0.0, max_drc_rt = 0.0, min_drc_rt = 1000.0;
    double drc_dt = 0.0, max_drc_dt = 0.0, min_drc_dt = 1000.0;
    int drc = 0, max_drc = 0, min_drc = 1000;
    for(int i = 0; i < stripe_num; i++)
    {
        int tmp_drc = 0;
        double tmp_drc_rt = 0, tmp_drc_dt = 0;
        for(unsigned int j = 0; j < ec_schema.k; j++)
        {
            auto response = client.blocks_repair({j}, i);
            tmp_drc += response.num_of_blocks_cross_rack;
            tmp_drc_rt += response.repair_time;
            tmp_drc_dt += response.decoding_time;
        }
        drc += tmp_drc;
        drc_rt += tmp_drc_rt;
        drc_dt += tmp_drc_dt;
        tmp_drc /= ec_schema.k;
        tmp_drc_rt /= ec_schema.k;
        tmp_drc_dt /= ec_schema.k;
        if(max_drc < tmp_drc)
            max_drc = tmp_drc;
        if(min_drc > tmp_drc)
            min_drc = tmp_drc;
        if(max_drc_rt < tmp_drc_rt)
            max_drc_rt = tmp_drc_rt;
        if(min_drc_rt > tmp_drc_rt)
            min_drc_rt = tmp_drc_rt;
        if(max_drc_dt < tmp_drc_dt)
            max_drc_dt = tmp_drc_dt;
        if(min_drc_dt > tmp_drc_dt)
                min_drc_dt = tmp_drc_dt;

    }
    if(LOG_TO_FILE)
    {
        outfile << "Degraded Read Time:\n";
        outfile << "1. Simulation: DRC = " << (double)drc / (double)(stripe_num * ec_schema.k) 
                << ", MAX_DRC = " << max_drc << ", MIN_DRC = " << min_drc << "\n";
        outfile << "2. Repair_time = " << (double)drc_rt / (double)(stripe_num * ec_schema.k) 
                << ", MAX_Rt = " << max_drc_rt << ", MIN_Rt = " << min_drc_rt << "\n";
        outfile << "3. Decoding_time = " << (double)drc_dt / (double)(stripe_num * ec_schema.k) 
                << ", MAX_Dt = " << max_drc_dt << ", MIN_Dt = " << min_drc_dt << "\n";
    }
    else
    {
        std::cout << "Node Repair Time/Rate:" << std::endl;
        std::cout << "1. Simulation: DRC = " << (double)drc / (double)(stripe_num * ec_schema.k) 
                << ", MAX_DRC = " << max_drc << ", MIN_DRC = " << min_drc << std::endl;
        std::cout << "2. Repair_time = " << (double)drc_rt / (double)(stripe_num * ec_schema.k) 
                << ", MAX_Rt = " << max_drc_rt << ", MIN_Rt = " << min_drc_rt << std::endl;
        std::cout << "3. Decoding_time = " << (double)drc_dt / (double)(stripe_num * ec_schema.k) 
                << ", MAX_Dt = " << max_drc_dt << ", MIN_Dt = " << min_drc_dt << std::endl;
    }

    // node repair
    double nrc_rt = 0.0, max_nrc_rt = 0.0, min_nrc_rt = 1000.0;
    double nrc_dt = 0.0, max_nrc_dt = 0.0, min_nrc_dt = 1000.0;
    int nrc = 0, max_nrc = 0, min_nrc = 1000;
    for(int i = 0; i < stripe_num; i++)
    {
        int tmp_nrc = 0;
        double tmp_nrc_rt = 0, tmp_nrc_dt = 0;
        for(unsigned int j = 0; j < ec_schema.k + ec_schema.l + ec_schema.g; j++)
        {
            auto response = client.blocks_repair({j}, i);
            tmp_nrc += response.num_of_blocks_cross_rack;
            tmp_nrc_rt += response.repair_time;
            tmp_nrc_dt += response.decoding_time;
        }
        nrc += tmp_nrc;
        nrc_rt += tmp_nrc_rt;
        nrc_dt += tmp_nrc_dt;
        tmp_nrc /= ec_schema.k;
        tmp_nrc_rt /= ec_schema.k;
        tmp_nrc_dt /= ec_schema.k;
        if(max_nrc < tmp_nrc)
            max_nrc = tmp_nrc;
        if(min_nrc > tmp_nrc)
            min_nrc = tmp_nrc;
        if(max_nrc_rt < tmp_nrc_rt)
            max_nrc_rt = tmp_nrc_rt;
        if(min_nrc_rt > tmp_nrc_rt)
            min_nrc_rt = tmp_nrc_rt;
        if(max_nrc_dt < tmp_nrc_dt)
            max_nrc_dt = tmp_nrc_dt;
        if(min_nrc_dt > tmp_nrc_dt)
                min_nrc_dt = tmp_nrc_dt;
    }
    double object_size = (double)(ec_schema.block_size * ec_schema.k) / (1024 * 1024);
    if(LOG_TO_FILE)
    {
        outfile << "Degraded Read Time:\n";
        outfile << "1. Simulation: NRC = " << (double)nrc / (double)(stripe_num * ec_schema.k) 
                << ", MAX_NRC = " << max_nrc << ", MIN_NRC = " << min_nrc << "\n";
        outfile << "2. Repair_time = " << (double)nrc_rt / (double)(stripe_num * ec_schema.k) 
                << ", MAX_Rt = " << max_nrc_rt << ", MIN_Rt = " << min_nrc_rt << "\n";
        outfile << "3. Decoding_time = " << (double)nrc_dt / (double)(stripe_num * ec_schema.k) 
                << ", MAX_Dt = " << max_nrc_dt << ", MIN_Dt = " << min_nrc_dt << "\n";
        outfile << "4. Repair_rate = " <<  object_size / ((double)nrc_rt / (double)(stripe_num * ec_schema.k))
                << ", MAX_Rt = " << object_size / max_nrc_rt 
                << ", MIN_Rt = " << object_size / min_nrc_rt << "\n";
    }
    else
    {
        std::cout << "Degraded Read Time:" << std::endl;
        std::cout << "1. Simulation: NRC = " << (double)nrc / (double)(stripe_num * ec_schema.k) 
                << ", MAX_NRC = " << max_nrc << ", MIN_NRC = " << min_nrc << std::endl;
        std::cout << "2. Repair_time = " << (double)nrc_rt / (double)(stripe_num * ec_schema.k) 
                << ", MAX_Rt = " << max_nrc_rt << ", MIN_Rt = " << min_nrc_rt << std::endl;
        std::cout << "3. Decoding_time = " << (double)nrc_dt / (double)(stripe_num * ec_schema.k) 
                << ", MAX_Dt = " << max_nrc_dt << ", MIN_Dt = " << min_nrc_dt << std::endl;
        std::cout << "4. Repair_rate = " <<  object_size / ((double)nrc_rt / (double)(stripe_num * ec_schema.k))
                << ", MAX_Rt = " << object_size / max_nrc_rt 
                << ", MIN_Rt = " << object_size / min_nrc_rt << std::endl;
    }

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

    if(LOG_TO_FILE)
    {
        outfile << "finish!\n";
    }

    outfile.close();

    return 0;
}