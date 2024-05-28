#include "client.h"

int main(int argc, char **argv)
{
    if(argc != 2)
    {
        std::cout << "./simulation test_sets_num \n";
        exit(-1);
    }
    ECProject::ECSchema ec_schema;

    ec_schema.partial_decoding = true;
    ec_schema.ec_type = ECProject::Azu_LRC;
    ec_schema.ns_type = ECProject::NodeSelectionType::random;
    ec_schema.block_size = 1;

    ECProject::Client client("0.0.0.0", CLIENT_PORT, "0.0.0.0", COORDINATOR_PORT);

    int max_k_drc = 0, max_l_drc = 0, max_g_drc = 0, max_t_drc = 0;
    int max_k_nrc = 0, max_l_nrc = 0, max_g_nrc = 0, max_t_nrc = 0;
    double max_rate_nrc = 0;
    double max_rate_drc = 0;
    double avg_rate_nrc = 0;
    double avg_rate_drc = 0;
    int sets_num = std::stoi(std::string(argv[1]));
    int T = sets_num;
    while(T--)
    {
        ec_schema.k = ECProject::random_range(4, 128);
        ec_schema.l = ECProject::random_range(2, ec_schema.k / 2);
        ec_schema.g = ECProject::random_range(3, std::min((int)ec_schema.k, 32));

        int t = 2;
        if(ec_schema.g >= 5)
        {
            t = ECProject::random_range(2, 3);
        }
        if(t == 2)
        {
            ec_schema.ft_level = ECProject::FaultToleranceLevel::two_regions;
        }
        else
        {
            ec_schema.ft_level = ECProject::FaultToleranceLevel::three_regions;
        }
        size_t value_length = ec_schema.k * ec_schema.block_size;

        // ran
        ec_schema.placement_type = ECProject::PlacementType::ran;
        int stripe_num = 5;

        client.set_ec_parameters(ec_schema);
        std::unordered_map<std::string, std::string> key_value;
        ECProject::generate_unique_random_strings(5, value_length, stripe_num, key_value);

        //set
        for(auto &kv : key_value)
        {
            double t_encoding_time = client.set(kv.first, kv.second);
        }
        // degraded read repair
        double ran_drc = 0;
        for(int i = 0; i < stripe_num; i++)
        {
            for(unsigned int j = 0; j < ec_schema.k; j++)
            {
                auto response = client.blocks_repair({j}, i);
                ran_drc += (double)response.num_of_blocks_cross_rack;
            }
        }
        ran_drc = ran_drc / (double)(stripe_num * ec_schema.k);
        // node repair
        double ran_nrc = 0;
        for(int i = 0; i < stripe_num; i++)
        {
            for(unsigned int j = 0; j < ec_schema.k + ec_schema.l + ec_schema.g; j++)
            {
                auto response = client.blocks_repair({j}, i);
                // std::cout << response.decoding_time << " " << response.repair_time << " " << response.num_of_blocks_cross_rack << std::endl;
                ran_nrc += (double)response.num_of_blocks_cross_rack;
            }
        }
        ran_nrc = ran_nrc / (double)(stripe_num * ec_schema.k);
        
        client.delete_all_stripes();

        // opt
        ec_schema.placement_type = ECProject::PlacementType::opt;
        stripe_num = 1;
        std::unordered_map<std::string, std::string> key_value1;
        ECProject::generate_unique_random_strings(5, value_length, stripe_num, key_value1);
        client.set_ec_parameters(ec_schema);

        //set
        for(auto &kv : key_value1)
        {
            double t_encoding_time = client.set(kv.first, kv.second);
        }
        // degraded read repair
        double opt_drc = 0;
        for(int i = 0; i < stripe_num; i++)
        {
            for(unsigned int j = 0; j < ec_schema.k; j++)
            {
                auto response = client.blocks_repair({j}, i);
                opt_drc += (double)response.num_of_blocks_cross_rack;
            }
        }
        opt_drc = opt_drc / (double)(stripe_num * ec_schema.k);
        // node repair
        double opt_nrc = 0;
        for(int i = 0; i < stripe_num; i++)
        {
            for(unsigned int j = 0; j < ec_schema.k + ec_schema.l + ec_schema.g; j++)
            {
                auto response = client.blocks_repair({j}, i);
                opt_nrc += (double)response.num_of_blocks_cross_rack;
            }
        }
        opt_nrc = opt_nrc / (double)(stripe_num * ec_schema.k);
        
        client.delete_all_stripes();

        // cal
        double nrc_rate = (ran_nrc - opt_nrc) / ran_nrc;
        double drc_rate = (ran_drc - opt_drc) / ran_drc;
        avg_rate_nrc += nrc_rate;
        avg_rate_drc += drc_rate;
        if(max_rate_drc < drc_rate)
        {
            max_rate_drc = drc_rate;
            max_k_drc = ec_schema.k;
            max_l_drc = ec_schema.l;
            max_g_drc = ec_schema.g;
            max_t_drc = t;
        }
        if(max_rate_nrc < nrc_rate)
        {
            max_rate_nrc = nrc_rate;
            max_k_nrc = ec_schema.k;
            max_l_nrc = ec_schema.l;
            max_g_nrc = ec_schema.g;
            max_t_nrc = t;
        } 
    }
    std::cout << "Average : NRC = " << avg_rate_nrc / sets_num << ", DRC = " << avg_rate_drc / sets_num << ".\n"
              << "Max: NRC = " << max_rate_nrc << ", DRC = " << max_rate_drc << ".\n"
              << "MAX_NRC : " << max_k_nrc << "," << max_l_nrc << "," << max_g_nrc << "," << max_t_nrc << " "
              << "MAX_DRC : " << max_k_drc << "," << max_l_drc << "," << max_g_drc << "," << max_t_drc << "\n";
    return 0;
}