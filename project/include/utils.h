#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include <memory>
#include <string>
#include <string.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <random>
#include <cmath>
#include <ctime>
#include <sys/time.h>
#include <source_location>

#define LOG_TO_FILE true
#define IF_SIMULATION true
#define IF_SIMULATE_CROSS_RACK true
#define IF_DEBUG false
#define IF_THREE_HIERARCHY false    // for hierarchy extension
#define IF_DIRECT_FROM_NODE true    // proxy can directly access data from nodes in other racks
#define IN_MEMORY true              // store data in memory or disk
#define IF_NEW_LOAD_METRIC false
#define COORDINATOR_PORT 11111
#define CLIENT_PORT 22222
#define my_assert(condition) exit_when((condition), std::source_location::current())

namespace ECProject
{
    enum ECFAMILY
    {
        LRCs,
        others
    };
    enum ECTYPE
	{
		Azu_LRC,
		Azu_LRC_1,
		Opt_LRC,
		Opt_Cau_LRC,
		Uni_Cau_LRC
	};
    enum PlacementType
    {
        flat,
        ran,
        opt,
        ecwide // only for Azu_LRC
    };
    enum NodeSelectionType
    {
        random,
        load_balance
    };
    enum FaultToleranceLevel    // region can be rack or cluster
    {
        single_region,
        two_regions,
        three_regions,
        four_regions,
        random_region
    };
    enum FailureType
    {
        single_block,
        multi_blocks,
        single_rack,
        single_cluster
    }; 

    typedef struct ECSchema
    {
        bool partial_decoding;
        ECTYPE ec_type;
        PlacementType placement_type;
        FaultToleranceLevel ft_level;
        NodeSelectionType ns_type;
        short k;    // num of data blocks
        short l;    // num of local parity blocks
        short g;    // num of global parity blocks
        size_t block_size;
    }ECSchema;


    typedef struct object_info
    {
        size_t value_len;
        std::vector<unsigned int> stripes;
    }object_info;

    typedef struct Stripe
    {
        unsigned int stripe_id;
        ECTYPE ec_type;
        short k;
        short l;
        short g;
        size_t block_size;
        // data blocks, local parity blocks, global parity blocks in order
        std::vector<unsigned int> blocks2nodes;
        std::vector<std::string> objects;   // in order with data blocks
    }Stripe;
    
    typedef struct Node
    {
        unsigned int node_id;
        std::string node_ip;
        int node_port;
        unsigned int map2rack;
        std::unordered_map<unsigned int, int> stripes_blocks;

        double storage_cost;
        double network_cost;
        double bandwidth;
        double storage;
    }Node;
    
    typedef struct Rack
    {
        int rack_id;
        std::string proxy_ip;
        int proxy_port;
        unsigned int map2cluster;
        std::vector<unsigned int> nodes;
    }Rack;

    typedef struct Cluster
    {
        int cluster_id;
        std::vector<unsigned int> racks;
    }Cluster;
    
    typedef struct placement_info
    {
        ECTYPE ec_type;
        std::vector<unsigned int> stripe_ids;
        std::vector<int> offsets; // for cross-object striping
        std::string key;
        size_t value_len;
        size_t block_size;
        size_t tail_block_size;
        short k;
        short l;
        short g;
        // data blocks, local parity blocks, global parity blocks in order
        std::vector<std::pair<std::string, int>> datanode_ip_port;
        std::string client_ip;
        int client_port;
    }placement_info;

    typedef struct delete_plan
    {
        std::vector<std::string> block_ids;
        std::vector<std::pair<std::string, int>> datanode_ip_port;
    }delete_plan;

    typedef struct migration_plan
    {
        std::vector<size_t> block_sizes;
        std::vector<std::string> block_ids;
        std::vector<std::pair<std::string, int>> src_nodes;
        std::vector<std::pair<std::string, int>> dst_nodes;
    }migration_plan;
    
    typedef struct main_repair_plan
    {
        unsigned int rack_id;
        unsigned int stripe_id;
        short k;
        short l;
        short g;
        size_t block_size;
        ECTYPE ec_type;
        bool partial_decoding;
        bool is_local_repair;
        std::vector<int> live_blocks_index;
        std::vector<int> failed_blocks_index;
        std::vector<std::vector<std::pair<int, std::pair<std::string, int>>>> help_racks_blocks_info;
        std::vector<std::pair<int, std::pair<std::string, int>>> inner_rack_help_blocks_info;
        std::vector<std::pair<int, std::pair<std::string, int>>> new_locations;
    }main_repair_plan;

    typedef struct help_repair_plan
    {
        unsigned int rack_id;
        unsigned int stripe_id;
        short k;
        short l;
        short g;
        size_t block_size;
        ECTYPE ec_type;
        bool partial_decoding;
        bool is_local_repair;
        std::vector<int> live_blocks_index;
        std::vector<int> failed_blocks_index;   // the order of blocks index must be consistent with that in main_repair_plan
        std::vector<std::pair<int, std::pair<std::string, int>>> inner_rack_help_blocks_info;
        std::string main_proxy_ip;
        int main_proxy_port;
    }help_repair_plan;

    // response
    typedef struct set_resp
    {
        std::string proxy_ip;
        int proxy_port;
    }set_resp;

    typedef struct repair_resp
    {
        double decoding_time;
        double repair_time;
        int num_of_blocks_cross_rack;
    }repair_resp;

    typedef struct bias_info
    {
        double rack_storage_bias;
        double rack_network_bias;
        double node_storage_bias;
        double node_network_bias;
    }bias_info;
    

    ECFAMILY check_ec_family(ECTYPE ec_type);

    // generate a random index in [0,len)
    int random_index(size_t len);
    // generate a random integer in [min,max]
    int random_range(int min, int max);
    // generate n random number in [min,max]
    void random_n_num(int min, int max, int n, std::vector<int> &random_numbers);

    std::string generate_random_string(int length);

    void generate_unique_random_strings(int key_length, int value_length, int n, std::unordered_map<std::string, std::string> &key_value);

    void generate_unique_random_keys(int key_length, int n, std::unordered_set<std::string> &keys);

    void exit_when(bool condition, const std::source_location &location);

    int bytes_to_int(std::vector<unsigned char> &bytes);

    std::vector<unsigned char> int_to_bytes(int integer);

    double bytes_to_double(std::vector<unsigned char> &bytes);

    std::vector<unsigned char> double_to_bytes(double doubler);
}
#endif