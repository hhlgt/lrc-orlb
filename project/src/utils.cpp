#include "utils.h"

const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

ECProject::ECFAMILY ECProject::check_ec_family(ECTYPE ec_type)
{
    if(ec_type == Azu_LRC || ec_type == Azu_LRC_1 || ec_type == Opt_LRC
       || ec_type == Opt_Cau_LRC || ec_type == Uni_Cau_LRC)
    {
        return ECFAMILY::LRCs;
    }
    return ECFAMILY::others;
}

int ECProject::random_index(size_t len) 
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, len - 1);
    return dist(gen);
}

int ECProject::random_range(int min, int max)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(min, max);
    return dist(gen);
}

void ECProject::random_n_num(int min, int max, int n, std::vector<int> &random_numbers)
{
    my_assert(n <= max - min + 1);
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(min, max);

    int cnt = 0;
    int num = dis(gen);
    random_numbers.push_back(num);
    cnt++;
    while(cnt < n)
    {
        while(std::find(random_numbers.begin(), random_numbers.end(), num) != random_numbers.end())
        {
        num = dis(gen);
        }
        random_numbers.push_back(num);
        cnt++;
    }
}

// generate random strings
std::string ECProject::generate_random_string(int length) 
{
    std::string result;

    std::mt19937 rng(std::time(0)); // take current time as random seed
    std::uniform_int_distribution<int> distribution(0, charset.size() - 1);

    for (int i = 0; i < length; ++i) {
        int random_index = distribution(rng);
        result += charset[random_index];
    }

    return result;
}

// generate n key-value pairs with distinct keys
void ECProject::generate_unique_random_strings(
    int key_length, int value_length, int n,
    std::unordered_map<std::string, std::string> &key_value) 
{

    std::mt19937 rng(std::time(0)); // take current time as random seed
    std::uniform_int_distribution<int> distribution(0, charset.size() - 1);

    for (int i = 0; i < n; i++) {
        std::string key;

        do {
            for (int i = 0; i < key_length; ++i) {
                int random_index = distribution(rng);
                key += charset[random_index];
            }
        } while (key_value.find(key) != key_value.end());

        std::string value(value_length, key[0]);

        key_value[key] = value;
    }
}

void ECProject::generate_unique_random_keys(int key_length, int n, std::unordered_set<std::string> &keys)
{
    std::mt19937 rng(std::time(0)); // take current time as random seed
    std::uniform_int_distribution<int> distribution(0, charset.size() - 1);

    for (int i = 0; i < n; i++) {
        std::string key;
        do {
            for (int i = 0; i < key_length; ++i) {
                int random_index = distribution(rng);
                key += charset[random_index];
            }
        } while (keys.find(key) != keys.end());

        keys.insert(key);
    }
}

void ECProject::exit_when(bool condition, const std::source_location &location) {
    if (!condition) {
        std::cerr << "Condition failed at " << location.file_name() << ":" << location.line()
                << " - " << location.function_name() << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

int ECProject::bytes_to_int(std::vector<unsigned char> &bytes) {
    int integer;
    unsigned char *p = (unsigned char *)(&integer);
    for (int i = 0; i < int(bytes.size()); i++) {
        memcpy(p + i, &bytes[i], 1);
    }
    return integer;
}

std::vector<unsigned char> ECProject::int_to_bytes(int integer) {
    std::vector<unsigned char> bytes(sizeof(int));
    unsigned char *p = (unsigned char *)(&integer);
    for (int i = 0; i < int(bytes.size()); i++) {
        memcpy(&bytes[i], p + i, 1);
    }
    return bytes;
}

double ECProject::bytes_to_double(std::vector<unsigned char> &bytes)
{
    double doubler;
    memcpy(&doubler, bytes.data(), sizeof(double));
    return doubler;
}

std::vector<unsigned char> ECProject::double_to_bytes(double doubler)
{
    std::vector<unsigned char> bytes(sizeof(double));
    memcpy(bytes.data(), &doubler, sizeof(double));
    return bytes;
}