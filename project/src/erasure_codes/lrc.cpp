#include <lrc.h>
#include <common.h>

namespace ECProject
{
    int return_group_size(ECTYPE lrc_type, int k, int g, int l)
    {
        int r = -1;
        if(Azu_LRC == lrc_type || Opt_Cau_LRC == lrc_type)
        {
            r = (k + l - 1) / l;
        }
        else if(Azu_LRC_1 == lrc_type)
        {
            r = (k + l - 2) / (l - 1);
        }
        else if(Opt_LRC == lrc_type || Uni_Cau_LRC == lrc_type)
        {
            r = (k + g + l - 1) / l;
        }
        return r;
    }

    // data blocks, local parity blocks, global parity blocks in order
    int return_group_id(ECTYPE lrc_type, int k, int g, int l, int block_id)
    {
        int group_id = -1;
        int r = -1;
        if(Azu_LRC == lrc_type || Opt_Cau_LRC == lrc_type || Azu_LRC_1 == lrc_type)
        {
            r = (k + l - 1) / l;
            if(Azu_LRC_1 == lrc_type)
                r = (k + l - 2) / (l - 1);
            
            if(block_id < k)
            {
                group_id = block_id / r;
            }
            else if(block_id >= k && block_id < k + g)
            {
                group_id = l;
            }
            else
            {
                group_id = block_id - k - g;
            }
        }
        else if(Opt_LRC == lrc_type || Uni_Cau_LRC == lrc_type)
        {
            r = (k + g + l - 1) / l;
            if(block_id < k + g)
                group_id = block_id / r;
            else
                group_id = block_id - k - g;
        }
        return group_id;
    }

    bool make_encoding_matrix_Azu_LRC(int k, int g, int l, int *final_matrix)
    {
        int r = (k + l - 1) / l;
        int *matrix = reed_sol_vandermonde_coding_matrix(k, g + 1, 8);
        
        bzero(final_matrix, sizeof(int) * k * (g + l));

        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[i * k + j] = matrix[(i + 1) * k + j];
            }
        }

        for(int i = 0; i < l; i++)
        {
            for(int j = 0; j < k; j++)
            {
                if(i * r <= j && j < (i + 1) * r)
                {
                    final_matrix[(i + g) * k + j] = 1;
                }
            }
        }

        free(matrix);
        return true;
    }

    bool make_encoding_matrix_Azu_LRC_1(int k, int g, int l, int *final_matrix)     
    {
        int r = (k + l - 2) / (l - 1);
        int *matrix = reed_sol_vandermonde_coding_matrix(k, g + 1, 8);
        
        bzero(final_matrix, sizeof(int) * k * (g + l));

        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[i * k + j] = matrix[(i + 1) * k + j];
            }
        }

        std::vector<int> l_matrix(l * (k + g), 0);
        std::vector<int> d_g_matrix((k + g) * k, 0);
        int idx = 0;
        for(int i = 0; i < l - 1; i++)
        {
            int group_size = std::min(r, k - i * r);
            for(int j = 0; j < group_size; j++)
            {
                l_matrix[i * (k + g) + idx] = 1;
                idx++;
            }
        }
        for(int j = 0; j < g; j++)
        {
            l_matrix[(l - 1) * (k + g) + idx] = 1;
            idx++;
        }
        for(int i = 0; i < k; i++)
        {
            d_g_matrix[i * k + i] = 1;
        }
        idx = k * k;
        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                d_g_matrix[idx + i * k + j] = matrix[(i + 1) * k + j];
            }
        }

        int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(), l, k + g, k + g, k, 8);

        idx = g * k;
        for(int i = 0; i < l; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
            }
        }

        free(matrix);
        free(mix_matrix);
        return true;
    }

    // version 2, another version can be constructed like ECProject::make_encoding_matrix_Uni_Cau_LRC
    bool make_encoding_matrix_Opt_LRC(int k, int g, int l, int *final_matrix)
    {
        int r = (k + g + l - 1) / l;
        int *matrix = reed_sol_vandermonde_coding_matrix(k, g + 1, 8);
        
        bzero(final_matrix, sizeof(int) * k * (g + l));

        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[i * k + j] = matrix[(i + 1) * k + j];
            }
        }

        std::vector<int> l_matrix(l * (k + g), 0);
        std::vector<int> d_g_matrix((k + g) * k, 0);
        int idx = 0;
        for(int i = 0; i < l; i++)
        {
            int group_size = std::min(r, k + g - i * r);
            for(int j = 0; j < group_size; j++)
            {
                l_matrix[i * (k + g) + idx] = 1;
                idx++;
            }
        }
        for(int i = 0; i < k; i++)
        {
            d_g_matrix[i * k + i] = 1;
        }
        idx = k * k;
        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                d_g_matrix[idx + i * k + j] = matrix[(i + 1) * k + j];
            }
        }

        // print_matrix(l_matrix.data(), l, k + g, "l_matrix");
        // print_matrix(d_g_matrix.data(), k + g, k, "d_g_matrix");

        int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(), l, k + g, k + g, k, 8);

        idx = g * k;
        for(int i = 0; i < l; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
            }
        }

        free(matrix);
        free(mix_matrix);
        return true;
    }

    bool make_encoding_matrix_Opt_Cau_LRC(int k, int g, int l, int *final_matrix)
    {
        int r = (k + l - 1) / l;
        int *matrix = cauchy_good_general_coding_matrix(k, g + 1, 8);

        bzero(final_matrix, sizeof(int) * k * (g + l));

        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[i * k + j] = matrix[i * k + j];
            }
        }


        int d_idx = 0;
        for(int i = 0; i < l; i++)
        {
            for(int j = 0; j < k; j++)
            {
                if(i * r <= j && j < (i + 1) * r)
                {
                    final_matrix[(i + g) * k + j] = matrix[g * k + d_idx];
                    d_idx++;
                }
            }
        }

        // print_matrix(final_matrix, g + l, k, "final_matrix_before_xor");

        for(int i = 0; i < l; i++)
        {
            for(int j = 0; j < g; j++)
            {
                galois_region_xor((char *)&matrix[j * k], (char *)&final_matrix[(i + g) * k], 4 * k);
            }
        }

        // print_matrix(final_matrix, g + l, k, "final_matrix_after_xor");

        free(matrix);

        return true;
    }

    bool make_encoding_matrix_Opt_Cau_LRC_v2(int k, int g, int l, int *final_matrix)
    {
        int r = (k + l - 1) / l;
        int *matrix = cauchy_good_general_coding_matrix(k, g + 1, 8);
        
        bzero(final_matrix, sizeof(int) * k * (g + l));

        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[i * k + j] = matrix[i * k + j];
            }
        }

        std::vector<int> l_matrix(l * (k + g), 0);
        std::vector<int> d_g_matrix((k + g) * k, 0);
        int idx = 0;
        for(int i = 0; i < l; i++)
        {
            int group_size = std::min(r, k - i * r);
            for(int j = 0; j < group_size; j++)
            {
                l_matrix[i * (k + g) + idx] = matrix[g * k + idx];;
                idx++;
            }
            for(int j = k; j < k + g; j++)
            {
                l_matrix[i * (k + g) + j] = 1;
            }
        }
        for(int i = 0; i < k; i++)
        {
            d_g_matrix[i * k + i] = 1;
        }
        idx = k * k;
        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                d_g_matrix[idx + i * k + j] = matrix[i * k + j];
            }
        }

        // print_matrix(l_matrix.data(), l, k + g, "l_matrix");
        // print_matrix(d_g_matrix.data(), k + g, k, "d_g_matrix");

        int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(), l, k + g, k + g, k, 8);

        idx = g * k;
        for(int i = 0; i < l; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
            }
        }

        // print_matrix(final_matrix, g + l, k, "final_matrix");

        free(matrix);
        free(mix_matrix);
        return true;
    }

    bool make_encoding_matrix_Uni_Cau_LRC(int k, int g, int l, int *final_matrix)
    {
        int r = (k + g + l - 1) / l;
        int *matrix = cauchy_good_general_coding_matrix(k, g + 1, 8);

        bzero(final_matrix, sizeof(int) * k * (g + l));

        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[i * k + j] = matrix[i * k + j];
            }
        }

        std::vector<int> l_matrix(l * k, 0);
        int d_idx = 0;
        int l_idx = 0;
        for(int i = 0; i < l && d_idx < k; i++)
        {
            int group_size = std::min(r, k + g - i * r);
            for(int j = 0; j < group_size && d_idx < k; j++)
            {
                l_matrix[i * k + d_idx] = matrix[g * k + d_idx];
                d_idx++;
            }
            l_idx = i;
        }

        // print_matrix(l_matrix.data(), l, k, "l_matrix_before_xor");

        int g_idx = 0;
        for(int i = l_idx; i < l; i++)
        {
            int sub_g_num = -1;
            int group_size = std::min(r, k + g - i * r);
            if(group_size < r)  // must be the last group
            {
                sub_g_num = g - g_idx;
            }
            else
            {
                sub_g_num = (i + 1) * r - (k + g_idx);
            }
            std::vector<int> sub_g_matrix(sub_g_num * k, 0);
            for(int j = 0; j < sub_g_num; j++)
            {
                for(int jj = 0; jj < k; jj++)
                {
                    sub_g_matrix[j * k + jj] = matrix[g_idx * k + jj];
                }
                g_idx++;
            }
            for(int j = 0; j < sub_g_num; j++)
            {
                galois_region_xor((char *)&sub_g_matrix[j * k], (char *)&l_matrix[i * k], 4 * k);
            }
        }

        // print_matrix(l_matrix.data(), l, k, "l_matrix_after_xor");

        int idx = g * k;
        for(int i = 0; i < l; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[idx + i * k + j] = l_matrix[i * k + j];
            }
        }

        // print_matrix(final_matrix, g + l, k, "final_matrix");

        free(matrix);

        return true;
    }

    bool make_encoding_matrix_Uni_Cau_LRC_v2(int k, int g, int l, int *final_matrix)
    {
        int r = (k + g + l - 1) / l;
        int *matrix = cauchy_good_general_coding_matrix(k, g + 1, 8);
        
        bzero(final_matrix, sizeof(int) * k * (g + l));

        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[i * k + j] = matrix[i * k + j];
            }
        }

        std::vector<int> l_matrix(l * (k + g), 0);
        std::vector<int> d_g_matrix((k + g) * k, 0);
        int idx = 0;
        for(int i = 0; i < l; i++)
        {
            int group_size = std::min(r, k + g - i * r);
            for(int j = 0; j < group_size; j++)
            {   
                if(idx < k)
                {
                    l_matrix[i * (k + g) + idx] = matrix[g * k + idx];  
                }
                else
                {
                    l_matrix[i * (k + g) + idx] = 1;
                }
                idx++;
            }
        }
        for(int i = 0; i < k; i++)
        {
            d_g_matrix[i * k + i] = 1;
        }
        idx = k * k;
        for(int i = 0; i < g; i++)
        {
            for(int j = 0; j < k; j++)
            {
                d_g_matrix[idx + i * k + j] = matrix[i * k + j];
            }
        }

        // print_matrix(l_matrix.data(), l, k + g, "l_matrix");
        // print_matrix(d_g_matrix.data(), k + g, k, "d_g_matrix");

        int *mix_matrix = jerasure_matrix_multiply(l_matrix.data(), d_g_matrix.data(), l, k + g, k + g, k, 8);

        idx = g * k;
        for(int i = 0; i < l; i++)
        {
            for(int j = 0; j < k; j++)
            {
                final_matrix[idx + i * k + j] = mix_matrix[i * k + j];
            }
        }

        // print_matrix(final_matrix, g + l, k, "final_matrix");

        free(matrix);
        free(mix_matrix);
        return true;
    }

    bool make_encoding_matrix(ECTYPE lrc_type, int k, int g, int l, int *final_matrix)
    {
        if(lrc_type == Azu_LRC)
        {
            return make_encoding_matrix_Azu_LRC(k, g, l, final_matrix);
        }
        else if(lrc_type == Azu_LRC_1)
        {
            return make_encoding_matrix_Azu_LRC_1(k, g, l, final_matrix);
        }
        else if(lrc_type == Opt_LRC)
        {
            return make_encoding_matrix_Opt_LRC(k, g, l, final_matrix);
        }
        else if(lrc_type == Opt_Cau_LRC)
        {
            return make_encoding_matrix_Opt_Cau_LRC(k, g, l, final_matrix);
        }
        else if(lrc_type == Uni_Cau_LRC)
        {
            return make_encoding_matrix_Uni_Cau_LRC(k, g, l, final_matrix);
        }
        return false;
    }

    bool make_group_matrix_Azu_LRC(int k, int g, int l, int *group_matrix, int group_id)
    {
        int r = (k + l - 1) / l;

        for(int i = 0; i < l; i++)
        {
            if(i == group_id)
            {
                int group_size = std::min(r, k - i * r);
                for(int j = 0; j < group_size; j++)
                {
                    group_matrix[j] = 1;
                }
            }
        }
        return true;
    }

    bool make_group_matrix_Azu_LRC_1(int k, int g, int l, int *group_matrix, int group_id)
    {
        int r = (k + l - 2) / (l - 1);
        for(int i = 0; i < l - 1; i++)
        {
            if(i == group_id)
            {
                int group_size = std::min(r, k - i * r);
                for(int j = 0; j < group_size; j++)
                {
                    group_matrix[j] = 1;
                }
            }
        }
        for(int j = k; j < k + g; j++)
        {
            group_matrix[j] = 1;
        }
        return true;
    }

    bool make_group_matrix_Opt_LRC(int k, int g, int l, int *group_matrix, int group_id)
    {
        int r = (k + g + l - 1) / l;
        for(int i = 0; i < l; i++)
        {
            if(i == group_id)
            {
                int group_size = std::min(r, k + g - i * r);
                for(int j = 0; j < group_size; j++)
                {
                    group_matrix[j] = 1;
                }
            }
        }
        return true;
    }

    bool make_group_matrix_Opt_Cau_LRC(int k, int g, int l, int *group_matrix, int group_id)
    {
        int r = (k + l - 1) / l;
        int *matrix = cauchy_good_general_coding_matrix(k, g + 1, 8);
        int idx = 0;
        for(int i = 0; i < l; i++)
        {
            int group_size = std::min(r, k - i * r);
            for(int j = 0; j < group_size; j++)
            {
                if(i == group_id)
                    group_matrix[j] = matrix[g * k + idx];;
                idx++;
            }
            for(int j = group_size; j < group_size + g; j++)
            {
                if(i == group_id)
                    group_matrix[j] = 1;
            }
        }
        return true;
    }

    bool make_group_matrix_Uni_Cau_LRC(int k, int g, int l, int *group_matrix, int group_id)
    {
        int r = (k + g + l - 1) / l;
        int *matrix = cauchy_good_general_coding_matrix(k, g + 1, 8);
        int idx = 0;
        for(int i = 0; i < l; i++)
        {
            int group_size = std::min(r, k + g - i * r);
            for(int j = 0; j < group_size; j++)
            {   
                if(i == group_id)
                {
                    if(idx < k)
                    {
                        group_matrix[j] = matrix[g * k + idx];  
                    }
                    else
                    {
                        group_matrix[j] = 1;
                    }
                }
                idx++;
            }
        }
        std::cout << std::endl;
        return true;
    }

    bool make_group_matrix(ECTYPE lrc_type, int k, int g, int l, int *group_matrix, int group_id)
    {
        if(lrc_type == Azu_LRC)
        {
            return make_group_matrix_Azu_LRC(k, g, l, group_matrix, group_id);
        }
        else if(lrc_type == Azu_LRC_1)
        {
            return make_group_matrix_Azu_LRC_1(k, g, l, group_matrix, group_id);
        }
        else if(lrc_type == Opt_LRC)
        {
            return make_group_matrix_Opt_LRC(k, g, l, group_matrix, group_id);
        }
        else if(lrc_type == Opt_Cau_LRC)
        {
            return make_group_matrix_Opt_Cau_LRC(k, g, l, group_matrix, group_id);
        }
        else if(lrc_type == Uni_Cau_LRC)
        {
            return make_group_matrix_Uni_Cau_LRC(k, g, l, group_matrix, group_id);
        }
        return false;
    }

    bool encode_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size)
    {
        std::vector<int> lrc_matrix((g + l) * k, 0);
        make_encoding_matrix_Azu_LRC(k, g, l, lrc_matrix.data());
        jerasure_matrix_encode(k, g + l, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
        return true;
    }

    bool encode_Azu_LRC_1(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size)
    {
        // std::vector<int> lrc_matrix((g + l - 1) * k, 0);
        // make_encoding_matrix_Azu_LRC(k, g, l - 1, lrc_matrix.data());
        // jerasure_matrix_encode(k, g + l - 1, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
        // std::vector<int> new_matrix(g, 1);
        // jerasure_matrix_encode(g, 1, 8, new_matrix.data(), coding_ptrs, &coding_ptrs[g + l - 1], block_size);
        std::vector<int> lrc_matrix((g + l) * k, 0);
        make_encoding_matrix_Azu_LRC_1(k, g, l, lrc_matrix.data());
        jerasure_matrix_encode(k, g + l, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
        return true;
    }

    bool encode_Opt_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size)
    {
        // int *matrix = reed_sol_vandermonde_coding_matrix(k, g, 8);
        // free(matrix);
        // std::vector<int> group_number(l, 0);
        // int r = (k + g + l - 1) / l;
        // for(int i = 0; i < l; i++)
        // {
        //     int group_size = std::min(r, k + g - i * r);
        //     std::vector<char *> vector_number(group_size);
        //     char **new_data = (char **)vector_number.data();
        //     for(int j = 0; j < group_size; j++)
        //     {
        //         if(i * g + j < k)
        //         {
        //             new_data[j] = data_ptrs[i * g + j];
        //         }
        //         else
        //         {
        //             new_data[j] = coding_ptrs[i * g + j - k];
        //         }
        //     }
        //     std::vector<int> new_matrix(group_size, 1);
        //     jerasure_matrix_encode(group_size, 1, 8, new_matrix.data(), new_data, &coding_ptrs[g + i], block_size);
        // }
        std::vector<int> lrc_matrix((g + l) * k, 0);
        make_encoding_matrix_Opt_LRC(k, g, l, lrc_matrix.data());
        jerasure_matrix_encode(k, g + l, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
        return true;
    }

    bool encode_Opt_Cau_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size)
    {
        std::vector<int> lrc_matrix((g + l) * k, 0);
        make_encoding_matrix_Opt_Cau_LRC(k, g, l, lrc_matrix.data());
        jerasure_matrix_encode(k, g + l, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
        return true;
    }

    bool encode_Uni_Cau_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size)
    {
        std::vector<int> lrc_matrix((g + l) * k, 0);
        make_encoding_matrix_Uni_Cau_LRC(k, g, l, lrc_matrix.data());
        jerasure_matrix_encode(k, g + l, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
        return true;
    }

    bool encode_LRC(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size)
    {
        std::vector<int> lrc_matrix((g + l) * k, 0);
        make_encoding_matrix(lrc_type, k, g, l, lrc_matrix.data());
        // print_matrix(lrc_matrix.data(), g + l, k, "lrc_matrix");
        jerasure_matrix_encode(k, g + l, 8, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size);
        return true;
    }

    bool decode_LRC(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size, int *erasures, int failed_num)
    {
        std::vector<int> lrc_matrix((g + l) * k, 0);
        make_encoding_matrix(lrc_type, k, g, l, lrc_matrix.data());
        int i = 0;
        i = jerasure_matrix_decode(k, g + l, 8, lrc_matrix.data(), failed_num, erasures, data_ptrs, coding_ptrs, block_size);
        if(i == -1)
        {
            std::cout << "[Decode] Failed!" << std::endl;
            return false;
        }
        return true;
    }

    bool decode_LRC_local(ECTYPE lrc_type, int k, int g, int l, int group_id, int group_size, char **data_ptrs, char **coding_ptrs, size_t block_size, int *erasures)
    {
        std::vector<int> group_matrix(1 * group_size, 0);
        make_group_matrix(lrc_type, k, g, l, group_matrix.data(), group_id);
        int i = 0;
        i = jerasure_matrix_decode(group_size, 1, 8, group_matrix.data(), 1, erasures, data_ptrs, coding_ptrs, block_size);
        if(i == -1)
        {
            std::cout << "[Decode] Failed!" << std::endl;
            return false;
        }
        return true;
    }

    bool encode_partial_blocks_for_encoding_LRC_global(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size, std::shared_ptr<std::vector<int>> datas_idx_ptr, std::shared_ptr<std::vector<int>> parities_idx_ptr)
    {
        std::vector<int> lrc_matrix((k + g + l) * k, 0);
        get_full_matrix(lrc_matrix.data(), k);
        make_encoding_matrix(lrc_type, k, g, l, &(lrc_matrix.data())[k * k]);
        return encode_partial_blocks_for_encoding(k, g + l, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size, datas_idx_ptr, parities_idx_ptr);
    }

    bool encode_partial_blocks_for_decoding_LRC_global(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr)
    {
        std::vector<int> lrc_matrix((k + g + l) * k, 0);
        get_full_matrix(lrc_matrix.data(), k);
        make_encoding_matrix(lrc_type, k, g, l, &(lrc_matrix.data())[k * k]);
        return encode_partial_blocks_for_decoding(k, g + l, lrc_matrix.data(), data_ptrs, coding_ptrs, block_size, sls_idx_ptr, svrs_idx_ptr, fls_idx_ptr);
    }

    bool encode_partial_blocks_for_decoding_LRC_local(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr)
    {
        int group_size = int(svrs_idx_ptr->size());
        int group_id = -1;
        int min_idx = k + g + l;
        for(auto it = svrs_idx_ptr->begin(); it != svrs_idx_ptr->end(); it++)
        {
            int idx = *it;
            if(idx >= k + g)
                group_id = idx - k - g;
            if(idx < min_idx)
                min_idx = idx;
        }
        for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++)
        {
            int idx = *it;
            if(idx >= k + g)
                group_id = idx - k - g;
            if(idx < min_idx)
                min_idx = idx;
        }

        auto sls_idx_ptr_ = std::make_shared<std::vector<int>>();
        auto fls_idx_ptr_ = std::make_shared<std::vector<int>>();
        auto svrs_idx_ptr_ = std::make_shared<std::vector<int>>();
        for(auto it = svrs_idx_ptr->begin(); it != svrs_idx_ptr->end(); it++)
        {
            int idx = *it;
            if(idx >= k + g)
                svrs_idx_ptr_->push_back(group_size);
            else
                svrs_idx_ptr_->push_back(idx - min_idx);
        }
        for(auto it = fls_idx_ptr->begin(); it != fls_idx_ptr->end(); it++)
        {
            int idx = *it;
            if(idx >= k + g)
                fls_idx_ptr_->push_back(group_size);
            else
                fls_idx_ptr_->push_back(idx - min_idx);
        }
        for(auto it = sls_idx_ptr->begin(); it != sls_idx_ptr->end(); it++)
        {
            int idx = *it;
            if(idx >= k + g)
                sls_idx_ptr_->push_back(group_size);
            else
                sls_idx_ptr_->push_back(idx - min_idx);
        }

        std::vector<int> group_matrix((group_size + 1) * group_size, 0);
        get_full_matrix(group_matrix.data(), group_size);
        make_group_matrix(lrc_type, k, g, l, &(group_matrix.data())[group_size * group_size], group_id);
        // print_matrix(group_matrix.data(), group_size + 1, group_size, "group_matrix");
        return encode_partial_blocks_for_decoding(group_size, 1, group_matrix.data(), data_ptrs, coding_ptrs, block_size, sls_idx_ptr_, svrs_idx_ptr_, fls_idx_ptr_);
    }

    void generate_stripe_information_for_Azu_LRC(int k, int l, int g, std::vector<std::vector<int>> &stripe_info)
    {
        int r = (k + l - 1) / l;
        int idx = 0;
        for(int i = 0; i < l; i++)
        {
            std::vector<int> local_group;
            int group_size = std::min(r, k - i * r);
            for (int j = 0; j < group_size; j++)
            {
                local_group.push_back(idx);
                idx++;
            }
            local_group.push_back(k + g + i);
            stripe_info.push_back(local_group);
        }
        std::vector<int> local_group;
        for(int i = 0; i < g; i++)
        {
            local_group.push_back(idx);
            idx++;
        }
        stripe_info.push_back(local_group);
    }

    void return_block2group_Azu_LRC(int k, int l, int g, std::vector<int> &b2g)
    {
        int r = (k + l - 1) / l;
        int idx = 0;
        for(int i = 0; i < l; i++)
        {
            int group_size = std::min(r, k - i * r);
            for (int j = 0; j < group_size; j++)
            {
                b2g[idx] = i;
                idx++;
            }
            b2g[k + g + i] = i;
        }
        for(int i = 0; i < g; i++)
        {
            b2g[idx] = l;
            idx++;
        }
    }

    bool check_if_decodable_Azu_LRC(int k, int g, int l, std::vector<int> fls_idxs)
    {
        int r = (k + l - 1) / l;
        std::unordered_map<int, int> b2g;
        std::vector<int> group_fd_cnt;
        std::vector<int> group_fgp_cnt;
        std::vector<int> group_slp_cnt;
        int sgp_cnt = g;
        int idx = 0;
        for (int i = 0; i < l; i++)
        {
            int group_size = std::min(r, k - i * r);
            for (int j = 0; j < group_size; j++)
            {
                b2g.insert(std::make_pair(idx, i));
                idx++;
            }
            b2g.insert(std::make_pair(k + g + i, i));
            group_fd_cnt.push_back(0);
            group_slp_cnt.push_back(1);
        }

        for (auto it = fls_idxs.begin(); it != fls_idxs.end(); it++)
        {
            int block_id = *it;
            if (block_id < k)
            {
                group_fd_cnt[b2g[block_id]] += 1;
            }
            else if (block_id < k + g && block_id >= k)
            {
                sgp_cnt -= 1;
            }
            else
            {
                group_slp_cnt[block_id - k - g] -= 1;
            }
        }
        for (int i = 0; i < l; i++)
        {
            if (group_slp_cnt[i] && group_slp_cnt[i] <= group_fd_cnt[i])
            {
                group_fd_cnt[i] -= group_slp_cnt[i];
                group_slp_cnt[i] = 0;
            }
        }
        for (int i = 0; i < l; i++)
        {
            if (sgp_cnt >= group_fd_cnt[i])
            {
                sgp_cnt -= group_fd_cnt[i];
                group_fd_cnt[i] = 0;
            }
            else
            {
                return false;
            }
        }
        return true;
    }
}