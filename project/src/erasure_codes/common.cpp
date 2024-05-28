#include "common.h"

void ECProject::print_matrix(int *matrix, int rows, int cols, std::string msg)
{
	std::cout << msg << ":" << std::endl;
	for(int i = 0; i < rows; i++)
	{
		for(int j = 0; j < cols; j++)
		{
			std::cout << matrix[i * cols + j] << " ";
		}
		std::cout << std::endl;
	}
}

void ECProject::get_full_matrix(int *matrix, int k)
{
	for(int i = 0; i < k; i++)
	{
		matrix[i * k + i] = 1;
	}
}

bool ECProject::make_submatrix_by_rows(int cols, int *matrix, int *new_matrix, std::shared_ptr<std::vector<int>> blocks_idx_ptr)
{
	int i = 0;
	for (auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++)
	{
		int j = *it;
		memcpy(&new_matrix[i * cols], &matrix[j * cols], cols * sizeof(int));
		i++;
	}
	return true;
}

bool ECProject::make_submatrix_by_cols(int cols, int rows, int *matrix, int *new_matrix, std::shared_ptr<std::vector<int>> blocks_idx_ptr)
{
	int block_num = int(blocks_idx_ptr->size());
	int i = 0;
	for (auto it = blocks_idx_ptr->begin(); it != blocks_idx_ptr->end(); it++)
	{
		int j = *it;
		for(int u = 0; u < rows; u++)
		{
			new_matrix[u * block_num + i] = matrix[u * cols + j];
		}
		i++;
	}
	return true;
}

/*
	we assume the blocks is organized as followed:
	data_ptrs = [B11, ..., Bp1, B12, ..., Bp2, ........., B1n, ..., Bpn]
	coding_ptrs = [P1, ..., Pn]
	block_num = p * n, parity_num = p
	then Pi = Bi1 + Bi2 + ... + Bin, 1 <= i <= p
*/
bool ECProject::perform_addition(char **data_ptrs, char **coding_ptrs, size_t block_size, int block_num, int parity_num)
{
    if(block_num % parity_num != 0)
    {
        printf("invalid! %d mod %d != 0\n", block_num, parity_num);
        return false;
    }
    int block_num_per_parity = block_num / parity_num;

    std::vector<char *> t_data(block_num);
    char **data = (char **)t_data.data();
	int cnt = 0;
	for(int i = 0; i < parity_num; i++)
	{
		for(int j = 0; j < block_num_per_parity; j++)
		{
			data[cnt++] = data_ptrs[j * parity_num + i];
		}
	}

    for(int i = 0; i < parity_num; i++)
    {
        std::vector<int> new_matrix(1 * block_num_per_parity, 1);
        jerasure_matrix_encode(block_num_per_parity, 1, 8, new_matrix.data(), &data[i * block_num_per_parity], &coding_ptrs[i], block_size);
    }
    return true;
}

// any parity_idx >= k
bool ECProject::encode_partial_blocks_for_encoding(int k, int m, int *full_matrix, char **data_ptrs, char **coding_ptrs, size_t block_size, std::shared_ptr<std::vector<int>> datas_idx_ptr, std::shared_ptr<std::vector<int>> parities_idx_ptr)
{
	int block_num = int(datas_idx_ptr->size());
	int parity_num = int(parities_idx_ptr->size());
	std::vector<int> matrix(parity_num * k, 0);
	make_submatrix_by_rows(k, full_matrix, matrix.data(), parities_idx_ptr);

	std::vector<int> new_matrix(parity_num * block_num, 1);
	make_submatrix_by_cols(k, parity_num, matrix.data(), new_matrix.data(), datas_idx_ptr);

    jerasure_matrix_encode(block_num, parity_num, 8, new_matrix.data(), data_ptrs, coding_ptrs, block_size);
	return true;
}

bool ECProject::encode_partial_blocks_for_decoding(int k, int m, int *full_matrix, char **data_ptrs, char **coding_ptrs, size_t block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr)
{
	int local_survivors_num = int(sls_idx_ptr->size());
	int failures_num = int(fls_idx_ptr->size());
	std::vector<int> failures_matrix(failures_num * k, 0);
	std::vector<int> survivors_matrix(k * k, 0);
	make_submatrix_by_rows(k, full_matrix, failures_matrix.data(), fls_idx_ptr);
	make_submatrix_by_rows(k, full_matrix, survivors_matrix.data(), svrs_idx_ptr);
	// print_matrix(failures_matrix.data(), failures_num, k, "failures_matrix");
	// print_matrix(survivors_matrix.data(), k, k, "survivors_matrix");

	std::vector<int> inverse_matrix(k * k, 0);
	jerasure_invert_matrix(survivors_matrix.data(), inverse_matrix.data(), k, 8);
	// print_matrix(inverse_matrix.data(), k, k, "inverse_matrix");
	
	int *decoding_matrix = jerasure_matrix_multiply(failures_matrix.data(), inverse_matrix.data(), failures_num, k, k, k, 8);
	std::vector<int> encoding_matrix(failures_num * local_survivors_num, 0);
	int i = 0;
	for(auto it2 = sls_idx_ptr->begin(); it2 != sls_idx_ptr->end(); it2++, i++)
	{
		int idx = 0;
		for(auto it3 = svrs_idx_ptr->begin(); it3 != svrs_idx_ptr->end(); it3++, idx++)
		{
			if(*it2 == *it3)
				break;
		}

		for(int u = 0; u < failures_num; u++)
		{
			encoding_matrix[u * local_survivors_num + i] = decoding_matrix[u * k + idx];
		}
	}
	jerasure_matrix_encode(local_survivors_num, failures_num, 8, encoding_matrix.data(), data_ptrs, coding_ptrs, block_size);
	free(decoding_matrix);
	
	return true;
}
