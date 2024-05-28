#ifndef LRC_H
#define LRC_H
#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "common.h"
#include "utils.h"

namespace ECProject
{
	int return_group_size(ECTYPE lrc_type, int k, int g, int l);
	int return_group_id(ECTYPE lrc_type, int k, int g, int l, int block_id);

	// full encoding matrix, with demensions as (g + l) × k
	bool make_encoding_matrix_Azu_LRC(int k, int g, int l, int *final_matrix);
	bool make_encoding_matrix_Azu_LRC_1(int k, int g, int l, int *final_matrix);
	bool make_encoding_matrix_Opt_LRC(int k, int g, int l, int *final_matrix);
	bool make_encoding_matrix_Opt_Cau_LRC(int k, int g, int l, int *final_matrix);
	bool make_encoding_matrix_Opt_Cau_LRC_v2(int k, int g, int l, int *final_matrix);
	bool make_encoding_matrix_Uni_Cau_LRC(int k, int g, int l, int *final_matrix);
	bool make_encoding_matrix_Uni_Cau_LRC_v2(int k, int g, int l, int *final_matrix);
	bool make_encoding_matrix(ECTYPE lrc_type, int k, int g, int l, int *final_matrix);

	// encoding matrix for a single local parity, with demensions as l × group_size
	bool make_group_matrix_Azu_LRC(int k, int g, int l, int *group_matrix, int group_id);
	bool make_group_matrix_Azu_LRC_1(int k, int g, int l, int *group_matrix, int group_id);
	bool make_group_matrix_Opt_LRC(int k, int g, int l, int *group_matrix, int group_id);
	bool make_group_matrix_Opt_Cau_LRC(int k, int g, int l, int *group_matrix, int group_id);
	bool make_group_matrix_Uni_Cau_LRC(int k, int g, int l, int *group_matrix, int group_id);
	bool make_group_matrix_Uni_Cau_LRC(int k, int g, int l, int *group_matrix, int group_id);
	bool make_group_matrix(ECTYPE lrc_type, int k, int g, int l, int *group_matrix, int group_id);

	// encode
	bool encode_Azu_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size);
	bool encode_Azu_LRC_1(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size);
	bool encode_Opt_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size);
	bool encode_Opt_Cau_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size);
	bool encode_Uni_Cau_LRC(int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size);
	bool encode_LRC(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size);
	bool decode_LRC(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size, int *erasures, int failed_num);
	bool decode_LRC_local(ECTYPE lrc_type, int k, int g, int l, int group_id, int group_size, char **data_ptrs, char **coding_ptrs, size_t block_size, int *erasures);

	// encode partial blocks for encoding global parity block, can be used in global parity recalculation in merging process
	bool encode_partial_blocks_for_encoding_LRC_global(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size, std::shared_ptr<std::vector<int>> datas_idx_ptr, std::shared_ptr<std::vector<int>> parities_idx_ptr);
    // global repair: encode partial blocks for decoding when using global parities
    bool encode_partial_blocks_for_decoding_LRC_global(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr);
    // local repair: encode partial blocks for decoding in a local group
    bool encode_partial_blocks_for_decoding_LRC_local(ECTYPE lrc_type, int k, int g, int l, char **data_ptrs, char **coding_ptrs, size_t block_size, std::shared_ptr<std::vector<int>> sls_idx_ptr, std::shared_ptr<std::vector<int>> svrs_idx_ptr, std::shared_ptr<std::vector<int>> fls_idx_ptr);

	void generate_stripe_information_for_Azu_LRC(int k, int l, int g, std::vector<std::vector<int>> &stripe_info);
	void return_block2group_Azu_LRC(int k, int l, int g, std::vector<int> &b2g);
	bool check_if_decodable_Azu_LRC(int k, int g, int l, std::vector<int> fls_idxs);
}


#endif