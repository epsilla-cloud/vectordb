#pragma once

#include <algorithm>
#include <boost/dynamic_bitset.hpp>
#include <cfloat>
#include <cstring>
#include <fstream>
#include <iostream>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "db/execution/candidate.hpp"
#include "db/index/space_l2.hpp"

namespace vectordb {
namespace engine {
namespace execution {

constexpr const int BruteforceThreshold = 1024;

class VecSearchExecutor {
 public:
  int64_t ntotal_ = 0;
  int64_t dimension_ = 0;
  int64_t start_search_point_ = 0;

  int64_t* offset_table_;   // The offset table for neighbor list for each node.
  int64_t* neighbor_list_;  // The neighbor list for each node consecutively stored.
  float* vector_table_;     // The vector table for each node consecutively stored.

  // Distance calculation function
  DISTFUNC<float> fstdistfunc_;
  void* dist_func_param_;

  // Query parameters
  int num_threads_;               // = 4;
  int64_t L_master_;              // = 100;
  int64_t L_local_;               // = 100;
  int64_t subsearch_iterations_;  // = 15;
  std::vector<int64_t> search_result_;
  std::vector<int64_t> init_ids_;
  boost::dynamic_bitset<> is_visited_;
  std::vector<Candidate> set_L_;
  std::vector<int64_t> local_queues_sizes_;
  std::vector<int64_t> local_queues_starts_;
  bool brute_force_search_;
  std::vector<Candidate> brute_force_queue_;

  VecSearchExecutor(
      const int64_t ntotal,
      const int64_t dimension,
      const int64_t start_search_point,
      int64_t* offset_table,
      int64_t* neighbor_list,
      float* vector_table,
      DISTFUNC<float> fstdistfunc,
      void* dist_func_param,
      int num_threads,
      int64_t L_master,
      int64_t L_local,
      int64_t subsearch_iterations);

  static int64_t AddIntoQueue(
      std::vector<Candidate>& queue,
      const int64_t queue_start,
      int64_t& queue_size,
      const int64_t queue_capacity,
      const Candidate& cand);

  static void AddIntoQueueAt(
      const Candidate& cand,
      std::vector<Candidate>& queue,
      const int64_t insert_index,
      const int64_t queue_start,
      int64_t& queue_top,
      const int64_t queue_size);

  static void InsertOneElementAt(
      const Candidate& cand,
      std::vector<Candidate>& queue_base,
      const int64_t insert_index,
      const int64_t queue_start,
      const int64_t queue_size);

  static int64_t MergeTwoQueuesInto1stQueueSeqFixed(
      std::vector<Candidate>& queue1,
      const int64_t queue1_start,
      const int64_t queue1_size,
      std::vector<Candidate>& queue2,
      const int64_t queue2_start,
      const int64_t queue2_size);

  static int64_t MergeTwoQueuesInto1stQueueSeqIncr(
      std::vector<Candidate>& queue1,
      const int64_t queue1_start,
      int64_t& queue1_size,
      const int64_t queue1_length,
      std::vector<Candidate>& queue2,
      const int64_t queue2_start,
      const int64_t queue2_size);

  int64_t MergeAllQueuesToMaster(
      std::vector<Candidate>& set_L,
      const std::vector<int64_t>& local_queues_starts,
      std::vector<int64_t>& local_queues_sizes,
      const int64_t local_queue_capacity,
      const int64_t L) const;

  int64_t ExpandOneCandidate(
      const int worker_id,
      const int64_t cand_id,
      const float* query_data,
      const float& dist_bound,
      float& dist_thresh,
      std::vector<Candidate>& set_L,
      const int64_t local_queue_start,
      int64_t& local_queue_size,
      const int64_t& local_queue_capacity,
      boost::dynamic_bitset<>& is_visited,
      uint64_t& local_count_computation);

  int64_t PickTopMToWorkers(
      std::vector<Candidate>& set_L,
      const std::vector<int64_t>& local_queues_starts,
      std::vector<int64_t>& local_queues_sizes,
      const int64_t local_queue_capacity,
      const int64_t k_uc) const;

  void PickTopMUnchecked(
      const int64_t M,
      const int64_t k_uc,
      std::vector<Candidate>& set_L,
      const int64_t local_queue_start,
      const int64_t local_queue_size,
      std::vector<int64_t>& top_m_candidates,
      int64_t& top_m_candidates_size,
      int64_t& last_k) const;

  void InitializeSetLPara(
      const float* query_data,
      const int64_t L,
      std::vector<Candidate>& set_L,
      const int64_t set_L_start,
      int64_t& set_L_size,
      const std::vector<int64_t>& init_ids,
      boost::dynamic_bitset<>& is_visited);

 public:
  //   uint64_t count_distance_computation_ = 0;

  ~VecSearchExecutor() {}

  void PrepareInitIds(
      std::vector<int64_t>& init_ids,
      const int64_t L) const;

  void SearchImpl(
      const float* query_data,
      const int64_t K,
      const int64_t L,
      std::vector<Candidate>& set_L,
      const std::vector<int64_t>& init_ids,
      std::vector<int64_t>& set_K,
      const int64_t local_queue_capacity,
      const std::vector<int64_t>& local_queues_starts,
      std::vector<int64_t>& local_queues_sizes,
      boost::dynamic_bitset<>& is_visited,
      const int64_t index_threshold);

  bool Search(const float* query_data, const int64_t K);
};  // Class VecSearchExecutor

}  // namespace execution
}  // namespace engine
}  // namespace vectordb
