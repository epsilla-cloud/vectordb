#pragma once

#include <algorithm>
#include <cfloat>
#include <cstring>
#include <fstream>
#include <iostream>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "db/ann_graph_segment.hpp"
#include "db/execution/candidate.hpp"
#include "db/index/space_l2.hpp"
#include "db/table_segment.hpp"
#include "db/vector.hpp"
#include "query/expr/expr_evaluator.hpp"
#include "query/expr/expr_types.hpp"
#include "utils/status.hpp"
#include "db/catalog/meta.hpp"

namespace vectordb {
namespace engine {
namespace execution {

constexpr const int BruteforceThreshold = 512;

class VecSearchExecutor {
 public:
  std::shared_ptr<ANNGraphSegment> ann_index_;  // Holding a pointer to make sure it doesn't get released prematurely during rebuild.
  int64_t total_indexed_vector_ = 0;            // The total number of nodes in the graph. Vector table could have more nodes (passed in at search time).
  int64_t dimension_ = 0;
  int64_t start_search_point_ = 0;

  int64_t* offset_table_;           // The offset table for neighbor list for each node.
  int64_t* neighbor_list_;          // The neighbor list for each node consecutively stored.
  VectorColumnData vector_column_;  // The vector column for each node consecutively stored.

  // Distance calculation function
  DistFunc fstdistfunc_;
  void* dist_func_param_;

  // Query parameters
  int num_threads_;               // = 4;
  int64_t L_master_;              // = 100;
  int64_t L_local_;               // = 100;
  int64_t subsearch_iterations_;  // = 15;
  bool prefilter_enabled_;        // = false;
  std::vector<int64_t> search_result_;
  std::vector<double> distance_;
  std::vector<int64_t> init_ids_;
  std::vector<bool> is_visited_;
  std::vector<Candidate> set_L_;
  std::vector<int64_t> local_queues_sizes_;
  std::vector<int64_t> local_queues_starts_;
  bool brute_force_search_;
  std::vector<Candidate> brute_force_queue_;

  VecSearchExecutor(
      const int64_t dimension,
      const int64_t start_search_point,
      std::shared_ptr<ANNGraphSegment> ann_index,
      int64_t* offset_table,
      int64_t* neighbor_list,
      std::variant<DenseVectorColumnDataContainer, VariableLenAttrColumnContainer*> vector_column,
      DistFunc fstdistfunc,
      void* dist_func_param,
      int num_threads,
      int64_t L_master,
      int64_t L_local,
      int64_t subsearch_iterations,
      bool prefilter_enabled);

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
      const VectorPtr query_data,
      const float& dist_bound,
      // float& dist_thresh,
      std::vector<Candidate>& set_L,
      const int64_t local_queue_start,
      int64_t& local_queue_size,
      const int64_t& local_queue_capacity,
      std::vector<bool>& is_visited,
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
      const VectorPtr query_data,
      const int64_t L,
      std::vector<Candidate>& set_L,
      const int64_t set_L_start,
      int64_t& set_L_size,
      const std::vector<int64_t>& init_ids,
      std::vector<bool>& is_visited);

 public:
  //   uint64_t count_distance_computation_ = 0;

  ~VecSearchExecutor() {}

  void PrepareInitIds(
      std::vector<int64_t>& init_ids,
      const int64_t L) const;

  void SearchImpl(
      const VectorPtr query_data,
      const int64_t K,
      const int64_t L,
      std::vector<Candidate>& set_L,
      const std::vector<int64_t>& init_ids,
      std::vector<int64_t>& set_K,
      const int64_t local_queue_capacity,
      const std::vector<int64_t>& local_queues_starts,
      std::vector<int64_t>& local_queues_sizes,
      std::vector<bool>& is_visited,
      const int64_t index_threshold);

  bool BruteForceSearch(
      const VectorPtr query_data,
      const int64_t start,
      const int64_t end,
      const ConcurrentBitset& deleted,
      vectordb::query::expr::ExprEvaluator& expr_evaluator,
      vectordb::engine::TableSegment* table_segment,
      const int root_node_index);
  bool PreFilterBruteForceSearch(
      const VectorPtr query_data,
      const int64_t start,
      const int64_t end,
      const ConcurrentBitset& deleted,
      vectordb::query::expr::ExprEvaluator& expr_evaluator,
      vectordb::engine::TableSegment* table_segment,
      const int root_node_index);
  Status Search(
      const VectorPtr query_data,
      vectordb::engine::TableSegment* table_segment,
      const size_t limit,
      std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes,
      int64_t& result_size);

  Status SearchByAttribute(
      meta::TableSchema& table_schema,
      vectordb::engine::TableSegment* table_segment,
      const size_t skip,
      const size_t limit,
      vectordb::Json& primary_keys,
      std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes,
      int64_t& result_size);
};  // Class VecSearchExecutor

}  // namespace execution
}  // namespace engine
}  // namespace vectordb

/**
 *

  std::srand(std::time(nullptr));
  int x = 100, y = 100;
  int n = x * y;
  int dim = 1536;
  float *data = new float[n * dim];
  for (int i = 0; i < n; i++) {
    for (int p = 0; p < dim; ++p) {
      data[i * dim + p] = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
    }
  }
  std::cout << "here" << std::endl;
  // vectordb::engine::index::Graph knng(n);
  // vectordb::engine::index::KNNGraph graph(n, dim, 100, data, knng);

  auto ann_graph_segment = std::make_shared<vectordb::engine::ANNGraphSegment>();
  ann_graph_segment->BuildFromVectorTable(data, n, dim);
  // ann_graph_segment->Debug();
  std::cout << ann_graph_segment->navigation_point_ << std::endl;

  auto space_ = new vectordb::L2Space(dim);
  auto fstdistfunc_ = space_->get_dist_func();
  auto dist_func_param_ = space_->get_dist_func_param();

  omp_set_max_active_levels(2);
  omp_set_num_threads(4);
  std::cout << "start query" << std::endl;
  float *query = new float[dim];
  for (int p = 0; p < dim; ++p) {
    query[p] = static_cast<double>(std::rand()) / static_cast<double>(RAND_MAX);
    // std::cout << query[p] << " ";
  }
// #pragma omp parallel
  {
    auto executor = std::make_shared<vectordb::engine::execution::VecSearchExecutor>(
        n, dim,
        ann_graph_segment->navigation_point_,
        ann_graph_segment->offset_table_,
        ann_graph_segment->neighbor_list_,
        data,
        fstdistfunc_, dist_func_param_,
        4, 100, 100, 15);
    executor->brute_force_queue_.resize(10000000);

    int K = 10;
    for (auto i = 0; i < 100000000; i++) {
      // std::cout << std::endl;
      // std::cout << "Query" << std::endl;
      // for (int j = 0; j < dim; ++j) {
      //   std::cout << query[j] << " ";
      // }
      // std::cout << std::endl;
      // std::cout << "Compare" << std::endl;
      // executor->brute_force_search_ = false;
      executor->Search(query, K);
      // for (int j = 0; j < K; ++j) {
      //   std::cout << executor->search_result_[j] << " ";
      // }
      // std::cout << std::endl;
      // executor->brute_force_search_ = true;
      // executor->Search(query, K);
      // for (int j = 0; j < K; ++j) {
      //   std::cout << executor->search_result_[j] << " ";
      // }
      // std::cout << std::endl;
    }
  }
*/