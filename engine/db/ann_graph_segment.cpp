#include "db/ann_graph_segment.hpp"

#include "db/index/knn/knn.hpp"
#include "db/index/nsg/nsg.hpp"
#include <iostream>

namespace vectordb {
namespace engine {

struct NSGConfig {
  const size_t search_length;
  const size_t out_degree;
  const size_t candidate_pool_size;
  const size_t knng;

  NSGConfig(size_t search_length_, size_t out_degree_, size_t candidate_pool_size_, size_t knng_)
      : search_length(search_length_),
        out_degree(out_degree_),
        candidate_pool_size(candidate_pool_size_),
        knng(knng_) {}
};

// Recommended default: 45, 50, 300, 100
const NSGConfig Default_NSG_Config(45, 16, 60, 16);

ANNGraphSegment::ANNGraphSegment()
    : synced_with_disk_(false),
      first_record_id_(0),
      record_number_(0),
      offset_table_(nullptr),
      neighbor_list_(nullptr) {}

ANNGraphSegment::ANNGraphSegment(std::string& db_catalog_path)
    : synced_with_disk_(true),
      first_record_id_(0),
      record_number_(0),
      offset_table_(nullptr),
      neighbor_list_(nullptr) {
  // TODO: Load segment from disk
}

ANNGraphSegment::ANNGraphSegment(size_t size_limit)
    : synced_with_disk_(false),
      first_record_id_(0),
      record_number_(0),
      offset_table_(nullptr),
      neighbor_list_(nullptr) {
  // TODO: Create an in-memory segment
}

void ANNGraphSegment::BuildFromVectorTable(float* vector_table, size_t n, size_t dim) {
  record_number_ = n;

  // Build a KNN graph using NN descent.
  const int64_t k = Default_NSG_Config.knng;
  std::cout << "KNN" << std::endl;
  vectordb::engine::index::Graph knng(n);
  std::cout << "KNN graph" << std::endl;
  vectordb::engine::index::KNNGraph graph(n, dim, k, vector_table, knng);
  std::cout << "KNN graph finish" << std::endl;

  vectordb::engine::index::BuildParams b_params;
  b_params.candidate_pool_size = Default_NSG_Config.candidate_pool_size;
  b_params.out_degree = Default_NSG_Config.out_degree;
  b_params.search_length = Default_NSG_Config.search_length;

  vectordb::engine::index::NsgIndex::Metric_Type metric = vectordb::engine::index::NsgIndex::Metric_Type::Metric_Type_L2;

  auto index_ = std::make_shared<vectordb::engine::index::NsgIndex>(dim, n, metric);
  index_->SetKnnGraph(knng);
  size_t total_graph_size = index_->Build(n, vector_table, nullptr, b_params);

  // Convert the graph.
  if (offset_table_ != nullptr) {
    delete[] offset_table_;
  }
  offset_table_ = new int64_t[n + 1];  // +1 for the last node.
  if (neighbor_list_ != nullptr) {
    delete[] neighbor_list_;
  }
  neighbor_list_ = new int64_t[total_graph_size];
  int64_t offset = 0;
  for (size_t i = 0; i < n; ++i) {
    offset_table_[i] = offset;
    auto& knn = index_->nsg.at(i);
    for (size_t j = 0; j < knn.size(); ++j) {
      neighbor_list_[offset + j] = knn.at(j);
    }
    offset += knn.size();
  }
  offset_table_[n] = offset;
}

void ANNGraphSegment::Debug() {
  std::cout << "offset_table_:" << std::endl;
  for (size_t i = 0; i < record_number_ + 1; ++i) {
    std::cout << offset_table_[i] << " ";
  }
  std::cout << std::endl;
  std::cout << "neighbor_list_:" << std::endl;
  for (size_t i = 0; i < offset_table_[record_number_]; ++i) {
    std::cout << neighbor_list_[i] << " ";
  }
  std::cout << std::endl;
}

ANNGraphSegment::~ANNGraphSegment() {
  if (offset_table_ != nullptr) {
    delete[] offset_table_;
  }
  if (neighbor_list_ != nullptr) {
    delete[] neighbor_list_;
  }
}

}  // namespace engine
}  // namespace vectordb
