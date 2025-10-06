#include "db/ann_graph_segment.hpp"

#include <unistd.h>

#include <cstdio>
#include <iostream>

#include "db/index/knn/knn.hpp"
#include "db/index/nsg/nsg.hpp"
#include "utils/common_util.hpp"
#include "config/config.hpp"

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

// Fallback default (not used when globalConfig is available)
const NSGConfig Default_NSG_Config(45, 50, 300, 100);

ANNGraphSegment::ANNGraphSegment(bool skip_sync_disk)
    : skip_sync_disk_(skip_sync_disk),
      first_record_id_(0),
      record_number_(0),
      offset_table_(nullptr),
      neighbor_list_(nullptr),
      navigation_point_(0) {}

ANNGraphSegment::ANNGraphSegment(const std::string& db_catalog_path, int64_t table_id, int64_t field_id)
    : skip_sync_disk_(false),
      first_record_id_(0),
      record_number_(0),
      offset_table_(nullptr),
      neighbor_list_(nullptr),
      navigation_point_(0) {
  // Construct the file path
  std::string file_path = db_catalog_path + "/" + std::to_string(table_id) + "/ann_graph_" + std::to_string(field_id) + ".bin";

  if (server::CommonUtil::IsFileExist(file_path)) {
    // Open the file
    std::ifstream file(file_path, std::ios::binary);
    if (!file) {
      throw std::runtime_error("Cannot open file: " + file_path);
    }

    // Read the number of records
    file.read(reinterpret_cast<char*>(&record_number_), sizeof(record_number_));

    // Read the starting id of the first record
    file.read(reinterpret_cast<char*>(&first_record_id_), sizeof(first_record_id_));

    // Allocate memory for the offset array
    offset_table_ = std::make_unique<int64_t[]>(record_number_ + 1);

    // Read the offset array
    file.read(reinterpret_cast<char*>(offset_table_.get()), sizeof(int64_t) * (record_number_ + 1));

    // Get the total number of edges from the last element of offset_table_
    int64_t total_edges = offset_table_[record_number_];

    // Allocate memory for the neighbor list array with the correct size
    neighbor_list_ = std::make_unique<int64_t[]>(total_edges);

    // Read the neighbor list array
    file.read(reinterpret_cast<char*>(neighbor_list_.get()), sizeof(int64_t) * total_edges);

    // Read the nagivation point
    file.read(reinterpret_cast<char*>(&navigation_point_), sizeof(navigation_point_));

    // Close the file
    file.close();
  } else {
    // Create directory with an empty ann graph.
    std::string folder_path = db_catalog_path + "/" + std::to_string(table_id);
    auto mkdir_status = server::CommonUtil::CreateDirectory(folder_path);
    if (!mkdir_status.ok()) {
      throw mkdir_status.message();
    }
    offset_table_ = std::make_unique<int64_t[]>(record_number_ + 1);
    offset_table_[record_number_] = 0;
    neighbor_list_ = std::make_unique<int64_t[]>(record_number_);
    auto status = SaveANNGraph(db_catalog_path, table_id, field_id);
    if (!status.ok()) {
      throw status.message();
    }
  }
}

ANNGraphSegment::ANNGraphSegment(int64_t size_limit)
    : skip_sync_disk_(true),
      first_record_id_(0),
      record_number_(0),
      offset_table_(nullptr),
      neighbor_list_(nullptr),
      navigation_point_(0) {
  // TODO: Create an in-memory segment
}

// Status ANNGraphSegment::SaveANNGraph(const std::string& db_catalog_path, int64_t table_id, int64_t field_id) {
//   if (skip_sync_disk_) {
//     return Status::OK();
//   }

//   // Construct the file path
//   std::string path = db_catalog_path + "/" + std::to_string(table_id) + "/ann_graph_" + std::to_string(field_id) + ".bin";
//   std::string tmp_path = path + ".tmp";

//   std::ofstream file(tmp_path, std::ios::binary);
//   if (!file) {
//     return Status(DB_UNEXPECTED_ERROR, "Cannot open file: " + path);
//   }

//   // Write the number of records and the first record id
//   file.write(reinterpret_cast<const char*>(&record_number_), sizeof(record_number_));
//   file.write(reinterpret_cast<const char*>(&first_record_id_), sizeof(first_record_id_));

//   // Write the offset table
//   file.write(reinterpret_cast<const char*>(offset_table_), sizeof(int64_t) * (record_number_ + 1));

//   // Get the total number of edges from the last element of offset_table_
//   int64_t total_edges = offset_table_[record_number_];

//   // Write the neighbor list
//   file.write(reinterpret_cast<const char*>(neighbor_list_), sizeof(int64_t) * total_edges);

//   // Write the navigation point
//   file.write(reinterpret_cast<const char*>(&navigation_point_), sizeof(navigation_point_));

//   // Close the file
//   fsync(fileno(file));
//   file.close();

//   if (!file) {
//     return Status(DB_UNEXPECTED_ERROR, "Failed to write to file: " + path);
//   }

//   if (std::rename(tmp_path.c_str(), path.c_str()) != 0) {
//     // LOG_SERVER_ERROR_ << "Failed to rename temp file: " << temp_path << " to " << path;
//     return Status(INFRA_UNEXPECTED_ERROR, "Failed to rename temp file: " + tmp_path + " to " + path);
//   }

//   return Status::OK();
// }

Status ANNGraphSegment::SaveANNGraph(const std::string& db_catalog_path, int64_t table_id, int64_t field_id, bool force) {
  if (skip_sync_disk_ && !force) {
    return Status::OK();
  }

  // Construct the file path
  std::string path = db_catalog_path + "/" + std::to_string(table_id) + "/ann_graph_" + std::to_string(field_id) + ".bin";
  std::string tmp_path = path + ".tmp";

  FILE* file = fopen(tmp_path.c_str(), "wb");
  if (!file) {
    return Status(DB_UNEXPECTED_ERROR, "Cannot open file: " + path);
  }

  // Write the number of records and the first record id
  fwrite(&record_number_, sizeof(record_number_), 1, file);
  fwrite(&first_record_id_, sizeof(first_record_id_), 1, file);

  // Write the offset table
  fwrite(offset_table_.get(), sizeof(int64_t), record_number_ + 1, file);

  // Get the total number of edges from the last element of offset_table_
  int64_t total_edges = offset_table_[record_number_];

  // Write the neighbor list
  fwrite(neighbor_list_.get(), sizeof(int64_t), total_edges, file);

  // Write the navigation point
  fwrite(&navigation_point_, sizeof(navigation_point_), 1, file);

  // Flush changes to disk
  fflush(file);
  fsync(fileno(file));

  // Close the file
  fclose(file);

  if (std::rename(tmp_path.c_str(), path.c_str()) != 0) {
    // LOG_SERVER_ERROR_ << "Failed to rename temp file: " << temp_path << " to " << path;
    return Status(INFRA_UNEXPECTED_ERROR, "Failed to rename temp file: " + tmp_path + " to " + path);
  }

  return Status::OK();
}

void ANNGraphSegment::BuildFromVectorTable(VectorColumnData vector_column, int64_t n, int64_t dim, meta::MetricType metricType) {
  record_number_ = n;

  // Build a KNN graph using NN descent.
  const int64_t k = vectordb::globalConfig.NSGKnng.load(std::memory_order_acquire);
  logger_.Debug("KNN graph building start");
  vectordb::engine::index::Graph knng(n);
  vectordb::engine::index::KNNGraph graph(n, dim, k, vector_column, knng, metricType);
  logger_.Debug("KNN graph building finish");

  // Use adaptive search_length based on dataset size
  const int adaptive_search_length = vectordb::globalConfig.getAdaptiveSearchLength(n);
  const int out_degree = vectordb::globalConfig.NSGOutDegree.load(std::memory_order_acquire);
  const int candidate_pool_size = vectordb::globalConfig.NSGCandidatePoolSize.load(std::memory_order_acquire);

  logger_.Info("NSG Build Config: dataset_size=" + std::to_string(n) +
               ", search_length=" + std::to_string(adaptive_search_length) +
               " (adaptive), out_degree=" + std::to_string(out_degree) +
               ", candidate_pool_size=" + std::to_string(candidate_pool_size));

  vectordb::engine::index::BuildParams b_params;
  b_params.candidate_pool_size = candidate_pool_size;
  b_params.out_degree = out_degree;
  b_params.search_length = adaptive_search_length;

  vectordb::engine::index::NsgIndex::Metric_Type metric = vectordb::engine::index::NsgIndex::Metric_Type::Metric_Type_L2;

  auto index_ = std::make_shared<vectordb::engine::index::NsgIndex>(dim, n, metric);
  index_->SetKnnGraph(knng);
  int64_t total_graph_size = index_->Build(n, vector_column, nullptr, b_params);

  // Convert the graph.
  // Smart pointers will automatically clean up old memory
  offset_table_ = std::make_unique<int64_t[]>(n + 1);  // +1 for the last node.
  neighbor_list_ = std::make_unique<int64_t[]>(total_graph_size);
  int64_t offset = 0;
  for (int64_t i = 0; i < n; ++i) {
    offset_table_[i] = offset;
    auto& knn = index_->nsg.at(i);
    for (int64_t j = 0; j < knn.size(); ++j) {
      neighbor_list_[offset + j] = knn.at(j);
    }
    offset += knn.size();
  }
  offset_table_[n] = offset;
  navigation_point_ = index_->navigation_point;
}

void ANNGraphSegment::Debug() {
  std::cout << "offset_table_:" << std::endl;
  for (int64_t i = 0; i < record_number_ + 1; ++i) {
    std::cout << offset_table_[i] << " ";
  }
  std::cout << std::endl;
  std::cout << "neighbor_list_:" << std::endl;
  for (int64_t i = 0; i < offset_table_[record_number_]; ++i) {
    std::cout << neighbor_list_[i] << " ";
  }
  std::cout << std::endl;
  std::cout << "navigation_point_:" << std::endl;
  std::cout << navigation_point_ << std::endl;
}

ANNGraphSegment::~ANNGraphSegment() {
  // Smart pointers automatically clean up, no manual deletion needed
}

}  // namespace engine
}  // namespace vectordb
