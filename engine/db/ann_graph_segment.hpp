#pragma once

#include <atomic>
#include <string>
#include <unordered_map>

#include "db/catalog/meta.hpp"
#include "db/vector.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

using VectorColumnData = std::variant<
    DenseVectorColumnDataContainer,
    // pass pointer here to avoid unnecessary deep copy
    VariableLenAttrColumnContainer*>;

class ANNGraphSegment {
 public:
  // Default constructor just for table level init. Used by default table.
  explicit ANNGraphSegment(bool skip_disk_sync);
  // Load segment from disk.
  explicit ANNGraphSegment(const std::string& db_catalog_path, int64_t table_id, int64_t field_id);
  // Create an in-memory segment.
  explicit ANNGraphSegment(int64_t size_limit);

  // Build the ANN graph from vector table.
  void BuildFromVectorTable(VectorColumnData vector_column, int64_t n, int64_t dim, meta::MetricType metricType);

  void Debug();

  // Save the ANN graph index to disk.
  Status SaveANNGraph(const std::string& db_catalog_path, int64_t table_id, int64_t field_id, bool force = false);

  ~ANNGraphSegment();

 public:
  vectordb::engine::Logger logger_;
  bool skip_sync_disk_;  // For default DB, skip sync to disk.
  // bool synced_with_disk_;              // Whether the table segment is synced with disk.
  int64_t first_record_id_;             // The internal record id (node id) of the first record in the segment.
  std::atomic<int64_t> record_number_;  // Currently how many records (nodes) in the segment.
  std::unique_ptr<int64_t[]> offset_table_;   // The offset table for neighbor list for each node.
  std::unique_ptr<int64_t[]> neighbor_list_;  // The neighbor list for each node consecutively stored.
  int64_t navigation_point_;            // The navigation point for the starting search.
  // TODO: Will support these in the future when we support dynamic update NSG index.
  // ConcurrentBitset updated_;                                              // The updated bitset. If the i-th bit is 1, then the i-th record's neighbor list is updated.
  //                                                                         // In this case, query should consult the hashmap instead of the neighbor list for getting neighbors.
  // ConcurrentHashMap<size_t, std::vector<size_t>> updated_neighbor_list_;  // The updated neighbor lists. Only the nodes with updated bit set to 1
  //                                                                         // will have updated neighbor list.
};

}  // namespace engine
}  // namespace vectordb

/**
 *

  int x = 1000, y = 1000;
  int n = x * y;
  int dim = 128;
  float* data = new float[n * dim];
  for (int i = 0; i < x; i++) {
    for (int j = 0; j < y; ++j) {
      for (int p = 0; p < dim; ++p) {
        data[(i * y + j) * dim + p] = rand();
      }
      // int k = i * y + j;
      // data[k * dim] = i;
      // data[k * dim + 1] = j;
      // for (int p = 2; p < dim; ++p) {
      //   data[k * dim + p] = 0;
      // }
    }
  }
  std::cout << "here" << std::endl;
  // vectordb::engine::index::Graph knng(n);
  // vectordb::engine::index::KNNGraph graph(n, dim, 100, data, knng);

  auto ann_graph_segment = std::make_shared<vectordb::engine::ANNGraphSegment>();
  ann_graph_segment->BuildFromVectorTable(data, n, dim);
  ann_graph_segment->Debug();
*/
