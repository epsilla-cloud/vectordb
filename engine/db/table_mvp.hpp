#pragma once

#include <atomic>
#include <string>
#include <unordered_map>

#include "db/ann_graph_segment.hpp"
#include "db/catalog/meta.hpp"
#include "db/execution/vec_search_executor.hpp"
#include "db/table_segment_mvp.hpp"
#include "utils/atomic_counter.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/status.hpp"
#include "db/index/space_l2.hpp"
#include "db/wal/write_ahead_log.hpp"

namespace vectordb {
namespace engine {

class TableMVP {
 public:
  explicit TableMVP(meta::TableSchema& table_schema, const std::string& db_catalog_path, int64_t init_table_scale /*, int64_t executors_num*/);

  // Rebuild the table and ann graph, and save to disk.
  Status Rebuild(const std::string& db_catalog_path);

  Status Insert(vectordb::Json& records);

  Status Search(
    const std::string& field_name,
    std::vector<std::string>& query_fields, 
    int64_t query_dimension,
    const float* query_data, 
    const int64_t K, 
    vectordb::Json& result
  );

  ~TableMVP();

 public:
  std::string db_catalog_path_;
  // The table schema.
  meta::TableSchema table_schema_;
  // Map from field name to field type.
  std::unordered_map<std::string, meta::FieldType> field_name_type_map_;
  // int64_t executors_num_;
  std::shared_ptr<TableSegmentMVP> table_segment_;  // The table segment loaded/synced from disk.
  // TODO: make this multi threading for higher throughput.
  std::vector<std::shared_ptr<execution::VecSearchExecutor>> executor_;  // The executor for vector search.
  std::vector<std::shared_ptr<ANNGraphSegment>> ann_graph_segment_;      // The ann graph segment for each vector field.
  std::vector<std::shared_ptr<vectordb::L2Space>> l2space_;              // The l2 space for each vector field.

  // One write ahead log per table.
  std::shared_ptr<WriteAheadLog> wal_;
};

}  // namespace engine
}  // namespace vectordb
