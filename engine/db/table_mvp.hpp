#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/ann_graph_segment.hpp"
#include "db/catalog/meta.hpp"
#include "db/execution/executor_pool.hpp"
#include "db/execution/vec_search_executor.hpp"
#include "db/index/space_cosine.hpp"
#include "db/index/space_ip.hpp"
#include "db/index/space_l2.hpp"
#include "db/sparse_vector.hpp"
#include "db/table_segment_mvp.hpp"
#include "db/wal/write_ahead_log.hpp"
#include "utils/atomic_counter.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/concurrent_vector.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

constexpr const int IntraQueryThreads = 4;
constexpr const int MasterQueueSize = 500;
constexpr const int LocalQueueSize = 500;
constexpr const int GlobalSyncInterval = 15;
constexpr const int MinimalGraphSize = 100;
constexpr const int NumExecutorPerField = 16;

constexpr const int RebuildThreads = 4;

class TableMVP {
 public:
  explicit TableMVP(meta::TableSchema &table_schema, const std::string &db_catalog_path, int64_t init_table_scale, bool is_leader /*, int64_t executors_num*/);

  // Rebuild the table and ann graph, and save to disk.
  Status Rebuild(const std::string &db_catalog_path);

  Status Insert(vectordb::Json &records);

  Status InsertPrepare(vectordb::Json &pks, vectordb::Json &result);

  Status Delete(
      vectordb::Json &records,
      const std::string &filter,
      std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes);

  Status Search(
      const std::string &field_name,
      std::vector<std::string> &query_fields,
      int64_t query_dimension,
      const QueryData query_data,
      const int64_t limit,
      vectordb::Json &result,
      std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
      bool with_distance);

  Status SearchByAttribute(
      std::vector<std::string> &query_fields,
      vectordb::Json &primary_keys,
      std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
      const int64_t skip,
      const int64_t limit,
      vectordb::Json &result);

  Status Project(
      std::vector<std::string> &query_fields,
      int64_t idlist_size,        // -1 means project all.
      std::vector<int64_t> &ids,  // doesn't matter if idlist_size is -1.
      vectordb::Json &result,
      bool with_distance,
      std::vector<double> &distances);

  void SetWALEnabled(bool enabled) {
    wal_->SetEnabled(enabled);
  }

  void SetLeader(bool is_leader);

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

  ThreadSafeVector<std::shared_ptr<execution::ExecutorPool>> executor_pool_;  // The executor for vector search.
  std::mutex executor_pool_mutex_;
  std::vector<std::shared_ptr<ANNGraphSegment>> ann_graph_segment_;      // The ann graph segment for each vector field.
  std::vector<std::shared_ptr<vectordb::SpaceInterface<float>>> space_;  // The space for each vector field.

  // One write ahead log per table.
  std::shared_ptr<WriteAheadLog> wal_;

  // If the segment is leader (handle sync to storage) or follower (passively sync from storage)
  std::atomic<bool> is_leader_;
};

}  // namespace engine
}  // namespace vectordb
