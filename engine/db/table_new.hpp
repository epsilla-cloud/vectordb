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
#include "db/table_segment_new.hpp"
#include "db/vector.hpp"
#include "db/wal/write_ahead_log.hpp"
#include "db/incremental_compactor.hpp"
#include "utils/atomic_counter.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/concurrent_vector.hpp"
#include "utils/status.hpp"
#include "services/embedding_service.hpp"
#include "logger/logger.hpp"
#include "db/execution/aggregation.hpp"

namespace vectordb {
namespace engine {

/**
 * @brief Table class - manages table-level operations and data storage
 * 
 * This class handles table-level operations including vector search execution,
 * ANN graph segment management, and data operations (insert, search, delete, get).
 * It manages both vector and attribute fields with concurrent access support.
 * 
 * Key features:
 * - Vector search executor pool with configurable threads
 * - ANN graph segment management for efficient vector search
 * - Write-Ahead Logging (WAL) for durability
 * - Compaction and rebuilding capabilities
 * - Concurrent operations with thread-safety
 * 
 * Renamed from TableMVP to follow standard naming conventions.
 */
class Table {
 public:
  explicit Table(
    meta::TableSchema &table_schema,
    const std::string &db_catalog_path,
    int64_t init_table_scale,
    bool is_leader,
    std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service,
    std::unordered_map<std::string, std::string> &headers);

  ~Table() = default;

  // Core table operations
  Status Release();
  Status Dump(const std::string &db_catalog_path);

  // Data operations
  Status Insert(vectordb::Json &records, 
               std::unordered_map<std::string, std::string> &headers, 
               bool upsert = false);

  Status InsertPrepare(vectordb::Json &pks, vectordb::Json &result);

  Status Delete(vectordb::Json &records,
               const std::string &filter,
               std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes);

  Status Search(const std::string &field_name,
               std::vector<std::string> &query_fields,
               int64_t query_dimension,
               const VectorPtr query_data,
               const int64_t limit,
               vectordb::Json &result,
               std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
               bool with_distance,
               std::vector<vectordb::engine::execution::FacetExecutor> &facet_executors,
               vectordb::Json &facets);

  Status SearchByAttribute(std::vector<std::string> &query_fields,
                          vectordb::Json &primary_keys,
                          std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
                          const int64_t skip,
                          const int64_t limit,
                          vectordb::Json &projects,
                          std::vector<vectordb::engine::execution::FacetExecutor> &facet_executors,
                          vectordb::Json &facets);

  Status Project(std::vector<std::string> &query_fields,
                int64_t idlist_size,
                std::vector<int64_t> &ids,
                vectordb::Json &result,
                bool with_distance,
                std::vector<double> &distances);

  // Index management
  Status Rebuild(const std::string &db_catalog_path);
  Status Compact(double threshold = 0.3);
  bool NeedsCompaction(double threshold = 0.3) const;

  // Configuration management  
  Status SwapExecutors();
  void SetWALEnabled(bool enabled);
  void SetIsLeader(bool is_leader);

  // Statistics
  size_t GetRecordCount();
  size_t GetCapacity();
  Status GetStatistics(vectordb::Json& stats);

  // Table metadata
  const meta::TableSchema& GetSchema() const { return table_schema_; }
  std::string GetTableName() const { return table_schema_.name_; }
  int64_t GetTableId() const { return table_schema_.id_; }

 private:
  meta::TableSchema table_schema_;
  std::string db_catalog_path_;
  int64_t init_table_scale_;
  bool is_leader_;
  
  // Core components
  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service_;
  std::unordered_map<std::string, std::string> headers_;
  
  // Storage and indexing
  std::vector<std::shared_ptr<TableSegment>> segments_;
  std::shared_ptr<WriteAheadLog> wal_;
  std::shared_ptr<IncrementalCompactor> compactor_;
  
  // Execution
  std::shared_ptr<execution::ExecutorPool> executor_pool_;
  
  // Thread safety
  mutable std::shared_mutex segments_mutex_;
  
  // Statistics
  vectordb::AtomicCounter record_counter_;
  std::atomic<size_t> capacity_{0};
  
  vectordb::engine::Logger logger_;
  
  // Helper methods
  Status InitializeExecutors();
  Status LoadExistingSegments();
  Status CreateNewSegment();
  std::shared_ptr<TableSegment> GetActiveSegment();
};

}  // namespace engine
}  // namespace vectordb