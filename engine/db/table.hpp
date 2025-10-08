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
#include "db/table_segment.hpp"
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
    std::unordered_map<std::string, std::string> &headers /*, int64_t executors_num*/);

  // Rebuild the table and ann graph, and save to disk.
  Status Rebuild(const std::string &db_catalog_path);
  
  // Smart rebuild: intelligently choose between incremental and full rebuild
  // This is the main entry point for rebuild operations
  Status SmartRebuild(const std::string &db_catalog_path);

  // Compact the table by removing deleted records
  Status Compact(double threshold = 0.3);

  // Check if any segment needs compaction
  bool NeedsCompaction(double threshold = 0.3) const;

 private:
  // === Rebuild strategy methods ===
  
  // Check if rebuild should be skipped
  bool ShouldSkipRebuild(int64_t old_count, int64_t new_count, double deleted_ratio);
  
  // Perform incremental rebuild (only process new nodes)
  Status IncrementalRebuild(const std::string &db_catalog_path);
  
  // Perform full rebuild (rebuild entire graph)
  Status FullRebuild(const std::string &db_catalog_path);
  
  // Check if disk save is needed
  bool ShouldSaveToDisk();
  
  // === Rebuild state tracking ===
  std::atomic<int> incremental_rebuild_count_{0};  // Consecutive incremental rebuilds
  std::atomic<int> total_rebuild_count_{0};        // Total rebuilds since start
  std::chrono::steady_clock::time_point last_save_time_{std::chrono::steady_clock::now()};
  std::mutex rebuild_compact_mutex_;               // Protect rebuild and compact operations
  
 public:

  // Swap executors during config change.
  Status SwapExecutors();

  Status Release();

  Status Insert(vectordb::Json &records, std::unordered_map<std::string, std::string> &headers, bool upsert = false);

  Status InsertPrepare(vectordb::Json &pks, vectordb::Json &result);

  Status Delete(
      vectordb::Json &records,
      const std::string &filter,
      std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes);

  Status Search(
      const std::string &field_name,
      std::vector<std::string> &query_fields,
      int64_t query_dimension,
      const VectorPtr query_data,
      const int64_t limit,
      vectordb::Json &result,
      std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
      bool with_distance,
      std::vector<vectordb::engine::execution::FacetExecutor> &facet_executors,
      vectordb::Json &facets);

  Status SearchByAttribute(
      std::vector<std::string> &query_fields,
      vectordb::Json &primary_keys,
      std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
      const int64_t skip,
      const int64_t limit,
      vectordb::Json &projects,
      std::vector<vectordb::engine::execution::FacetExecutor> &facet_executors,
      vectordb::Json &facets);

  Status Project(
      std::vector<std::string> &query_fields,
      int64_t idlist_size,        // -1 means project all.
      std::vector<int64_t> &ids,  // doesn't matter if idlist_size is -1.
      vectordb::Json &result,
      bool with_distance,
      std::vector<double> &distances);

  Status Dump(const std::string &db_catalog_path);

  size_t GetRecordCount();
  size_t GetCapacity();

  // Get the number of active (non-deleted) vectors
  size_t GetActiveVectorCount() {
    if (!table_segment_) {
      return 0;
    }
    size_t total = table_segment_->GetRecordCount();
    size_t deleted = table_segment_->GetDeletedCount();
    return total - deleted;
  }

  // Get detailed vector statistics
  struct VectorStats {
    size_t total_vectors;      // Total vectors (including deleted)
    size_t active_vectors;     // Active vectors (excluding deleted)
    size_t deleted_vectors;    // Deleted vectors
    double deleted_ratio;      // Ratio of deleted vectors
    size_t capacity;           // Current capacity
  };

  VectorStats GetVectorStats() {
    VectorStats stats{};
    if (!table_segment_) {
      return stats;
    }

    stats.total_vectors = table_segment_->GetRecordCount();
    stats.deleted_vectors = table_segment_->GetDeletedCount();
    stats.active_vectors = stats.total_vectors - stats.deleted_vectors;
    stats.capacity = table_segment_->size_limit_;

    if (stats.total_vectors > 0) {
      stats.deleted_ratio = (double)stats.deleted_vectors / stats.total_vectors;
    } else {
      stats.deleted_ratio = 0.0;
    }

    return stats;
  }

  void SetWALEnabled(bool enabled) {
    wal_->SetEnabled(enabled);
  }

  Status FlushWAL() {
    if (!wal_) {
      return Status::OK();  // No WAL configured
    }
    
    // Force flush WAL to disk
    try {
      // Get current WAL file handle and force fsync
      // This ensures all buffered writes are persisted
      wal_->Flush();
      return Status::OK();
    } catch (const std::exception& e) {
      return Status(DB_UNEXPECTED_ERROR, "Failed to flush WAL: " + std::string(e.what()));
    }
  }

  void SetLeader(bool is_leader);

  ~Table();

 public:
  vectordb::engine::Logger logger_;
  std::string db_catalog_path_;
  // The table schema.
  meta::TableSchema table_schema_;
  // Map from field name to field type.
  std::unordered_map<std::string, meta::FieldType> field_name_field_type_map_;
  // Map from field name to field type.
  std::unordered_map<std::string, meta::MetricType> field_name_metric_type_map_;

  // int64_t executors_num_;
  std::shared_ptr<TableSegment> table_segment_;  // The table segment loaded/synced from disk.
  // TODO: make this multi threading for higher throughput.

  ThreadSafeVector<std::shared_ptr<execution::ExecutorPool>> executor_pool_;  // The executor for vector search.
  std::mutex executor_pool_mutex_;
  std::vector<std::shared_ptr<ANNGraphSegment>> ann_graph_segment_;  // The ann graph segment for each vector field.

  // One write ahead log per table.
  std::shared_ptr<WriteAheadLog> wal_;

  // If the segment is leader (handle sync to storage) or follower (passively sync from storage)
  std::atomic<bool> is_leader_;

  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service_;
  
  // Incremental compactor for background compaction
  std::unique_ptr<IncrementalCompactor> compactor_;
  
  // Enable/disable incremental compaction
  void EnableIncrementalCompaction(const CompactionConfig& config = CompactionConfig());
  void DisableIncrementalCompaction();
};

// Backward compatibility typedef
using TableMVP = Table;

}  // namespace engine
}  // namespace vectordb