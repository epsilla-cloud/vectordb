#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <shared_mutex>

#include "db/catalog/meta.hpp"
#include "db/database.hpp"
#include "db/table.hpp"
#include "db/vector.hpp"
#include "query/expr/expr.hpp"
#include "utils/status.hpp"
#include "utils/concurrent_map.hpp"
#include "services/embedding_service.hpp"
#include "logger/logger.hpp"
#include "config/config.hpp"
#include "server/fulltext/fulltext_engine.hpp"
#include "server/fulltext/fulltext_manager.hpp"

namespace vectordb {
namespace engine {

class DBServer {
 public:
  DBServer();

  ~DBServer();
  
  // WAL flush management
  void StartWALFlushThread();
  void StopWALFlushThread();
  Status FlushAllWAL();

  // Monitoring and statistics
  void StartMonitoringThread();
  void StopMonitoringThread();
  void PrintMonitoringStats();
  
  // WAL flush statistics
  struct WALFlushStats {
    uint64_t total_flushes;
    uint64_t successful_flushes;
    uint64_t failed_flushes;
    uint64_t last_flush_time;
    uint64_t total_flush_duration_ms;
  };

  // CRITICAL FIX: Add mutex to protect consistent snapshot reads
  WALFlushStats GetWALFlushStats() const {
    // Lock to ensure consistent snapshot across all statistics
    // Without this lock, we might read inconsistent state during concurrent updates
    std::lock_guard<std::mutex> lock(wal_stats_mutex_);

    WALFlushStats stats;
    stats.total_flushes = wal_flush_stats_total_flushes_.load(std::memory_order_relaxed);
    stats.successful_flushes = wal_flush_stats_successful_flushes_.load(std::memory_order_relaxed);
    stats.failed_flushes = wal_flush_stats_failed_flushes_.load(std::memory_order_relaxed);
    stats.last_flush_time = wal_flush_stats_last_flush_time_.load(std::memory_order_relaxed);
    stats.total_flush_duration_ms = wal_flush_stats_total_duration_ms_.load(std::memory_order_relaxed);
    return stats;
  }

  Status LoadDB(const std::string& db_name, const std::string& db_catalog_path, int64_t init_table_scale, bool wal_enabled, std::unordered_map<std::string, std::string> &headers);
  Status UnloadDB(const std::string& db_name);
  Status ReleaseDB(const std::string& db_name);
  Status DumpDB(const std::string& db_name, const std::string& db_catalog_path);
  Status GetStatistics(const std::string& db_name, vectordb::Json& response);
  Status CreateTable(const std::string& db_name, meta::TableSchema& table_schema, size_t& table_id);
  Status CreateTable(const std::string& db_name, const std::string& table_schema_json, size_t& table_id);
  Status DropTable(const std::string& db_name, const std::string& table_name);
  std::shared_ptr<Database> GetDB(const std::string& db_name);
  Status ListTables(const std::string& db_name, std::vector<std::string>& table_names);
  Status Insert(const std::string& db_name, const std::string& table_name, vectordb::Json& records, std::unordered_map<std::string, std::string> &headers, bool upsert = false);
  Status InsertPrepare(const std::string& db_name, const std::string& table_name, vectordb::Json& pks, vectordb::Json& result);
  Status Delete(
      const std::string& db_name,
      const std::string& table_name,
      vectordb::Json& pkList,
      const std::string& filter);
  Status Search(
      const std::string& db_name,
      const std::string& table_name,
      std::string& field_name,
      std::vector<std::string>& query_fields,
      int64_t query_dimension,
      const VectorPtr query_data,
      const int64_t limit,
      vectordb::Json& result,
      const std::string& filter,
      bool with_distance,
      vectordb::Json& facets_config,
      vectordb::Json& facets);
  Status SearchByContent(
      const std::string& db_name,
      const std::string& table_name,
      std::string& index_name,
      std::vector<std::string>& query_fields,
      std::string& query,
      const int64_t limit,
      vectordb::Json& result,
      const std::string& filter,
      bool with_distance,
      vectordb::Json& facets_config,
      vectordb::Json& facets,
      std::unordered_map<std::string, std::string> &headers);

  Status Project(
      const std::string& db_name,
      const std::string& table_name,
      std::vector<std::string>& query_fields,
      vectordb::Json& primary_keys,
      const std::string& filter,
      const int64_t skip,
      const int64_t limit,
      vectordb::Json& result,
      vectordb::Json& facets_config,
      vectordb::Json& facets);

  void StartRebuild() {
    if (rebuild_started_) {
      return;
    }
    rebuild_started_ = true;
    // Start the thread to periodically call Rebuild
    rebuild_thread_ = std::thread(&DBServer::RebuildPeriodically, this);
  }

  Status RebuildOndemand() {
    if (rebuild_started_) {
      return Status(DB_UNEXPECTED_ERROR, "Auto rebuild is enabled. Cannot conduct on-demand rebuild.");
    }
    rebuild_started_ = true;
    Rebuild();
    rebuild_started_ = false;
    return Status::OK();
  }

  void SetLeader(bool is_leader) {
    is_leader_ = is_leader;
    meta_->SetLeader(is_leader_);
    
    // Use copy of vector to avoid holding lock during DB operations
    std::vector<std::shared_ptr<Database>> dbs_copy;
    {
      std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
      dbs_copy = dbs_;  // Copy the vector of shared_ptrs
    }
    
    // Now iterate without holding the lock
    for (auto db : dbs_copy) {
      if (db) {
        db->SetLeader(is_leader);
      }
    }
  }

  Status Rebuild();

  Status Compact(const std::string& db_name = "", const std::string& table_name = "", double threshold = 0.3);

  Status SwapExecutors();
  
  Status GetRecordCount(const std::string& db_name, 
                       const std::string& table_name,
                       vectordb::Json& result);

  void InjectEmbeddingService(std::string& embedding_service_url) {
    embedding_service_ = std::make_shared<vectordb::engine::EmbeddingService>(embedding_service_url);
    meta_->InjectEmbeddingService(embedding_service_);
  }

 private:
  vectordb::engine::Logger logger_;
  std::shared_ptr<meta::Meta> meta_;  // The db meta.
  // Thread-safe map for db name to index mapping
  utils::ConcurrentUnorderedMap<std::string, size_t> db_name_to_id_map_;  // The db name to db index map.

  // Protect dbs_ vector with shared_mutex
  mutable std::shared_mutex dbs_mutex_;
  std::vector<std::shared_ptr<Database>> dbs_;                    // The dbs.
  std::thread rebuild_thread_;
  bool stop_rebuild_thread_ = false;
  bool rebuild_started_ = false;
  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service_;

  // Full-text search engine integration
  std::unique_ptr<vectordb::server::fulltext::FullTextEngine> fulltext_engine_;

  // WAL flush thread management
  std::thread wal_flush_thread_;
  std::atomic<bool> stop_wal_flush_thread_{false};
  std::atomic<bool> wal_flush_thread_started_{false};

  // WAL flush statistics (using atomic for thread safety)
  std::atomic<uint64_t> wal_flush_stats_total_flushes_{0};
  std::atomic<uint64_t> wal_flush_stats_successful_flushes_{0};
  std::atomic<uint64_t> wal_flush_stats_failed_flushes_{0};
  std::atomic<uint64_t> wal_flush_stats_last_flush_time_{0};
  std::atomic<uint64_t> wal_flush_stats_total_duration_ms_{0};

  // CRITICAL FIX: Mutex to protect consistent snapshot reads of WAL stats
  mutable std::mutex wal_stats_mutex_;

  // Monitoring thread management
  std::thread monitoring_thread_;
  std::atomic<bool> stop_monitoring_thread_{false};
  std::atomic<bool> monitoring_thread_started_{false};

  // Worker pool initialization
  void InitializeWorkerPools();
  void ShutdownWorkerPools();

  // Private WAL flush worker
  void WALFlushWorker();

  // Private monitoring worker
  void MonitoringWorker();

  // periodically in a separate thread
  void RebuildPeriodically() {
    while (!stop_rebuild_thread_) {
      Rebuild();  // Call the Rebuild function

      // Get rebuild interval from global config (allows runtime configuration)
      int interval_ms = vectordb::globalConfig.RebuildInterval.load(std::memory_order_acquire);
      const std::chrono::milliseconds rebuild_interval(interval_ms);
      
      // Introduce the time delay before the next Rebuild
      std::this_thread::sleep_for(rebuild_interval);
    }
  };

  std::atomic<bool> is_leader_;
};

}  // namespace engine
}  // namespace vectordb
