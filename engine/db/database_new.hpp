#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>
#include <shared_mutex>

#include "db/catalog/meta.hpp"
#include "db/table_new.hpp"
#include "utils/status.hpp"
#include "utils/concurrent_map.hpp"
#include "services/embedding_service.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

/**
 * @brief Database class - manages tables within a database instance
 * 
 * This class represents a database container that manages multiple tables.
 * It handles WAL coordination, supports leader/follower replication,
 * and provides table lifecycle management.
 * 
 * Renamed from DBMVP to follow standard naming conventions.
 */
class Database {
 public:
  explicit Database(
    meta::DatabaseSchema& database_schema,
    int64_t init_table_scale,
    bool is_leader,
    std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service,
    std::unordered_map<std::string, std::string> &headers);

  ~Database() {}

  Status CreateTable(meta::TableSchema& table_schema);

  Status DropTable(const std::string& table_name);

  std::shared_ptr<Table> GetTable(const std::string& table_name);

  Status ListTables(std::vector<std::string>& table_names);

  Status Release();

  Status Dump(const std::string& db_catalog_path);

  void SetWALEnabled(bool enabled);

  void SetIsLeader(bool is_leader);

  size_t GetRecordCount();
  size_t GetCapacity();

  std::string GetDatabaseName() const {
    return database_schema_.name_;
  }

  std::string GetDatabasePath() const {
    return database_schema_.path_;
  }

  // Statistics and monitoring
  Status GetStatistics(vectordb::Json& stats);

 private:
  meta::DatabaseSchema database_schema_;
  int64_t init_table_scale_;
  bool is_leader_;
  bool wal_enabled_;
  
  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service_;
  std::unordered_map<std::string, std::string> headers_;
  
  // Thread-safe table management
  vectordb::ConcurrentMap<std::string, std::shared_ptr<Table>> tables_;
  std::shared_mutex tables_mutex_;
  
  vectordb::engine::Logger logger_;
  
  // Atomic counters for statistics
  std::atomic<size_t> total_record_count_{0};
  std::atomic<size_t> total_capacity_{0};
};

}  // namespace engine
}  // namespace vectordb