#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>
#include <shared_mutex>

#include "db/catalog/meta.hpp"
#include "db/table.hpp"
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
  Status DeleteTable(const std::string& table_name);
  std::vector<std::string> GetTables();
  std::shared_ptr<Table> GetTable(const std::string& table_name);
  Status Rebuild();
  Status Compact(const std::string& table_name = "", double threshold = 0.3);
  Status SwapExecutors();
  Status Release();
  Status Dump(const std::string& db_catalog_path);
  Status GetRecordCount(const std::string& table_name, vectordb::Json& result);
  std::string GetName() const { 
    // Extract database name from catalog path
    size_t last_slash = db_catalog_path_.find_last_of('/');
    if (last_slash != std::string::npos) {
      return db_catalog_path_.substr(last_slash + 1);
    }
    return db_catalog_path_;
  }

  void SetWALEnabled(bool enabled) {
    // Use copy of vector to avoid holding lock during table operations
    std::vector<std::shared_ptr<Table>> tables_copy;
    {
      std::shared_lock<std::shared_mutex> lock(tables_mutex_);
      tables_copy = tables_;  // Copy the vector of shared_ptrs
    }
    
    // Now iterate without holding the lock
    for (auto table : tables_copy) {
      if (table) {
        table->SetWALEnabled(enabled);
      }
    }
  }

  void SetLeader(bool is_leader) {
    is_leader_ = is_leader;
    
    // Use copy of vector to avoid holding lock during table operations
    std::vector<std::shared_ptr<Table>> tables_copy;
    {
      std::shared_lock<std::shared_mutex> lock(tables_mutex_);
      tables_copy = tables_;  // Copy the vector of shared_ptrs
    }
    
    // Now iterate without holding the lock
    for (auto table : tables_copy) {
      if (table) {
        table->SetLeader(is_leader);
      }
    }
  }

 public:
  vectordb::engine::Logger logger_;
  std::string db_catalog_path_;                                   // The path to the db catalog.
  // Thread-safe map for table name to id mapping
  utils::ConcurrentUnorderedMap<std::string, size_t> table_name_to_id_map_;  // The table name to table id map.
  
  // Protect tables_ vector with shared_mutex
  mutable std::shared_mutex tables_mutex_;
  std::vector<std::shared_ptr<Table>> tables_;                    // The tables in this database.
  int64_t init_table_scale_;
  std::atomic<bool> is_leader_;
  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service_;
};

using DatabasePtr = std::shared_ptr<Database>;

// Backward compatibility typedef
using DBMVP = Database;
using DBMVPPtr = DatabasePtr;

}  // namespace engine
}  // namespace vectordb