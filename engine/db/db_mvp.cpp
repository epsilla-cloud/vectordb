#include "db/db_mvp.hpp"

#include "utils/common_util.hpp"

namespace vectordb {
namespace engine {

DBMVP::DBMVP(
  meta::DatabaseSchema& database_schema,
  int64_t init_table_scale,
  bool is_leader,
  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service,
  std::unordered_map<std::string, std::string> &headers) {
  embedding_service_ = embedding_service;
  is_leader_ = is_leader;
  // Here you might want to initialize your database based on the provided schema
  // This may involve creating tables, loading data, etc.
  init_table_scale_ = init_table_scale;
  db_catalog_path_ = database_schema.path_;
  for (int i = 0; i < database_schema.tables_.size(); ++i) {
    auto table = std::make_shared<TableMVP>(
      database_schema.tables_[i],
      db_catalog_path_,
      init_table_scale_,
      is_leader_,
      embedding_service_,
      headers);
    
    // Thread-safe insertion
    size_t table_index;
    {
      std::unique_lock<std::shared_mutex> lock(tables_mutex_);
      tables_.push_back(table);
      table_index = tables_.size() - 1;
    }
    table_name_to_id_map_.insert_or_update(database_schema.tables_[i].name_, table_index);
  }
}

Status DBMVP::CreateTable(meta::TableSchema& table_schema) {
  if (table_name_to_id_map_.contains(table_schema.name_)) {
    return Status(TABLE_ALREADY_EXISTS, "Table already exists: " + table_schema.name_);
  }
  std::unordered_map<std::string, std::string> headers;
  auto table = std::make_shared<TableMVP>(table_schema, db_catalog_path_, init_table_scale_, is_leader_, embedding_service_, headers);
  
  // Thread-safe insertion
  size_t table_index;
  {
    std::unique_lock<std::shared_mutex> lock(tables_mutex_);
    tables_.push_back(table);
    table_index = tables_.size() - 1;
  }
  table_name_to_id_map_.insert_or_update(table_schema.name_, table_index);
  return Status::OK();
}

Status DBMVP::DeleteTable(const std::string& table_name) {
  auto table = GetTable(table_name);
  if (!table) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }
  auto table_id = table->table_schema_.id_;

  auto table_index = table_name_to_id_map_.find(table_name);
  if (!table_index.has_value()) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }
  
  // Safely remove table with proper synchronization
  std::shared_ptr<TableMVP> table_to_delete;
  {
    std::unique_lock<std::shared_mutex> lock(tables_mutex_);
    // Keep a reference to the table being deleted
    table_to_delete = tables_[table_index.value()];
    // Reset the shared_ptr in the vector (but the table still exists via table_to_delete)
    tables_[table_index.value()].reset();
  }
  
  // Remove from name map after releasing the lock
  table_name_to_id_map_.erase(table_name);

  if (is_leader_) {
    // Delete table from disk.
    // TODO: verify if rebuild will have conflict on disk file in 2 threads.
    std::string table_path = db_catalog_path_ + "/" + std::to_string(table_id);
    server::CommonUtil::DeleteDirectory(table_path);  // Completely remove the table.
  }

  return Status::OK();
}

std::vector<std::string> DBMVP::GetTables() {
  return table_name_to_id_map_.keys();
}

std::shared_ptr<TableMVP> DBMVP::GetTable(const std::string& table_name) {
  auto table_index = table_name_to_id_map_.find(table_name);
  if (!table_index.has_value()) {
    return nullptr;  // Or throw an exception or return an error status, depending on your error handling strategy
  }
  
  std::shared_lock<std::shared_mutex> lock(tables_mutex_);
  return tables_[table_index.value()];
}

Status DBMVP::Rebuild() {
  // Loop through all tables and rebuild
  std::vector<std::shared_ptr<TableMVP>> tables_snapshot;
  {
    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    tables_snapshot = tables_;  // Make a copy while holding lock
  }
  
  for (auto& table : tables_snapshot) {
    if (table != nullptr) {
      auto status = table->Rebuild(db_catalog_path_);
      if (!status.ok()) {
        logger_.Error("Rebuild table " + table->table_schema_.name_ + " failed.");
      }
    }
  }
  return Status::OK();
}

Status DBMVP::Compact(const std::string& table_name, double threshold) {
  if (table_name.empty()) {
    // Compact all tables
    std::vector<std::shared_ptr<TableMVP>> tables_snapshot;
    {
      std::shared_lock<std::shared_mutex> lock(tables_mutex_);
      tables_snapshot = tables_;  // Make a copy while holding lock
    }
    
    int compacted_count = 0;
    for (auto& table : tables_snapshot) {
      if (table != nullptr && table->NeedsCompaction(threshold)) {
        auto status = table->Compact(threshold);
        if (status.ok()) {
          compacted_count++;
        } else {
          logger_.Error("Compaction failed for table " + table->table_schema_.name_ + ": " + status.message());
        }
      }
    }
    return Status(DB_SUCCESS, "Compacted " + std::to_string(compacted_count) + " tables");
  } else {
    // Compact specific table
    auto table_index = table_name_to_id_map_.find(table_name);
    if (!table_index.has_value()) {
      return Status(INVALID_NAME, "Table " + table_name + " does not exist");
    }
    
    std::shared_ptr<TableMVP> table;
    {
      std::shared_lock<std::shared_mutex> lock(tables_mutex_);
      table = tables_[table_index.value()];
    }
    
    if (table == nullptr) {
      return Status(DB_UNEXPECTED_ERROR, "Table " + table_name + " is null");
    }
    
    return table->Compact(threshold);
  }
}

Status DBMVP::SwapExecutors() {
  // Loop through all tables and swap executors
  std::vector<std::shared_ptr<TableMVP>> tables_snapshot;
  {
    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    tables_snapshot = tables_;  // Make a copy while holding lock
  }
  
  for (auto& table : tables_snapshot) {
    if (table != nullptr) {
      auto status = table->SwapExecutors();
      if (!status.ok()) {
        logger_.Error("Swap executors for table " + table->table_schema_.name_ + " failed.");
      }
    }
  }
  return Status::OK();
}

Status DBMVP::Release() {
  // Loop through all tables and release
  std::vector<std::shared_ptr<TableMVP>> tables_snapshot;
  {
    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    tables_snapshot = tables_;  // Make a copy while holding lock
  }
  
  for (auto& table : tables_snapshot) {
    if (table != nullptr) {
      auto status = table->Release();
      if (!status.ok()) {
        logger_.Error("Release table " + table->table_schema_.name_ + " failed.");
      }
    }
  }
  return Status::OK();
}

Status DBMVP::Dump(const std::string& db_catalog_path) {
  // Loop through all tables and dump
  std::vector<std::shared_ptr<TableMVP>> tables_snapshot;
  {
    std::shared_lock<std::shared_mutex> lock(tables_mutex_);
    tables_snapshot = tables_;  // Make a copy while holding lock
  }
  
  bool success = true;
  for (auto& table : tables_snapshot) {
    if (table != nullptr) {
      auto status = table->Dump(db_catalog_path);
      if (!status.ok()) {
        logger_.Error("Dump table " + table->table_schema_.name_ + " failed.");
        success = false;
      }
    }
  }
  if (!success) {
    return Status(DB_UNEXPECTED_ERROR, "Dump database failed.");
  }
  return Status::OK();
}

}  // namespace engine
}  // namespace vectordb
