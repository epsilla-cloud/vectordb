#include "db/db_server.hpp"
#include "db/catalog/basic_meta_impl.hpp"
#include <iostream>

namespace vectordb {
namespace engine {

DBServer::DBServer() {
  // Initialize the meta database
  meta_ = std::make_shared<meta::BasicMetaImpl>();
}

Status DBServer::LoadDB(const std::string& db_name, std::string& db_catalog_path) {
  // Load database meta
  vectordb::Status status = meta_->LoadDatabase(db_catalog_path, db_name);
  if (!status.ok()) {
    std::cout << status.message() << std::endl;
    return status;
  }

  meta::DatabaseSchema db_schema;
  meta_->GetDatabase(db_name, db_schema);
  if (db_name_to_id_map_.find(db_name) != db_name_to_id_map_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "DB already exists: " + db_name);
  }

  auto db = std::make_shared<DBMVP>(db_schema);
  dbs_.push_back(db);
  db_name_to_id_map_[db_schema.name_] = dbs_.size() - 1;
  return Status::OK();
}

Status DBServer::UnloadDB(const std::string& db_name) {
  // Unload database from meta.
  vectordb::Status status = meta_->UnloadDatabase(db_name);
  if (!status.ok()) {
    return status;
  }

  // Unload database from memory.
  auto it = db_name_to_id_map_.find(db_name);
  if (it == db_name_to_id_map_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  dbs_[it->second] = nullptr;  // Set the shared_ptr to null
  db_name_to_id_map_.erase(it);
  return Status::OK();
}

std::shared_ptr<DBMVP> DBServer::GetDB(const std::string& db_name) {
  auto it = db_name_to_id_map_.find(db_name);
  if (it == db_name_to_id_map_.end()) {
    return nullptr;  // Or throw an exception or return an error status, depending on your error handling strategy
  }
  return dbs_[it->second];
}

Status DBServer::CreateTable(const std::string& db_name, meta::TableSchema& table_schema) {
  // Create table in meta.
  vectordb::Status status = meta_->CreateTable(db_name, table_schema);
  if (!status.ok()) {
    return status;
  }
  return GetDB(db_name)->CreateTable(table_schema);
}

Status DBServer::DropTable(const std::string& db_name, const std::string& table_name) {
  // Drop table from meta.
  vectordb::Status status = meta_->DropTable(db_name, table_name);
  if (!status.ok()) {
    return status;
  }
  return GetDB(db_name)->DeleteTable(table_name);
}

Status DBServer::Rebuild() {
  // Loop through all dbs and rebuild
  for (int64_t i = 0; i < dbs_.size(); ++i) {
    std::shared_ptr<DBMVP> db = dbs_[i];
    if (db != nullptr) {
      auto status = db->Rebuild();
      if (!status.ok()) {
        std::cout << "Rebuild db to " << db->db_catalog_path_ << " failed." << std::endl;
      }
    }
  }
  return Status::OK();
}

Status DBServer::Insert(const std::string& db_name, const std::string& table_name, vectordb::Json& records) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }
  return table->Insert(records);
}

Status DBServer::Search(
  const std::string& db_name,
  const std::string& table_name, 
  std::string& field_name,
  std::vector<std::string>& query_fields, 
  int64_t query_dimension,
  const float* query_data, 
  const int64_t K, 
  vectordb::Json& result
) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }
  return table->Search(field_name, query_fields, query_dimension, query_data, K, result);
}

}  // namespace engine
}  // namespace vectordb
