#include "db/db_mvp.hpp"

namespace vectordb {
namespace engine {

DBMVP::DBMVP(meta::DatabaseSchema& database_schema) {
  // Here you might want to initialize your database based on the provided schema
  // This may involve creating tables, loading data, etc.
  db_catalog_path_ = database_schema.path_;
  for (int i = 0; i < database_schema.tables_.size(); ++i) {
    auto table = std::make_shared<TableMVP>(database_schema.tables_[i], db_catalog_path_);
    tables_.push_back(table);
    table_name_to_id_map_[database_schema.tables_[i].name_] = tables_.size() - 1;
  }
}

Status DBMVP::CreateTable(meta::TableSchema& table_schema) {
  if (table_name_to_id_map_.find(table_schema.name_) != table_name_to_id_map_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "Table already exists: " + table_schema.name_);
  }
  auto table = std::make_shared<TableMVP>(table_schema, db_catalog_path_);
  tables_.push_back(table);
  table_name_to_id_map_[table_schema.name_] = tables_.size() - 1;
  return Status::OK();
}

Status DBMVP::DeleteTable(const std::string& table_name) {
  // TODO: delete the table from disk
  auto it = table_name_to_id_map_.find(table_name);
  if (it == table_name_to_id_map_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }
  tables_[it->second] = nullptr;  // Set the shared_ptr to null
  table_name_to_id_map_.erase(it);
  return Status::OK();
}

std::shared_ptr<TableMVP> DBMVP::GetTable(const std::string& table_name) {
  auto it = table_name_to_id_map_.find(table_name);
  if (it == table_name_to_id_map_.end()) {
    return nullptr;  // Or throw an exception or return an error status, depending on your error handling strategy
  }
  return tables_[it->second];
}

}  // namespace engine
}  // namespace vectordb
