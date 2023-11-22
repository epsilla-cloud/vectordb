#include "db/db_server.hpp"

#include <iostream>
#include <string>

#include "db/catalog/basic_meta_impl.hpp"

namespace vectordb {
namespace engine {

DBServer::DBServer() {
  // Initialize the meta database
  meta_ = std::make_shared<meta::BasicMetaImpl>();
}

DBServer::~DBServer() {
  // Set the stop_rebuild_thread_ flag to true to stop the periodic rebuild
  stop_rebuild_thread_ = true;

  // Wait for the rebuild_thread_ to finish and join before destruction
  if (rebuild_thread_.joinable()) {
    rebuild_thread_.join();
  }
}

Status DBServer::LoadDB(const std::string& db_name,
                        std::string& db_catalog_path, int64_t init_table_scale,
                        bool wal_enabled) {
  // Load database meta
  vectordb::Status status = meta_->LoadDatabase(db_catalog_path, db_name);
  if (!status.ok()) {
    std::cout << status.message() << std::endl;
    return status;
  }
  try {
    meta::DatabaseSchema db_schema;
    meta_->GetDatabase(db_name, db_schema);
    if (db_name_to_id_map_.find(db_name) != db_name_to_id_map_.end()) {
      return Status(DB_UNEXPECTED_ERROR, "DB already exists: " + db_name);
    }

    auto db = std::make_shared<DBMVP>(db_schema, init_table_scale, is_leader_);
    db->SetWALEnabled(wal_enabled);
    dbs_.push_back(db);
    db_name_to_id_map_[db_schema.name_] = dbs_.size() - 1;
    return Status::OK();
  } catch (std::exception& ex) {
    meta_->UnloadDatabase(db_name);
    return Status(DB_UNEXPECTED_ERROR, std::string(ex.what()));
  }
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
    return nullptr;  // Or throw an exception or return an error status,
                     // depending on your error handling strategy
  }
  return dbs_[it->second];
}

Status DBServer::CreateTable(const std::string& db_name,
                             meta::TableSchema& table_schema, size_t& table_id) {
  // Create table in meta.
  vectordb::Status status = meta_->CreateTable(db_name, table_schema, table_id);
  if (!status.ok()) {
    return status;
  }
  return GetDB(db_name)->CreateTable(table_schema);
}

Status DBServer::DropTable(const std::string& db_name,
                           const std::string& table_name) {
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
        std::cout << "Rebuild db to " << db->db_catalog_path_ << " failed."
                  << std::endl;
      }
    }
  }
  return Status::OK();
}

Status DBServer::ListTables(const std::string& db_name, std::vector<std::string>& table_names) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  table_names = db->GetTables();
  return Status::OK();
}

Status DBServer::Insert(const std::string& db_name,
                        const std::string& table_name,
                        vectordb::Json& records) {
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

Status DBServer::InsertPrepare(const std::string& db_name,
                               const std::string& table_name,
                               vectordb::Json& pks,
                               vectordb::Json& result) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }
  return table->InsertPrepare(pks, result);
}

Status DBServer::Delete(
    const std::string& db_name,
    const std::string& table_name,
    vectordb::Json& pkList,
    const std::string& filter) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }
  // Semantic check primary keys list.
  if (pkList.GetSize() > 0) {
    int pkIdx = 0;
    for (; pkIdx < table->table_schema_.fields_.size(); pkIdx++) {
      if (table->table_schema_.fields_[pkIdx].is_primary_key_) {
        break;
      }
    }
    if (pkIdx == table->table_schema_.fields_.size()) {
      return Status(DB_UNEXPECTED_ERROR, "Primary key not found: " + table_name);
    }

    // simple sanity check
    auto pkField = table->table_schema_.fields_[pkIdx];
    size_t pkListSize = pkList.GetSize();
    if (pkListSize == 0) {
      std::cout << "No pk to delete." << std::endl;
      return Status::OK();
    }
    switch (pkField.field_type_) {
      case meta::FieldType::INT1:  // fall through
      case meta::FieldType::INT2:  // fall through
      case meta::FieldType::INT4:  // fall through
      case meta::FieldType::INT8:
        for (int i = 0; i < pkListSize; i++) {
          if (!pkList.GetArrayElement(i).IsNumber()) {
            return Status(DB_UNEXPECTED_ERROR, "Primary key type mismatch at field position " + std::to_string(i));
          }
        }
        break;
      case meta::FieldType::STRING:
        for (int i = 0; i < pkListSize; i++) {
          if (!pkList.GetArrayElement(i).IsString()) {
            return Status(DB_UNEXPECTED_ERROR, "Primary key type mismatch at field position " + std::to_string(i));
          }
        }
        break;
      default:
        return Status(DB_UNEXPECTED_ERROR, "unexpected Primary key type.");
    }
  }

  // Filter validation
  std::vector<query::expr::ExprNodePtr> expr_nodes;
  Status expr_parse_status = vectordb::query::expr::Expr::ParseNodeFromStr(filter, expr_nodes, table->field_name_type_map_);
  if (!expr_parse_status.ok()) {
    return expr_parse_status;
  }

  return table->Delete(pkList, filter, expr_nodes);
}

Status DBServer::Search(const std::string& db_name,
                        const std::string& table_name, std::string& field_name,
                        std::vector<std::string>& query_fields,
                        int64_t query_dimension,
                        const Vector query_data,
                        const int64_t limit, vectordb::Json& result,
                        const std::string& filter,
                        bool with_distance) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }

  // Filter validation
  std::vector<query::expr::ExprNodePtr> expr_nodes;
  Status expr_parse_status = vectordb::query::expr::Expr::ParseNodeFromStr(filter, expr_nodes, table->field_name_type_map_);
  if (!expr_parse_status.ok()) {
    return expr_parse_status;
  }

  return table->Search(field_name, query_fields, query_dimension, query_data, limit,
                       result, expr_nodes, with_distance);
}

Status DBServer::Project(const std::string& db_name,
                         const std::string& table_name,
                         std::vector<std::string>& query_fields,
                         vectordb::Json& primary_keys,
                         const std::string& filter,
                         const int64_t skip,
                         const int64_t limit,
                         vectordb::Json& result) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }

  // Filter validation
  std::vector<query::expr::ExprNodePtr> expr_nodes;
  Status expr_parse_status = vectordb::query::expr::Expr::ParseNodeFromStr(filter, expr_nodes, table->field_name_type_map_);
  if (!expr_parse_status.ok()) {
    return expr_parse_status;
  }

  std::vector<int64_t> ids;
  std::vector<double> distances;
  return table->SearchByAttribute(query_fields, primary_keys, expr_nodes, skip, limit, result);
}

}  // namespace engine
}  // namespace vectordb
