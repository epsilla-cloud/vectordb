// basic_meta_impl.cpp
#include "db/catalog/basic_meta_impl.hpp"

#include "utils/common_util.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace engine {
namespace meta {

namespace {

constexpr const char* ID = "id";
constexpr const char* NAME = "name";
constexpr const char* PATH = "path";
constexpr const char* TABLES = "tables";
constexpr const char* FIELDS = "fields";
constexpr const char* AUTO_EMBEDDINGS = "auto_embeddings";
constexpr const char* SRC_FIELD_ID = "src_field_id";
constexpr const char* TGT_FIELD_ID = "tgt_field_id";
constexpr const char* MODEL_NAME = "model_name";
constexpr const char* FIELD_TYPE = "field_type";
constexpr const char* VECTOR_DIMENSION = "vector_dimension";
constexpr const char* METRIC_TYPE = "metric_type";

// Convert a Json object to a FieldSchema
Status LoadFieldSchemaFromJson(const vectordb::Json& json, meta::FieldSchema& field_schema) {
  field_schema.id_ = json.GetInt(ID);
  field_schema.name_ = json.GetString(NAME);
  field_schema.field_type_ = static_cast<meta::FieldType>(json.GetInt(FIELD_TYPE));
  // Only vector fields have vector_dimension_ and metric_type_.
  if (field_schema.field_type_ == meta::FieldType::VECTOR_FLOAT ||
      field_schema.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
    field_schema.vector_dimension_ = json.GetInt(VECTOR_DIMENSION);
    field_schema.metric_type_ = static_cast<meta::MetricType>(json.GetInt(METRIC_TYPE));
  }
  return Status::OK();
}

// Convert a Json object to a TableSchema
Status LoadTableSchemaFromJson(const vectordb::Json& json, meta::TableSchema& table_schema) {
  table_schema.id_ = json.GetInt(ID);
  table_schema.name_ = json.GetString(NAME);

  // Load fields
  size_t fields_size = json.GetArraySize(FIELDS);
  for (size_t i = 0; i < fields_size; ++i) {
    vectordb::Json field_json = json.GetArrayElement(FIELDS, i);
    meta::FieldSchema field_schema;
    LoadFieldSchemaFromJson(field_json, field_schema);
    table_schema.fields_.emplace_back(field_schema);
  }

  // Load auto_embeddings
  if (json.HasMember(AUTO_EMBEDDINGS)) {
    size_t auto_embeddings_size = json.GetArraySize(AUTO_EMBEDDINGS);
    for (size_t i = 0; i < auto_embeddings_size; ++i) {
      vectordb::Json auto_embedding_json = json.GetArrayElement(AUTO_EMBEDDINGS, i);
      meta::AutoEmbedding auto_embedding;
      auto_embedding.src_field_id_ = auto_embedding_json.GetInt(SRC_FIELD_ID);
      auto_embedding.tgt_field_id_ = auto_embedding_json.GetInt(TGT_FIELD_ID);
      auto_embedding.model_name_ = auto_embedding_json.GetString(MODEL_NAME);
      table_schema.auto_embeddings_.emplace_back(auto_embedding);
    }
  }

  return Status::OK();
}
}  // namespace

BasicMetaImpl::BasicMetaImpl() {
  DatabaseSchema default_db;
  default_db.name_ = "default";
  databases_["default"] = default_db;
}

BasicMetaImpl::~BasicMetaImpl() {}

Status BasicMetaImpl::LoadDatabase(std::string& db_catalog_path, const std::string& db_name) {
  if (loaded_databases_paths_.find(db_catalog_path) != loaded_databases_paths_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "Database catalog file is already loaded: " + db_catalog_path);
  }

  if (!server::CommonUtil::IsFileExist(db_catalog_path)) {
    return Status(DB_NOT_FOUND, "Database catalog file not found: " + db_catalog_path);
  }

  std::string json_content = server::CommonUtil::ReadContentFromFile(db_catalog_path);

  Json json;
  if (!json.LoadFromString(json_content)) {
    return Status(DB_UNEXPECTED_ERROR, "Failed to parse database catalog file: " + db_catalog_path);
  }

  // Load the actual database schema from the JSON data.
  DatabaseSchema db_schema;
  db_schema.id_ = json.GetInt("id");
  db_schema.name_ = json.GetString("name");
  db_schema.path_ = db_catalog_path;
  // Load tables
  size_t tables_size = json.GetArraySize(TABLES);
  for (size_t i = 0; i < tables_size; ++i) {
    vectordb::Json table_json = json.GetArrayElement(TABLES, i);
    meta::TableSchema table_schema;
    LoadTableSchemaFromJson(table_json, table_schema);
    db_schema.tables_.emplace_back(table_schema);
  }

  databases_[db_name] = db_schema;
  loaded_databases_paths_.insert(db_catalog_path);

  return Status::OK();
}

Status BasicMetaImpl::HasDatabase(const std::string& db_name, bool& response) {
  response = databases_.find(db_name) != databases_.end();
  return Status::OK();
}

Status BasicMetaImpl::GetDatabase(const std::string& db_name, DatabaseSchema& response) {
  auto it = databases_.find(db_name);
  if (it == databases_.end()) {
    return Status(DB_NOT_FOUND, "Database not found: " + db_name);
  }
  response = it->second;
  return Status::OK();
}

Status BasicMetaImpl::UnloadDatabase(const std::string& db_name) {
  auto it = databases_.find(db_name);
  if (it == databases_.end()) {
    return Status(DB_NOT_FOUND, "Database not found: " + db_name);
  }
  auto path = it->second.path_;
  loaded_databases_paths_.erase(path);
  databases_.erase(db_name);
  return Status::OK();
}

Status BasicMetaImpl::DropDatabase(const std::string& db_name) {
  auto it = databases_.find(db_name);
  if (it == databases_.end()) {
    return Status(DB_NOT_FOUND, "Database not found: " + db_name);
  }
  auto path = it->second.path_;
  server::CommonUtil::DeleteDirectory(path); // Completely remove the database.
  loaded_databases_paths_.erase(path);
  databases_.erase(db_name);
  return Status::OK();
}

Status BasicMetaImpl::CreateTable(std::string& db_name, TableSchema& table_schema) {
  // Table name cannot be duplicated.
  bool has_table = false;
  auto status = HasTable(db_name, table_schema.name_, has_table);
  if (!status.ok()) {
    return status;
  }
  if (has_table) {
    return Status(TABLE_ALREADY_EXISTS, "Table already exists: " + table_schema.name_);
  }

  databases_.find(db_name)->second.tables_.push_back(table_schema);
  // TODO: Flush the change to disk.

  return Status::OK();
}

Status BasicMetaImpl::HasTable(const std::string& db_name, const std::string& table_name, bool& response) {
  auto it = databases_.find(db_name);
  if (it == databases_.end()) {
    return Status(DB_NOT_FOUND, "Database not found: " + db_name);
  }

  // Assuming that each table has a unique name within its database
  for (const auto& table : it->second.tables_) {
    if (table_name == table.name_) {
      response = true;
      return Status::OK();
    }
  }

  response = false;
  return Status::OK();
}

Status BasicMetaImpl::GetTable(const std::string& db_name, const std::string& table_name, TableSchema& response) {
  auto it = databases_.find(db_name);
  if (it == databases_.end()) {
    return Status(DB_NOT_FOUND, "Database not found: " + db_name);
  }

  // Assuming that each table has a unique name within its database
  for (const auto& table : it->second.tables_) {
    if (table_name == table.name_) {
      response = table;
      return Status::OK();
    }
  }

  return Status(TABLE_NOT_FOUND, "Table not found: " + table_name);
}

Status BasicMetaImpl::DropTable(const std::string& db_name, const std::string& table_name) {
  auto it = databases_.find(db_name);
  if (it == databases_.end()) {
    return Status(DB_NOT_FOUND, "Database not found: " + db_name);
  }

  // Assuming that each table has a unique name within its database
  for (auto it_table = it->second.tables_.begin(); it_table != it->second.tables_.end(); ++it_table) {
    if (it_table->name_ == table_name) {
      it->second.tables_.erase(it_table);
      return Status::OK();
    }
  }

  // TODO: Flush the change to disk.

  return Status(TABLE_NOT_FOUND, "Table not found: " + table_name);
}

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
