// basic_meta_impl.cpp
#include "db/catalog/basic_meta_impl.hpp"

#include "utils/common_util.hpp"
#include "utils/json.hpp"

#include <iostream>

namespace vectordb {
namespace engine {
namespace meta {

namespace {

constexpr const char* DEFAULT_DB_NAME = "default";

constexpr const char* ID = "id";
constexpr const char* NAME = "name";
constexpr const char* PATH = "path";
constexpr const char* TABLES = "tables";
constexpr const char* FIELDS = "fields";
constexpr const char* AUTO_EMBEDDINGS = "auto_embeddings";
constexpr const char* SRC_FIELD_ID = "src_field_id";
constexpr const char* TGT_FIELD_ID = "tgt_field_id";
constexpr const char* MODEL_NAME = "model_name";
constexpr const char* IS_PRIMARY_KEY = "is_primary_key";
constexpr const char* FIELD_TYPE = "field_type";
constexpr const char* VECTOR_DIMENSION = "vector_dimension";
constexpr const char* METRIC_TYPE = "metric_type";

constexpr const char* DB_CATALOG_FILE_NAME = "catalog";

// Convert a Json object to a FieldSchema
Status LoadFieldSchemaFromJson(const vectordb::Json& json, meta::FieldSchema& field_schema) {
  field_schema.id_ = json.GetInt(ID);
  field_schema.name_ = json.GetString(NAME);
  field_schema.is_primary_key_ = json.GetBool(IS_PRIMARY_KEY);
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

// Convert a FieldSchema to a Json object
void DumpFieldSchemaToJson(const meta::FieldSchema& field_schema, vectordb::Json& json) {
  json.LoadFromString("{}");
  json.SetInt(ID, field_schema.id_);
  json.SetString(NAME, field_schema.name_);
  json.SetBool(IS_PRIMARY_KEY, field_schema.is_primary_key_);
  json.SetInt(FIELD_TYPE, static_cast<int>(field_schema.field_type_));
  // Only vector fields have vector_dimension_ and metric_type_.
  if (field_schema.field_type_ == meta::FieldType::VECTOR_FLOAT ||
      field_schema.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
    json.SetInt(VECTOR_DIMENSION, field_schema.vector_dimension_);
    json.SetInt(METRIC_TYPE, static_cast<int>(field_schema.metric_type_));
  }
}

// Convert a TableSchema to a Json object
void DumpTableSchemaToJson(const meta::TableSchema& table_schema, vectordb::Json& json) {
  json.LoadFromString("{}");
  json.SetInt(ID, table_schema.id_);
  json.SetString(NAME, table_schema.name_);

  // Dump fields
  std::vector<vectordb::Json> empty_array;
  json.SetArray(FIELDS, empty_array);
  for (const auto& field_schema : table_schema.fields_) {
    vectordb::Json field_json;
    DumpFieldSchemaToJson(field_schema, field_json);
    json.AddObjectToArray(FIELDS, field_json);
  }

  // Dump auto_embeddings
  if (!table_schema.auto_embeddings_.empty()) {
    std::vector<vectordb::Json> empty_array;
    json.SetArray(AUTO_EMBEDDINGS, empty_array);
    for (const auto& auto_embedding : table_schema.auto_embeddings_) {
      vectordb::Json auto_embedding_json;
      auto_embedding_json.LoadFromString("{}");
      auto_embedding_json.SetInt(SRC_FIELD_ID, auto_embedding.src_field_id_);
      auto_embedding_json.SetInt(TGT_FIELD_ID, auto_embedding.tgt_field_id_);
      auto_embedding_json.SetString(MODEL_NAME, auto_embedding.model_name_);
      json.AddObjectToArray(AUTO_EMBEDDINGS, auto_embedding_json);
    }
  }
}

// Convert a DatabaseSchema to a Json object
void DumpDatabaseSchemaToJson(const DatabaseSchema& db_schema, vectordb::Json& json) {
  json.LoadFromString("{}");
  json.SetInt("id", db_schema.id_);
  // json.AddString("name", db_schema.name_);
  
  // Initialize an empty array for tables
  std::vector<vectordb::Json> empty_array;
  json.SetArray(TABLES, empty_array);

  // Dump tables
  for (const auto& table_schema : db_schema.tables_) {
    vectordb::Json table_json;
    DumpTableSchemaToJson(table_schema, table_json);
    json.AddObjectToArray(TABLES, table_json);
  }
}

Status SaveDBToFile(const DatabaseSchema& db, const std::string& file_path) {
  // Skip the default database
  if (db.name_ == DEFAULT_DB_NAME) {
    return Status::OK();
  }

  // Convert the DatabaseSchema to a Json object
  vectordb::Json json;
  DumpDatabaseSchemaToJson(db, json);

  // Write the Json object to a string
  std::string json_string = json.DumpToString();

  // Write the string to the file
  return server::CommonUtil::AtomicWriteToFile(file_path, json_string);
}

int64_t GetNewTableId(const DatabaseSchema& db) {
  int64_t max_id = -1;
  for (const auto& table : db.tables_) {
    std::cout << table.id_ << " " << table.name_ << std::endl;
    if (table.id_ > max_id) {
      max_id = table.id_;
    }
  }
  return max_id + 1;
}

}  // namespace

BasicMetaImpl::BasicMetaImpl() {
  DatabaseSchema default_db;
  default_db.name_ = DEFAULT_DB_NAME;
  databases_[DEFAULT_DB_NAME] = default_db;
}

BasicMetaImpl::~BasicMetaImpl() {}

Status BasicMetaImpl::LoadDatabase(std::string& db_catalog_path, const std::string& db_name) {
  if (loaded_databases_paths_.find(db_catalog_path) != loaded_databases_paths_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "Database catalog file is already loaded: " + db_catalog_path);
  }
  if (databases_.find(db_name) != databases_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "DB already exists: " + db_name);
  }

  DatabaseSchema db_schema;
  db_schema.name_ = db_name;
  db_schema.path_ = db_catalog_path;
  if (server::CommonUtil::IsFileExist(db_catalog_path)) {
    std::string json_content = server::CommonUtil::ReadContentFromFile(db_catalog_path + "/" + DB_CATALOG_FILE_NAME);
    Json json;
    if (!json.LoadFromString(json_content)) {
      return Status(DB_UNEXPECTED_ERROR, "Failed to parse database catalog file: " + db_catalog_path);
    }

    // Load the actual database schema from the JSON data.
    db_schema.id_ = json.GetInt("id");
    // Load tables
    size_t tables_size = json.GetArraySize(TABLES);
    for (size_t i = 0; i < tables_size; ++i) {
      vectordb::Json table_json = json.GetArrayElement(TABLES, i);
      meta::TableSchema table_schema;
      LoadTableSchemaFromJson(table_json, table_schema);
      db_schema.tables_.emplace_back(table_schema);
    }
  } else {
    // Create directory with an empty db.
    server::CommonUtil::CreateDirectory(db_catalog_path);
    // Create database catalog file.
    DatabaseSchema db_schema;
    auto status = SaveDBToFile(db_schema, db_catalog_path + "/" + DB_CATALOG_FILE_NAME);

    if (!status.ok()) {
      return status;
    }
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
  server::CommonUtil::DeleteDirectory(path);  // Completely remove the database.
  loaded_databases_paths_.erase(path);
  databases_.erase(db_name);
  return Status::OK();
}

Status BasicMetaImpl::CreateTable(const std::string& db_name, TableSchema& table_schema) {
  // Table name cannot be duplicated.
  bool has_table = false;
  auto status = HasTable(db_name, table_schema.name_, has_table);
  if (!status.ok()) {
    return status;
  }
  if (has_table) {
    return Status(TABLE_ALREADY_EXISTS, "Table already exists: " + table_schema.name_);
  }

  // TODO: Validate the table schema.

  auto& db = databases_.find(db_name)->second;
  
  // TODO: a better way to assign table id.
  table_schema.id_ = GetNewTableId(db);
  std::cout << "table id: " << table_schema.id_ << std::endl;

  db.tables_.push_back(table_schema);
  // Flush the change of the database schema to disk.
  status = SaveDBToFile(db, db.path_ + "/" + DB_CATALOG_FILE_NAME);

  return status;
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

  auto& db = it->second;

  // Assuming that each table has a unique name within its database
  for (auto it_table = db.tables_.begin(); it_table != db.tables_.end(); ++it_table) {
    if (it_table->name_ == table_name) {
      db.tables_.erase(it_table);
      // Flush the change of the database schema to disk.
      auto status = SaveDBToFile(db, db.path_ + "/" + DB_CATALOG_FILE_NAME);
      return status;
    }
  }

  return Status(TABLE_NOT_FOUND, "Table not found: " + table_name);
}

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
