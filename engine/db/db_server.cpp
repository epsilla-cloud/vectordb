#include "db/db_server.hpp"

#include <iostream>
#include <string>
#include <chrono>

#include "db/catalog/basic_meta_impl.hpp"
#include "db/execution/aggregation.hpp"
#include "config/config.hpp"

namespace vectordb {
namespace engine {

DBServer::DBServer() {
  // Initialize the meta database
  meta_ = std::make_shared<meta::BasicMetaImpl>();
  
  // Start WAL flush thread if auto-flush is enabled
  if (vectordb::globalConfig.WALAutoFlush.load()) {
    StartWALFlushThread();
  }
}

DBServer::~DBServer() {
  // Set the stop_rebuild_thread_ flag to true to stop the periodic rebuild
  stop_rebuild_thread_ = true;

  // Wait for the rebuild_thread_ to finish and join before destruction
  if (rebuild_thread_.joinable()) {
    rebuild_thread_.join();
  }
  
  // Stop WAL flush thread
  StopWALFlushThread();
}

Status DBServer::LoadDB(const std::string& db_name,
                        const std::string& db_catalog_path, int64_t init_table_scale,
                        bool wal_enabled,
                        std::unordered_map<std::string, std::string> &headers) {
  // Load database meta
  vectordb::Status status = meta_->LoadDatabase(db_catalog_path, db_name);
  if (!status.ok()) {
    logger_.Error(status.message());
    return status;
  }
  try {
    meta::DatabaseSchema db_schema;
    meta_->GetDatabase(db_name, db_schema);
    if (db_name_to_id_map_.contains(db_name)) {
      return Status(DB_UNEXPECTED_ERROR, "DB already exists: " + db_name);
    }

    auto db = std::make_shared<Database>(db_schema, init_table_scale, is_leader_, embedding_service_, headers);
    db->SetWALEnabled(wal_enabled);
    
    // Thread-safe insertion
    size_t db_index;
    {
      std::unique_lock<std::shared_mutex> lock(dbs_mutex_);
      dbs_.push_back(db);
      db_index = dbs_.size() - 1;
    }
    db_name_to_id_map_.insert_or_update(db_schema.name_, db_index);
    return Status::OK();
  } catch (std::exception& ex) {
    meta_->UnloadDatabase(db_name);
    return Status(DB_UNEXPECTED_ERROR, std::string(ex.what()));
  }
}

Status DBServer::UnloadDB(const std::string& db_name) {
  // Get database instance before unloading
  auto db_index = db_name_to_id_map_.find(db_name);
  if (!db_index.has_value()) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  
  std::shared_ptr<Database> db_to_unload;
  {
    std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
    db_to_unload = dbs_[db_index.value()];
  }
  
  if (db_to_unload) {
    // Critical: Flush WAL before unloading to ensure data persistence
    logger_.Info("Flushing WAL before unloading database: " + db_name);
    auto flush_status = db_to_unload->FlushAllWAL();
    if (!flush_status.ok()) {
      logger_.Error("Failed to flush WAL for database " + db_name + ": " + flush_status.message());
      // Continue with unload but log the error
    }
    
    // Save metadata to disk
    logger_.Info("Saving metadata before unloading database: " + db_name);
    auto dump_status = db_to_unload->Dump(db_to_unload->db_catalog_path_);
    if (!dump_status.ok()) {
      logger_.Warning("Failed to save metadata for database " + db_name + ": " + dump_status.message());
      // Continue with unload but log the warning
    }
  }
  
  // Unload database from meta.
  vectordb::Status status = meta_->UnloadDatabase(db_name);
  if (!status.ok()) {
    return status;
  }

  // Unload database from memory.
  // Safely remove database with proper synchronization
  {
    std::unique_lock<std::shared_mutex> lock(dbs_mutex_);
    // Reset the shared_ptr in the vector
    dbs_[db_index.value()].reset();
  }
  
  // Remove from name map after releasing the lock
  db_name_to_id_map_.erase(db_name);
  
  logger_.Info("Successfully unloaded database: " + db_name);
  return Status::OK();
}

Status DBServer::ReleaseDB(const std::string& db_name) {
  // Release database from memory.
  auto db_index = db_name_to_id_map_.find(db_name);
  if (!db_index.has_value()) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  
  std::shared_ptr<Database> db;
  {
    std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
    db = dbs_[db_index.value()];
  }
  
  if (db) {
    db->Release();
  }
  return Status::OK();
}

Status DBServer::DumpDB(const std::string& db_name,
                        const std::string& db_catalog_path) {
  // Check that DB exists.
  auto db_index = db_name_to_id_map_.find(db_name);
  if (!db_index.has_value()) {
    return Status(DB_NOT_FOUND, "DB not found: " + db_name);
  }
  // Create the folder if not exist.
  if (!server::CommonUtil::CreateDirectory(db_catalog_path).ok()) {
    return Status(DB_UNEXPECTED_ERROR, "Failed to create directory: " + db_catalog_path);
  }
  // Dump meta to disk.
  meta::DatabaseSchema db_schema;
  meta_->GetDatabase(db_name, db_schema);
  auto status = meta_->SaveDBToFile(db_schema, db_catalog_path + "/catalog");
  if (!status.ok()) {
    return status;
  }
  // Dump DB to disk.
  std::shared_ptr<Database> db;
  {
    std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
    db = dbs_[db_index.value()];
  }
  
  if (!db) {
    return Status(DB_UNEXPECTED_ERROR, "DB is null");
  }
  return db->Dump(db_catalog_path);
}

Status DBServer::GetStatistics(const std::string& db_name,
                               vectordb::Json& response) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  
  // Create a snapshot of tables to avoid race conditions
  std::vector<std::shared_ptr<TableMVP>> tables_snapshot;
  {
    std::shared_lock<std::shared_mutex> lock(db->tables_mutex_);
    tables_snapshot = db->tables_;  // Copy the vector while holding the lock
  }
  
  // Now safely iterate over the snapshot without holding the lock
  for (auto& table: tables_snapshot) {
    if (table) {  // Check if table is not null
      vectordb::Json table_result;
      table_result.LoadFromString("{}");
      table_result.SetString("tableName", table->table_schema_.name_);
      table_result.SetInt("totalRecords", table->GetRecordCount());
      response.AddObjectToArray("result", table_result);
    }
  }
  return Status::OK();
}

std::shared_ptr<Database> DBServer::GetDB(const std::string& db_name) {
  auto db_index = db_name_to_id_map_.find(db_name);
  if (!db_index.has_value()) {
    return nullptr;  // Or throw an exception or return an error status,
                     // depending on your error handling strategy
  }
  
  std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
  return dbs_[db_index.value()];
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

Status DBServer::CreateTable(const std::string& db_name,
                             const std::string& table_schema_json, size_t& table_id) {
  vectordb::Json parsedBody;
  auto valid = parsedBody.LoadFromString(table_schema_json);
  if (!valid) {
    return Status{USER_ERROR, "Invalid JSON payload."};
  }

  bool return_table_id = false;
  if (parsedBody.HasMember("returnTableId")) {
    return_table_id = parsedBody.GetBool("returnTableId");
  }

  vectordb::engine::meta::TableSchema table_schema;
  if (!parsedBody.HasMember("name")) {
    return Status{USER_ERROR, "Missing table name in your payload."};
  }
  table_schema.name_ = parsedBody.GetString("name");

  if (!parsedBody.HasMember("fields")) {
    return Status{USER_ERROR, "Missing fields in your payload."};
  }
  size_t fields_size = parsedBody.GetArraySize("fields");
  bool has_primary_key = false;
  for (size_t i = 0; i < fields_size; i++) {
    auto body_field = parsedBody.GetArrayElement("fields", i);
    vectordb::engine::meta::FieldSchema field;
    field.id_ = i;
    field.name_ = body_field.GetString("name");
    if (body_field.HasMember("primaryKey")) {
      field.is_primary_key_ = body_field.GetBool("primaryKey");
      if (field.is_primary_key_) {
        if (has_primary_key) {
          return Status{USER_ERROR, "At most one field can be primary key."};
        }
        has_primary_key = true;
      }
    }
    if (body_field.HasMember("dataType")) {
      std::string d_type;
      field.field_type_ = engine::meta::GetFieldType(d_type.assign(body_field.GetString("dataType")));
    }
    if (
        field.field_type_ == vectordb::engine::meta::FieldType::VECTOR_DOUBLE ||
        field.field_type_ == vectordb::engine::meta::FieldType::VECTOR_FLOAT ||
        field.field_type_ == vectordb::engine::meta::FieldType::SPARSE_VECTOR_FLOAT ||
        field.field_type_ == vectordb::engine::meta::FieldType::SPARSE_VECTOR_DOUBLE) {
      if (!body_field.HasMember("dimensions")) {
        return Status{USER_ERROR, "Vector field must have dimensions."};
      }
    }
    if (body_field.HasMember("dimensions")) {
      field.vector_dimension_ = body_field.GetInt("dimensions");
    }
    if (body_field.HasMember("metricType")) {
      std::string m_type;
      field.metric_type_ = engine::meta::GetMetricType(m_type.assign(body_field.GetString("metricType")));
      if (field.metric_type_ == vectordb::engine::meta::MetricType::UNKNOWN) {
        return Status{USER_ERROR, "invalid metric type: " + body_field.GetString("metricType")};
      }
    }
    table_schema.fields_.push_back(field);
  }

  if (parsedBody.HasMember("autoEmbedding")) {
    size_t embeddings_size = parsedBody.GetArraySize("autoEmbedding");
    for (size_t i = 0; i < embeddings_size; i++) {
      auto body_embedding = parsedBody.GetArrayElement("autoEmbedding", i);
      vectordb::engine::meta::AutoEmbedding embedding;
      embedding.src_field_id_ = body_embedding.GetInt("source");
      embedding.tgt_field_id_ = body_embedding.GetInt("target");
      embedding.model_name_ = body_embedding.GetString("modelName");
      table_schema.auto_embeddings_.push_back(embedding);
    }
  }

  return CreateTable(db_name, table_schema, table_id);
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
  std::vector<std::shared_ptr<Database>> dbs_snapshot;
  {
    std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
    dbs_snapshot = dbs_;  // Make a copy while holding lock
  }
  
  for (auto& db : dbs_snapshot) {
    if (db != nullptr) {
      auto status = db->Rebuild();
      if (!status.ok()) {
        logger_.Error("Rebuild db to " + db->db_catalog_path_ + " failed.");
      }
    }
  }
  return Status::OK();
}

Status DBServer::Compact(const std::string& db_name, const std::string& table_name, double threshold) {
  if (db_name.empty()) {
    // Compact all databases
    std::vector<std::shared_ptr<Database>> dbs_snapshot;
    {
      std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
      dbs_snapshot = dbs_;  // Make a copy while holding lock
    }
    
    int compacted_count = 0;
    for (auto& db : dbs_snapshot) {
      if (db != nullptr) {
        auto status = db->Compact(table_name, threshold);
        if (status.ok()) {
          compacted_count++;
        } else {
          logger_.Error("Compaction failed for database: " + status.message());
        }
      }
    }
    return Status(DB_SUCCESS, "Compacted " + std::to_string(compacted_count) + " databases");
  } else {
    // Compact specific database
    auto db = GetDB(db_name);
    if (db == nullptr) {
      return Status(INVALID_NAME, "Database " + db_name + " does not exist");
    }
    return db->Compact(table_name, threshold);
  }
}

Status DBServer::SwapExecutors() {
  // Loop through all dbs and swap executors
  std::vector<std::shared_ptr<Database>> dbs_snapshot;
  {
    std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
    dbs_snapshot = dbs_;  // Make a copy while holding lock
  }
  
  for (auto& db : dbs_snapshot) {
    if (db != nullptr) {
      auto status = db->SwapExecutors();
      if (!status.ok()) {
        logger_.Error("Swap executors for db of " + db->db_catalog_path_ + " failed.");
      }
    }
  }
  return Status::OK();
}

Status DBServer::GetRecordCount(const std::string& db_name, 
                                const std::string& table_name,
                                vectordb::Json& result) {
  // If db_name is empty, get total count across all databases
  if (db_name.empty()) {
    size_t total_count = 0;
    vectordb::Json db_counts;
    db_counts.LoadFromString("{}");
    
    std::vector<std::shared_ptr<Database>> dbs_snapshot;
    {
      std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
      dbs_snapshot = dbs_;
    }
    
    for (auto& db : dbs_snapshot) {
      if (db != nullptr) {
        vectordb::Json db_result;
        db_result.LoadFromString("{}");
        auto status = db->GetRecordCount(table_name, db_result);
        if (status.ok()) {
          size_t db_total = db_result.GetInt("totalRecords");
          total_count += db_total;
          db_counts.SetObject(db->GetName(), db_result);
        }
      }
    }
    
    result.SetInt("totalRecords", total_count);
    result.SetObject("databases", db_counts);
    return Status::OK();
  }
  
  // Get specific database
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  
  return db->GetRecordCount(table_name, result);
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
                        vectordb::Json& records,
                        std::unordered_map<std::string, std::string> &headers,
                        bool upsert) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }
  return table->Insert(records, headers, upsert);
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
  
  // Log deletion request received at DBServer level
  size_t pk_count = pkList.GetSize();
  std::string log_msg = "[DBServer] Delete request received for db=" + db_name + 
                       ", table=" + table_name;
  if (pk_count > 0) {
    log_msg += ", primaryKeys=" + std::to_string(pk_count) + " items";
  }
  if (!filter.empty()) {
    log_msg += ", filter=[" + filter + "]";
  }
  logger_.Debug(log_msg);
  
  auto db = GetDB(db_name);
  if (db == nullptr) {
    logger_.Error("[DBServer] Database not found: " + db_name);
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    logger_.Error("[DBServer] Table not found: " + table_name + " in db: " + db_name);
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
      logger_.Error("[DBServer] Primary key field not found in table: " + table_name);
      return Status(DB_UNEXPECTED_ERROR, "Primary key not found: " + table_name);
    }

    // simple sanity check
    auto pkField = table->table_schema_.fields_[pkIdx];
    size_t pkListSize = pkList.GetSize();
    if (pkListSize == 0) {
      logger_.Info("[DBServer] No primary keys provided for deletion.");
      return Status::OK();
    }
    
    logger_.Debug("[DBServer] Validating " + std::to_string(pkListSize) + " primary keys of type: " + 
                 std::to_string(static_cast<int>(pkField.field_type_)));
    
    switch (pkField.field_type_) {
      case meta::FieldType::INT1:  // fall through
      case meta::FieldType::INT2:  // fall through
      case meta::FieldType::INT4:  // fall through
      case meta::FieldType::INT8:
        for (int i = 0; i < pkListSize; i++) {
          if (!pkList.GetArrayElement(i).IsNumber()) {
            logger_.Error("[DBServer] Primary key type mismatch at position " + std::to_string(i) + 
                         " - expected number");
            return Status(DB_UNEXPECTED_ERROR, "Primary key type mismatch at field position " + std::to_string(i));
          }
        }
        break;
      case meta::FieldType::STRING:
        for (int i = 0; i < pkListSize; i++) {
          if (!pkList.GetArrayElement(i).IsString()) {
            logger_.Error("[DBServer] Primary key type mismatch at position " + std::to_string(i) + 
                         " - expected string");
            return Status(DB_UNEXPECTED_ERROR, "Primary key type mismatch at field position " + std::to_string(i));
          }
        }
        break;
      default:
        logger_.Error("[DBServer] Unexpected primary key type: " + 
                     std::to_string(static_cast<int>(pkField.field_type_)));
        return Status(DB_UNEXPECTED_ERROR, "unexpected Primary key type.");
    }
    
    logger_.Debug("[DBServer] Primary key validation completed successfully");
  }

  // Filter validation
  std::vector<query::expr::ExprNodePtr> expr_nodes;
  if (!filter.empty()) {
    logger_.Debug("[DBServer] Validating filter expression: " + filter);
  }
  Status expr_parse_status = vectordb::query::expr::Expr::ParseNodeFromStr(filter, expr_nodes, table->field_name_field_type_map_);
  if (!expr_parse_status.ok()) {
    logger_.Error("[DBServer] Filter validation failed: " + expr_parse_status.message());
    return expr_parse_status;
  }
  
  if (!filter.empty()) {
    logger_.Debug("[DBServer] Filter validation completed successfully");
  }
  
  logger_.Debug("[DBServer] Forwarding deletion request to table layer");
  auto result = table->Delete(pkList, filter, expr_nodes);
  
  if (result.ok()) {
    logger_.Debug("[DBServer] Table deletion completed successfully: " + result.message());
  } else {
    logger_.Error("[DBServer] Table deletion failed: " + result.message());
  }
  
  return result;
}

vectordb::query::expr::NodeType getAggregationNodeType(std::string &str, std::string& agg_nut_expr) {
  std::string temp_str = str;
  std::transform(temp_str.begin(), temp_str.end(), temp_str.begin(), [](unsigned char c) {
    return std::toupper(c);
  });
  if (temp_str.substr(0, 4) == "SUM(" && temp_str[temp_str.size() - 1] == ')') {
    agg_nut_expr = str.substr(4, str.size() - 5);
    return vectordb::query::expr::NodeType::SumAggregation;
  } else if (temp_str.substr(0, 4) == "MAX(" && temp_str[temp_str.size() - 1] == ')') {
    agg_nut_expr = str.substr(4, str.size() - 5);
    return vectordb::query::expr::NodeType::MaxAggregation;
  } else if (temp_str.substr(0, 4) == "MIN(" && temp_str[temp_str.size() - 1] == ')') {
    agg_nut_expr = str.substr(4, str.size() - 5);
    return vectordb::query::expr::NodeType::MinAggregation;
  } else if (temp_str.substr(0, 6) == "COUNT(" && temp_str[temp_str.size() - 1] == ')') {
    agg_nut_expr = "1";
    return vectordb::query::expr::NodeType::CountAggregation;
  } else {
    return vectordb::query::expr::NodeType::Invalid;
  }
}

Status preprocessFacets(
  vectordb::Json& facets_config,
  std::shared_ptr<TableMVP> table,
  std::vector<vectordb::engine::execution::FacetExecutor> &facet_executors
) {
int64_t facet_size = facets_config.GetSize();
  for (int64_t i = 0; i < facet_size; ++i) {
    auto facet_config = facets_config.GetArrayElement(i);
    auto groupby_config = facet_config.GetArray("group");
    bool global_groupby = false;
    std::vector<std::string> groupby_exprs;
    if (groupby_config.GetSize() == 0) {
      global_groupby = true;
      groupby_exprs.emplace_back("1");
    } else if (groupby_config.GetSize() > 1) {
      return Status(DB_UNEXPECTED_ERROR, "Multi-expression group is not supported yet: " + groupby_config.DumpToString());
    } else {
      groupby_exprs.emplace_back(groupby_config.GetArrayElement(0).GetString());
    }
    std::vector<std::vector<query::expr::ExprNodePtr>> groupby_expr_nodes;
    for (auto& expr: groupby_exprs) {
      std::vector<query::expr::ExprNodePtr> nodes;
      Status status = vectordb::query::expr::Expr::ParseNodeFromStr(expr, nodes, table->field_name_field_type_map_, false);
      if (!status.ok()) {
        return status;
      }
      auto data_type = nodes[nodes.size() - 1]->value_type;
      if (data_type == vectordb::query::expr::ValueType::INT ||
          data_type == vectordb::query::expr::ValueType::DOUBLE ||
          data_type == vectordb::query::expr::ValueType::STRING ||
          data_type == vectordb::query::expr::ValueType::BOOL) {
        groupby_expr_nodes.emplace_back(nodes);
      } else {
        return Status(DB_UNEXPECTED_ERROR, "Group by expression must be int, double, bool, or string.");
      }
    }
    std::vector<std::string> agg_exprs;
    std::vector<vectordb::query::expr::NodeType> agg_types;
    std::vector<std::vector<query::expr::ExprNodePtr>> agg_expr_nodes;
    auto agg_config = facet_config.GetArray("aggregate");
    size_t agg_size = agg_config.GetSize();
    if (agg_size == 0) {
      return Status(DB_UNEXPECTED_ERROR, "Aggregation is not specified.");
    }
    for (size_t j = 0; j < agg_size; ++j) {
      std::string agg_expr = agg_config.GetArrayElement(j).GetString();
      std::string agg_nut_expr;
      auto agg_type = getAggregationNodeType(agg_expr, agg_nut_expr);
      if (agg_type == vectordb::query::expr::NodeType::Invalid) {
        return Status(DB_UNEXPECTED_ERROR, "Invalid aggregation expression: " + agg_expr);
      }
      agg_exprs.emplace_back(agg_expr);
      agg_types.emplace_back(agg_type);
      std::vector<query::expr::ExprNodePtr> nodes;
      Status status = vectordb::query::expr::Expr::ParseNodeFromStr(agg_nut_expr, nodes, table->field_name_field_type_map_, false);
      if (!status.ok()) {
        return status;
      }
      agg_expr_nodes.emplace_back(nodes);
    }
    facet_executors.emplace_back(
      vectordb::engine::execution::FacetExecutor(
        global_groupby,
        groupby_exprs,
        groupby_expr_nodes,
        agg_types,
        agg_exprs,
        agg_expr_nodes
      )
    );
  }
  return Status::OK();
}

Status DBServer::Search(const std::string& db_name,
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
                        vectordb::Json& facets) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }

  // Check if field_name is empty, must contain only 1 vector field/index.
  if (field_name.empty()) {
    for (auto& field: table->table_schema_.fields_) {
      if (field.field_type_ == meta::FieldType::VECTOR_FLOAT ||
          field.field_type_ == meta::FieldType::VECTOR_DOUBLE ||
          field.field_type_ == meta::FieldType::SPARSE_VECTOR_FLOAT ||
          field.field_type_ == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
        if (!field_name.empty()) {
          return Status(INVALID_PAYLOAD, "Must specify queryField if there are more than 1 vector fields.");
        }
        field_name = field.name_;
      }
    }
  }

  // Filter validation
  std::vector<query::expr::ExprNodePtr> expr_nodes;
  Status expr_parse_status = vectordb::query::expr::Expr::ParseNodeFromStr(filter, expr_nodes, table->field_name_field_type_map_);
  if (!expr_parse_status.ok()) {
    return expr_parse_status;
  }

  // Facets validation
  std::vector<vectordb::engine::execution::FacetExecutor> facet_executors;
  Status facet_status = preprocessFacets(facets_config, table, facet_executors);
  if (!facet_status.ok()) {
    return facet_status;
  }

  return table->Search(field_name, query_fields, query_dimension, query_data, limit,
                       result, expr_nodes, with_distance, facet_executors, facets);
}

Status DBServer::SearchByContent(
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
      std::unordered_map<std::string, std::string> &headers) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  auto table = db->GetTable(table_name);
  if (table == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }

  // Check if field_name is empty, must contain only 1 vector field/index.
  if (index_name.empty()) {
    for (auto& field: table->table_schema_.fields_) {
      if (!field.is_index_field_) {
        continue;
      }
      if (field.field_type_ == meta::FieldType::VECTOR_FLOAT ||
          field.field_type_ == meta::FieldType::VECTOR_DOUBLE ||
          field.field_type_ == meta::FieldType::SPARSE_VECTOR_FLOAT ||
          field.field_type_ == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
        if (!index_name.empty()) {
          return Status(INVALID_PAYLOAD, "Must specify queryIndex if there are more than 1 vector indices.");
        }
        index_name = field.name_;
      }
    }
  }
  if (index_name.empty()) {
    return Status(INVALID_PAYLOAD, "There is no index in the table. Cannot search by query content.");
  }

  // Filter validation
  std::vector<query::expr::ExprNodePtr> expr_nodes;
  Status expr_parse_status = vectordb::query::expr::Expr::ParseNodeFromStr(filter, expr_nodes, table->field_name_field_type_map_);
  if (!expr_parse_status.ok()) {
    return expr_parse_status;
  }

  // Embed the query content.
  // Assume the embedding service already normalize the vectors for dense vectors.
  for (auto& index: table->table_schema_.indices_) {
    if (index.name_ != index_name) {
      continue;
    }
    auto& field = table->table_schema_.fields_[index.tgt_field_id_];

    engine::VectorPtr query_vec;
    size_t query_dimension = field.vector_dimension_;
    std::vector<engine::DenseVectorElement> denseQueryVec;
    auto status = embedding_service_->denseEmbedQuery(
      index.embedding_model_name_,
      query,
      denseQueryVec,
      query_dimension,
      headers,
      index.dimensions > 0
    );

    if (!status.ok()) {
      logger_.Error("Embedding service error: " + status.message());
      return status;
    }
    query_vec = denseQueryVec.data();

    // Facets validation
    std::vector<vectordb::engine::execution::FacetExecutor> facet_executors;
    Status facet_status = preprocessFacets(facets_config, table, facet_executors);
    if (!facet_status.ok()) {
      return facet_status;
    }

    return table->Search(index_name, query_fields, query_dimension, query_vec, limit,
                         result, expr_nodes, with_distance, facet_executors, facets);
  }

  return Status(INVALID_PAYLOAD, "Index not found: " + index_name);
}

Status DBServer::Project(const std::string& db_name,
                         const std::string& table_name,
                         std::vector<std::string>& query_fields,
                         vectordb::Json& primary_keys,
                         const std::string& filter,
                         const int64_t skip,
                         const int64_t limit,
                         vectordb::Json& project,
                         vectordb::Json& facets_config,
                         vectordb::Json& facets) {
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
  Status expr_parse_status = vectordb::query::expr::Expr::ParseNodeFromStr(filter, expr_nodes, table->field_name_field_type_map_);
  if (!expr_parse_status.ok()) {
    return expr_parse_status;
  }

  // Facets validation
  std::vector<vectordb::engine::execution::FacetExecutor> facet_executors;
  Status facet_status = preprocessFacets(facets_config, table, facet_executors);
  if (!facet_status.ok()) {
    return facet_status;
  }

  std::vector<int64_t> ids;
  std::vector<double> distances;
  return table->SearchByAttribute(query_fields, primary_keys, expr_nodes, skip, limit, project, facet_executors, facets);
}

void DBServer::StartWALFlushThread() {
  if (wal_flush_thread_started_.load()) {
    logger_.Info("WAL flush thread already started");
    return;
  }
  
  stop_wal_flush_thread_ = false;
  wal_flush_thread_started_ = true;
  wal_flush_thread_ = std::thread(&DBServer::WALFlushWorker, this);
  
  logger_.Info("WAL auto-flush thread started with interval: " + 
               std::to_string(vectordb::globalConfig.WALFlushInterval.load()) + " seconds");
}

void DBServer::StopWALFlushThread() {
  if (!wal_flush_thread_started_.load()) {
    return;
  }
  
  logger_.Info("Stopping WAL flush thread...");
  stop_wal_flush_thread_ = true;
  
  if (wal_flush_thread_.joinable()) {
    wal_flush_thread_.join();
  }
  
  wal_flush_thread_started_ = false;
  logger_.Info("WAL flush thread stopped");
}

Status DBServer::FlushAllWAL() {
  auto start_time = std::chrono::steady_clock::now();
  
  wal_flush_stats_total_flushes_++;
  
  // Get copy of databases to avoid holding lock during flush
  std::vector<std::shared_ptr<Database>> dbs_copy;
  {
    std::shared_lock<std::shared_mutex> lock(dbs_mutex_);
    dbs_copy = dbs_;
  }
  
  bool any_error = false;
  int total_dbs = 0;
  int successful_dbs = 0;
  
  // Flush WAL for each database
  for (auto db : dbs_copy) {
    if (db) {
      total_dbs++;
      auto status = db->FlushAllWAL();
      if (status.ok()) {
        successful_dbs++;
      } else {
        logger_.Error("Failed to flush WAL for database: " + status.message());
        any_error = true;
      }
    }
  }
  
  auto end_time = std::chrono::steady_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  
  // Update statistics
  if (any_error) {
    wal_flush_stats_failed_flushes_++;
  } else {
    wal_flush_stats_successful_flushes_++;
  }

  wal_flush_stats_last_flush_time_ = std::chrono::system_clock::now().time_since_epoch().count();
  wal_flush_stats_total_duration_ms_ += duration_ms;

  // Only log if there were databases to flush or if there was an error
  if (total_dbs > 0 || any_error) {
    logger_.Info("WAL flush completed: " + std::to_string(successful_dbs) + "/" +
                 std::to_string(total_dbs) + " databases flushed in " +
                 std::to_string(duration_ms) + "ms");
  } else {
    // Log less frequently when idle (every 10th flush)
    static int idle_flush_count = 0;
    if (++idle_flush_count % 10 == 0) {
      logger_.Debug("WAL flush: No databases to flush (idle)");
    }
  }
  
  if (any_error) {
    return Status(DB_UNEXPECTED_ERROR, "Some WAL flushes failed");
  }
  
  return Status::OK();
}

void DBServer::WALFlushWorker() {
  logger_.Info("WAL flush worker thread started");
  
  while (!stop_wal_flush_thread_.load()) {
    // Get flush interval from config (may change at runtime)
    int flush_interval_seconds = vectordb::globalConfig.WALFlushInterval.load();
    
    // Sleep for the configured interval
    for (int i = 0; i < flush_interval_seconds && !stop_wal_flush_thread_.load(); ++i) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    // Check if we should stop
    if (stop_wal_flush_thread_.load()) {
      break;
    }
    
    // Check if auto-flush is still enabled
    if (!vectordb::globalConfig.WALAutoFlush.load()) {
      logger_.Debug("WAL auto-flush disabled, skipping flush");
      continue;
    }

    // Perform the flush (logging is handled inside FlushAllWAL)
    auto status = FlushAllWAL();
    
    if (!status.ok()) {
      logger_.Warning("Periodic WAL flush encountered errors: " + status.message());
    }
  }
  
  // Perform final flush before exiting
  logger_.Info("Performing final WAL flush before thread exit...");
  FlushAllWAL();
  
  logger_.Info("WAL flush worker thread exited");
}

}  // namespace engine
}  // namespace vectordb
