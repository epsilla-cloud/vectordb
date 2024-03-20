#include "db/db_server.hpp"

#include <iostream>
#include <string>

#include "db/catalog/basic_meta_impl.hpp"
#include "db/execution/aggregation.hpp"

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
    if (db_name_to_id_map_.find(db_name) != db_name_to_id_map_.end()) {
      return Status(DB_UNEXPECTED_ERROR, "DB already exists: " + db_name);
    }

    auto db = std::make_shared<DBMVP>(db_schema, init_table_scale, is_leader_, embedding_service_, headers);
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

Status DBServer::ReleaseDB(const std::string& db_name) {
  // Release database from memory.
  auto it = db_name_to_id_map_.find(db_name);
  if (it == db_name_to_id_map_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  dbs_[it->second]->Release();
  return Status::OK();
}

Status DBServer::DumpDB(const std::string& db_name,
                        const std::string& db_catalog_path) {
  // Check that DB exists.
  auto it = db_name_to_id_map_.find(db_name);
  if (it == db_name_to_id_map_.end()) {
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
  std::shared_ptr<DBMVP> db = dbs_[it->second];
  return db->Dump(db_catalog_path);
}

Status DBServer::GetStatistics(const std::string& db_name,
                               vectordb::Json& response) {
  auto db = GetDB(db_name);
  if (db == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "DB not found: " + db_name);
  }
  for (auto& table: db->tables_) {
    vectordb::Json table_result;
    table_result.LoadFromString("{}");
    table_result.SetString("tableName", table->table_schema_.name_);
    table_result.SetInt("totalRecords", table->GetRecordCount());
    response.AddObjectToArray("result", table_result);
  }
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
  for (int64_t i = 0; i < dbs_.size(); ++i) {
    std::shared_ptr<DBMVP> db = dbs_[i];
    if (db != nullptr) {
      auto status = db->Rebuild();
      if (!status.ok()) {
        logger_.Error("Rebuild db to " + db->db_catalog_path_ + " failed.");
      }
    }
  }
  return Status::OK();
}

Status DBServer::SwapExecutors() {
  // Loop through all dbs and swap executors
  for (int64_t i = 0; i < dbs_.size(); ++i) {
    std::shared_ptr<DBMVP> db = dbs_[i];
    if (db != nullptr) {
      auto status = db->SwapExecutors();
      if (!status.ok()) {
        logger_.Error("Swap executors for db of " + db->db_catalog_path_ + " failed.");
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
      logger_.Info("No pk to delete.");
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
  Status expr_parse_status = vectordb::query::expr::Expr::ParseNodeFromStr(filter, expr_nodes, table->field_name_field_type_map_);
  if (!expr_parse_status.ok()) {
    return expr_parse_status;
  }

  return table->Delete(pkList, filter, expr_nodes);
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

}  // namespace engine
}  // namespace vectordb
