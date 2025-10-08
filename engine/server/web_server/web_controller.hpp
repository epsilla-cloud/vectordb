#pragma once

#include <iostream>
#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <cstdlib>

#include "db/catalog/basic_meta_impl.hpp"
#include "db/catalog/meta.hpp"
#include "db/database.hpp"
#include "db/db_server.hpp"
#include "db/table.hpp"
#include "db/vector.hpp"
#include "server/web_server/dto/db_dto.hpp"
#include "server/web_server/dto/status_dto.hpp"
#include "server/web_server/handler/web_request_handler.hpp"
#include "server/web_server/utils/util.hpp"
#include "utils/error.hpp"
#include "utils/path_validator.hpp"
#include "utils/json.hpp"
#include "utils/status.hpp"
#include "utils/constants.hpp"
#include "config/config.hpp"
#include "db/compaction_manager.hpp"
#include "db/memory_pool_manager.hpp"
#include "db/batch_insertion_optimizer.hpp"
#include "server/web_server/handler/worker_pool_stats_handler.hpp"

#define WEB_LOG_PREFIX "[Web] "

// Macro to record vector API request statistics
#define RECORD_VECTOR_API(api_type) \
  do { \
    globalConfig.Vector##api_type##RequestCount.fetch_add(1, std::memory_order_relaxed); \
    globalConfig.Vector##api_type##LastRequestTime.store( \
      std::chrono::duration_cast<std::chrono::seconds>( \
        std::chrono::system_clock::now().time_since_epoch() \
      ).count(), \
      std::memory_order_relaxed \
    ); \
  } while(0)

namespace vectordb {

extern Config globalConfig;

namespace server {
namespace web {

// Use configurable value instead of hardcoded 150000
inline int64_t GetInitTableScale() {
  return globalConfig.InitialTableCapacity.load(std::memory_order_acquire);
}

class WebController : public oatpp::web::server::api::ApiController {
 public:
  WebController(const std::shared_ptr<ObjectMapper>& objectMapper)
      : oatpp::web::server::api::ApiController(objectMapper) {
    db_server = std::make_shared<vectordb::engine::DBServer>();
  }

 public:
  static std::shared_ptr<WebController>
  createShared(OATPP_COMPONENT(std::shared_ptr<ObjectMapper>, objectMapper)) {
    return std::make_shared<WebController>(objectMapper);
  }

  std::shared_ptr<vectordb::engine::DBServer> db_server;
  std::shared_ptr<vectordb::engine::db::BatchInsertionOptimizer> batch_optimizer;

  // Initialize batch optimizer
  void InitBatchOptimizer() {
    if (!batch_optimizer) {
      vectordb::engine::db::BatchInsertionConfig config;
      config.max_batch_size = 1000;
      config.worker_threads = 4;
      config.enable_async_wal = true;
      config.enable_simd = true;
      batch_optimizer = std::make_shared<vectordb::engine::db::BatchInsertionOptimizer>(config);
    }
  }

/**
 *  Begin ENDPOINTs generation ('ApiController' codegen)
 */
#include OATPP_CODEGEN_BEGIN(ApiController)

  ADD_CORS(root)

  ENDPOINT("GET", "/", root) {
    auto response = createResponse(Status::CODE_200, "Welcome to Epsilla VectorDB.");
    response->putHeader(Header::CONTENT_TYPE, "text/plain");
    return response;
  }

  ADD_CORS(State)

  ENDPOINT("GET", "/state", State) {
    vectordb::Json response;
    response.LoadFromString("{}");

    response.SetInt("statusCode", 200);
    response.SetString("message", "Server is online!");

    // Add vector API statistics
    vectordb::Json apiStats;
    apiStats.LoadFromString("{}");

    // Insert stats
    vectordb::Json insertStats;
    insertStats.LoadFromString("{}");
    insertStats.SetInt("totalRequests", globalConfig.VectorInsertRequestCount.load(std::memory_order_relaxed));
    insertStats.SetInt("lastRequestTime", globalConfig.VectorInsertLastRequestTime.load(std::memory_order_relaxed));
    apiStats.SetObject("insert", insertStats);

    // Query stats
    vectordb::Json queryStats;
    queryStats.LoadFromString("{}");
    queryStats.SetInt("totalRequests", globalConfig.VectorQueryRequestCount.load(std::memory_order_relaxed));
    queryStats.SetInt("lastRequestTime", globalConfig.VectorQueryLastRequestTime.load(std::memory_order_relaxed));
    apiStats.SetObject("query", queryStats);

    // Delete stats
    vectordb::Json deleteStats;
    deleteStats.LoadFromString("{}");
    deleteStats.SetInt("totalRequests", globalConfig.VectorDeleteRequestCount.load(std::memory_order_relaxed));
    deleteStats.SetInt("lastRequestTime", globalConfig.VectorDeleteLastRequestTime.load(std::memory_order_relaxed));
    apiStats.SetObject("delete", deleteStats);

    // Update stats (reserved for future use)
    vectordb::Json updateStats;
    updateStats.LoadFromString("{}");
    updateStats.SetInt("totalRequests", globalConfig.VectorUpdateRequestCount.load(std::memory_order_relaxed));
    updateStats.SetInt("lastRequestTime", globalConfig.VectorUpdateLastRequestTime.load(std::memory_order_relaxed));
    apiStats.SetObject("update", updateStats);

    response.SetObject("vectorApiStats", apiStats);

    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(LoadDB)

  ENDPOINT("POST", "/api/load", LoadDB,
           BODY_STRING(String, body),
           REQUEST(std::shared_ptr<IncomingRequest>, request)) {
    vectordb::Json parsedBody;
    auto dto = StatusDto::createShared();
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }

    // Collect request headers
    std::unordered_map<std::string, std::string> headers;
    auto headerValue = request->getHeader(OPENAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[OPENAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(JINAAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[JINAAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(VOYAGEAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[VOYAGEAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(MIXEDBREADAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[MIXEDBREADAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(NOMIC_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[NOMIC_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(MISTRALAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[MISTRALAI_KEY_HEADER] = headerValue->c_str();
    }

    std::string raw_db_path = parsedBody.GetString("path");
    std::string raw_db_name = parsedBody.GetString("name");
    
    // Validate and sanitize the database path
    std::string db_path;
    vectordb::Status validation_status = vectordb::engine::PathValidator::ValidatePath(raw_db_path, db_path, false);
    if (!validation_status.ok()) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Invalid database path: " + validation_status.message();
      return createDtoResponse(Status::CODE_400, dto);
    }
    
    // Validate and sanitize the database name
    std::string db_name;
    validation_status = vectordb::engine::PathValidator::ValidateDbName(raw_db_name, db_name);
    if (!validation_status.ok()) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Invalid database name: " + validation_status.message();
      return createDtoResponse(Status::CODE_400, dto);
    }
    
    int64_t init_table_scale = GetInitTableScale();

    // DEPRECATED: vectorScale parameter is deprecated and ignored
    // The table will start with InitialTableCapacity (default 1000) and grow automatically
    // For performance-sensitive scenarios with known large datasets, configure via
    // INITIAL_TABLE_CAPACITY environment variable instead
    if (parsedBody.HasMember("vectorScale")) {
      // Log deprecation warning but do not use the value
      // init_table_scale remains as GetInitTableScale()
      // Users should use INITIAL_TABLE_CAPACITY environment variable for global configuration
    }
    bool wal_enabled = true;
    if (parsedBody.HasMember("walEnabled")) {
      wal_enabled = parsedBody.GetBool("walEnabled");
    }
    vectordb::Status status = db_server->LoadDB(db_name, db_path, init_table_scale, wal_enabled, headers);

    if (status.code() == DB_ALREADY_EXIST) {
      // DB already exists error.
      dto->statusCode = Status::CODE_409.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_409, dto);
    } else if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }

    dto->statusCode = Status::CODE_200.code;
    dto->message = "Load/Create " + db_name + " successfully.";
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(UnloadDB)

  ENDPOINT("POST", "api/{db_name}/unload", UnloadDB, PATH(String, db_name, "db_name")) {
    vectordb::Status status = db_server->UnloadDB(db_name);

    auto dto = StatusDto::createShared();
    if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }

    dto->statusCode = Status::CODE_200.code;
    dto->message = "Unload " + db_name + " successfully.";
    return createDtoResponse(Status::CODE_200, dto);
  }

  // Release DB memory for dbfactory.
  ADD_CORS(ReleaseDB)

  ENDPOINT("POST", "api/{db_name}/release", ReleaseDB, PATH(String, db_name, "db_name")) {
    vectordb::Status status = db_server->ReleaseDB(db_name);

    auto dto = StatusDto::createShared();
    if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }

    dto->statusCode = Status::CODE_200.code;
    dto->message = "Release " + db_name + " successfully.";
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(DropDB)

  ENDPOINT("DELETE", "api/{db_name}/drop", DropDB, PATH(String, db_name, "db_name")) {
    // Actual erase To be implemented.

    vectordb::Status status = db_server->UnloadDB(db_name);

    auto dto = StatusDto::createShared();
    if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }

    dto->statusCode = Status::CODE_200.code;
    dto->message = "Drop " + db_name + " successfully.";
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(DumpDB)

  ENDPOINT("POST", "/api/dump", DumpDB,
           BODY_STRING(String, body),
           REQUEST(std::shared_ptr<IncomingRequest>, request)) {
    vectordb::Json parsedBody;
    auto dto = StatusDto::createShared();
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }

    std::string db_path = parsedBody.GetString("path");
    std::string db_name = parsedBody.GetString("name");
    vectordb::Status status = db_server->DumpDB(db_name, db_path);

    if (status.code() == DB_NOT_FOUND) {
      // DB not found.
      dto->statusCode = Status::CODE_404.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_404, dto);
    } else if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }

    dto->statusCode = Status::CODE_200.code;
    dto->message = "Dump " + db_name + " successfully.";
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(CreateTable)

  ENDPOINT("POST", "/api/{db_name}/schema/tables", CreateTable,
           PATH(String, db_name, "db_name"),
           BODY_STRING(String, body)) {
    auto dto = StatusDto::createShared();

    vectordb::Json parsedBody;
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }

    bool return_table_id = false;
    if (parsedBody.HasMember("returnTableId")) {
      return_table_id = parsedBody.GetBool("returnTableId");
    }

    vectordb::engine::meta::TableSchema table_schema;
    if (!parsedBody.HasMember("name")) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Missing table name in your payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }
    
    // Validate and sanitize table name
    std::string raw_table_name = parsedBody.GetString("name");
    std::string sanitized_table_name;
    vectordb::Status validation_status = vectordb::engine::PathValidator::ValidateTableName(raw_table_name, sanitized_table_name);
    if (!validation_status.ok()) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Invalid table name: " + validation_status.message();
      return createDtoResponse(Status::CODE_400, dto);
    }
    table_schema.name_ = sanitized_table_name;

    if (!parsedBody.HasMember("fields")) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Missing fields in your payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }
    size_t fields_size = parsedBody.GetArraySize("fields");
    bool has_primary_key = false;
    for (size_t i = 0; i < fields_size; i++) {
      auto body_field = parsedBody.GetArrayElement("fields", i);
      vectordb::engine::meta::FieldSchema field;
      field.id_ = i;
      
      // Validate and sanitize field name
      std::string raw_field_name = body_field.GetString("name");
      std::string sanitized_field_name;
      validation_status = vectordb::engine::PathValidator::ValidateFieldName(raw_field_name, sanitized_field_name);
      if (!validation_status.ok()) {
        dto->statusCode = Status::CODE_400.code;
        dto->message = "Invalid field name '" + raw_field_name + "': " + validation_status.message();
        return createDtoResponse(Status::CODE_400, dto);
      }
      field.name_ = sanitized_field_name;
      if (body_field.HasMember("primaryKey")) {
        field.is_primary_key_ = body_field.GetBool("primaryKey");
        if (field.is_primary_key_) {
          if (has_primary_key) {
            dto->statusCode = Status::CODE_400.code;
            dto->message = "At most one field can be primary key.";
            return createDtoResponse(Status::CODE_400, dto);
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
          field.field_type_ == vectordb::engine::meta::FieldType::VECTOR_FLOAT) {
        if (!body_field.HasMember("dimensions")) {
          dto->statusCode = Status::CODE_400.code;
          dto->message = "Vector field must have dimensions.";
          return createDtoResponse(Status::CODE_400, dto);
        }
      }
      if (body_field.HasMember("dimensions")) {
        field.vector_dimension_ = body_field.GetInt("dimensions");
      }
      if (body_field.HasMember("metricType")) {
        std::string m_type;
        field.metric_type_ = engine::meta::GetMetricType(m_type.assign(body_field.GetString("metricType")));
        if (field.metric_type_ == vectordb::engine::meta::MetricType::UNKNOWN) {
          dto->statusCode = Status::CODE_400.code;
          dto->message = "invalid metric type: " + body_field.GetString("metricType");
          return createDtoResponse(Status::CODE_400, dto);
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

    if (parsedBody.HasMember("indices")) {
      size_t indices_size = parsedBody.GetArraySize("indices");
      for (size_t i = 0; i < indices_size; i++) {
        auto body_index = parsedBody.GetArrayElement("indices", i);
        vectordb::engine::meta::Index index;
        index.name_ = body_index.GetString("name");
        index.field_name_ = body_index.GetString("field");
        if (body_index.HasMember("model")) {
          index.embedding_model_name_ = body_index.GetString("model");
        } else {
          index.embedding_model_name_ = vectordb::engine::meta::DEFAULT_MODEL_NAME;
        }
        if (body_index.HasMember("dimensions")) {
          index.dimensions = body_index.GetInt("dimensions");
        }
        table_schema.indices_.push_back(index);
      }
    }

    size_t table_id;
    vectordb::Status status = db_server->CreateTable(db_name, table_schema, table_id);

    if (!status.ok()) {
      auto status_code = Status::CODE_500;
      if (status.code() == TABLE_ALREADY_EXISTS) {
        dto->statusCode = Status::CODE_409.code;
        status_code = Status::CODE_409;
      } else {
        dto->statusCode = Status::CODE_500.code;
      }
      dto->message = status.message();
      return createDtoResponse(status_code, dto);
    }

    if (return_table_id) {
      auto res_dto = ObjectRespDto::createShared();
      res_dto->statusCode = Status::CODE_200.code;
      res_dto->message = "Create " + table_schema.name_ + " successfully.";
      oatpp::parser::json::mapping::ObjectMapper mapper;
      res_dto->result = mapper.readFromString<oatpp::Any>("{\"tableId\": " + std::to_string(table_id) + "}");
      return createDtoResponse(Status::CODE_200, res_dto);
    } else {
      dto->statusCode = Status::CODE_200.code;
      dto->message = "Create " + table_schema.name_ + " successfully.";
      return createDtoResponse(Status::CODE_200, dto);
    }
  }

  ADD_CORS(DropTable)

  ENDPOINT("DELETE", "api/{db_name}/schema/tables/{table_name}", DropTable,
           PATH(String, db_name, "db_name"),
           PATH(String, table_name, "table_name")) {
    vectordb::Status status = db_server->DropTable(db_name, table_name);

    auto dto = StatusDto::createShared();
    if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }

    dto->statusCode = Status::CODE_200.code;
    dto->message = "Drop " + table_name + " from " + db_name + " successfully.";
    return createDtoResponse(Status::CODE_200, dto);
  }

  // TODO: implement with corresponding funtion later.
  ADD_CORS(DescribeSchema)

  ENDPOINT("GET", "/api/{db_name}/schema/tables/{table_name}/describe", DescribeSchema,
           PATH(String, db_name, "db_name"),
           PATH(String, table_name, "table_name")) {
    // vectordb::engine::meta::TableSchema table_schema;
    // vectordb::Status status = meta->GetTable(db_name, table_name, table_schema);
    // if (!status.ok()) {
    // return createResponse(Status::CODE_500, "Failed to get " + table_name + ".");
    // }

    auto dto = SchemaInfoDto::createShared();
    dto->message = "Get information of " + table_name + " from " + db_name + " successfully.";
    // dto->result = table_schema;
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(ListTables)

  ENDPOINT("GET", "/api/{db_name}/schema/tables/show", ListTables, PATH(String, db_name, "db_name")) {
    auto dto = TableListDto::createShared();
    auto res_dto = TableListDto::createShared();

    std::vector<std::string> table_names;
    vectordb::Status status = db_server->ListTables(db_name, table_names);

    if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }

    res_dto->statusCode = Status::CODE_200.code;
    res_dto->message = "Get all tables in " + db_name + " successfully.";
    res_dto->result = {};
    for (const auto& name : table_names) {
      res_dto->result->push_back(name);
    }
    return createDtoResponse(Status::CODE_200, res_dto);
  }

  // Add RESTful endpoint for listing tables
  ADD_CORS(ListTablesREST)
  
  ENDPOINT("GET", "/api/{db_name}/schema/tables", ListTablesREST, PATH(String, db_name, "db_name")) {
    auto dto = TableListDto::createShared();
    auto res_dto = TableListDto::createShared();

    std::vector<std::string> table_names;
    vectordb::Status status = db_server->ListTables(db_name, table_names);

    if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }

    res_dto->statusCode = Status::CODE_200.code;
    res_dto->message = "";
    res_dto->result = {};
    
    // Return table names directly (similar to the original endpoint but with simplified response)
    for (const auto& name : table_names) {
      res_dto->result->push_back(name);
    }
    return createDtoResponse(Status::CODE_200, res_dto);
  }

  ADD_CORS(InsertRecords)

  ENDPOINT("POST", "/api/{db_name}/data/insert", InsertRecords,
           PATH(String, db_name, "db_name"),
           BODY_STRING(String, body),
           REQUEST(std::shared_ptr<IncomingRequest>, request)) {
    RECORD_VECTOR_API(Insert);  // Record API statistics
    auto status_dto = StatusDto::createShared();
    vectordb::Json parsedBody;
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    if (!parsedBody.HasMember("table")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "table is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    if (!parsedBody.HasMember("data")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "data is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    std::string table_name = parsedBody.GetString("table");

    bool upsert = false;
    if (parsedBody.HasMember("upsert")) {
      upsert = parsedBody.GetBool("upsert");
    }

    // Collect request headers
    std::unordered_map<std::string, std::string> headers;
    auto headerValue = request->getHeader(OPENAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[OPENAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(JINAAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[JINAAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(VOYAGEAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[VOYAGEAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(MIXEDBREADAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[MIXEDBREADAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(NOMIC_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[NOMIC_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(MISTRALAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[MISTRALAI_KEY_HEADER] = headerValue->c_str();
    }

    auto data = parsedBody.GetArray("data");
    vectordb::Status insert_status = db_server->Insert(db_name, table_name, data, headers, upsert);
    if (!insert_status.ok()) {
      status_dto->statusCode = Status::CODE_500.code;
      status_dto->message = insert_status.message();
      return createDtoResponse(Status::CODE_500, status_dto);
    }
    vectordb::Json response;
    response.LoadFromString("{\"result\": " + insert_status.message() + "}");
    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Insert data to " + table_name + " successfully.");
    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(InsertRecordsOptimized)

  ENDPOINT("POST", "/api/{db_name}/data/insert_optimized", InsertRecordsOptimized,
           PATH(String, db_name, "db_name"),
           BODY_STRING(String, body),
           REQUEST(std::shared_ptr<IncomingRequest>, request)) {
    RECORD_VECTOR_API(Insert);  // Record API statistics
    auto status_dto = StatusDto::createShared();

    // Initialize batch optimizer if needed
    InitBatchOptimizer();

    vectordb::Json parsedBody;
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    if (!parsedBody.HasMember("table")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "table is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    if (!parsedBody.HasMember("records")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "records is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    std::string table_name = parsedBody.GetString("table");

    // Get table dimension (assuming we can get it from db_server)
    // For now, we'll extract it from the first record
    auto records = parsedBody.GetArray("records");
    if (records.GetSize() == 0) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "No records to insert.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    auto first_record = records.GetArrayElement(0);
    auto vec_array = first_record.GetArray("vec");
    size_t dimension = vec_array.GetSize();

    // Process with batch optimizer
    auto start_time = std::chrono::high_resolution_clock::now();
    auto optimized_batches = batch_optimizer->ProcessInsertionRequest(parsedBody, dimension);

    // Insert optimized batches
    size_t total_inserted = 0;
    for (const auto& batch : optimized_batches) {
      // Convert batch to JSON format expected by db_server
      vectordb::Json batch_data;
      batch_data.LoadFromString("[]");

      for (size_t i = 0; i < batch.count; ++i) {
        vectordb::Json record;
        record.SetInt("id", batch.ids[i]);

        std::vector<vectordb::Json> vec_array;
        vec_array.reserve(batch.dimension);
        for (size_t j = 0; j < batch.dimension; ++j) {
          vectordb::Json val;
          val.LoadFromString(std::to_string(batch.vectors[i * batch.dimension + j]));
          vec_array.push_back(val);
        }
        record.SetArray("vec", vec_array);

        batch_data.AddObjectToArray(record);
      }

      // Collect headers
      std::unordered_map<std::string, std::string> headers;

      // Insert batch
      bool upsert = parsedBody.HasMember("upsert") ? parsedBody.GetBool("upsert") : false;
      vectordb::Status insert_status = db_server->Insert(db_name, table_name, batch_data, headers, upsert);

      if (!insert_status.ok()) {
        status_dto->statusCode = Status::CODE_500.code;
        status_dto->message = insert_status.message();
        return createDtoResponse(Status::CODE_500, status_dto);
      }

      total_inserted += batch.count;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time).count();

    // Get metrics
    auto metrics = batch_optimizer->GetMetrics();

    vectordb::Json response;
    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Optimized insert completed successfully");
    response.SetInt("inserted", total_inserted);
    response.SetInt("duration_ms", duration_ms);
    response.SetDouble("vectors_per_second", metrics.avg_vectors_per_second);

    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(InsertRecordsPrepare)

  ENDPOINT("POST", "/api/{db_name}/data/insertprepare", InsertRecordsPrepare,
           PATH(String, db_name, "db_name"),
           BODY_STRING(String, body)) {
    RECORD_VECTOR_API(Insert);  // Record API statistics
    auto status_dto = StatusDto::createShared();

    vectordb::Json parsedBody;
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    if (!parsedBody.HasMember("table")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "table is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    vectordb::Json pks;
    pks.LoadFromString("[]");
    if (parsedBody.HasMember("primaryKeys")) {
      pks = parsedBody.GetArray("primaryKeys");
    }

    std::string table_name = parsedBody.GetString("table");

    vectordb::Json result;
    vectordb::Status insert_status = db_server->InsertPrepare(db_name, table_name, pks, result);

    if (!insert_status.ok()) {
      status_dto->statusCode = Status::CODE_500.code;
      status_dto->message = insert_status.message();
      return createDtoResponse(Status::CODE_500, status_dto);
    }

    vectordb::Json response;
    response.LoadFromString("{}");
    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "");
    response.SetObject("result", result);
    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(DeleteRecordsByPK)

  ENDPOINT("POST", "/api/{db_name}/data/delete", DeleteRecordsByPK,
           PATH(String, db_name, "db_name"),
           BODY_STRING(String, body)) {
    RECORD_VECTOR_API(Delete);  // Record API statistics
    auto dto = StatusDto::createShared();
    vectordb::Json requestBody;
    auto valid = requestBody.LoadFromString(body);

    if (!valid) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }

    if (!requestBody.HasMember("table")) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Missing table name in your payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }

    vectordb::Json pks;
    pks.LoadFromString("[]");
    size_t pk_count = 0;
    if (requestBody.HasMember("primaryKeys")) {
      pks = requestBody.GetArray("primaryKeys");
      pk_count = pks.GetSize();
      if (pk_count == 0) {
        dto->statusCode = Status::CODE_400.code;
        dto->message = "If the primaryKeys field is provided, it cannot be empty.";
        return createDtoResponse(Status::CODE_400, dto);
      }
    }
    std::string filter;
    if (requestBody.HasMember("filter")) {
      filter = requestBody.GetString("filter");
    }

    if (!requestBody.HasMember("primaryKeys") && !requestBody.HasMember("filter")) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Must provide primary key list or filter in your payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }

    auto table = requestBody.GetString("table");
    
    // Log deletion request details
    std::string log_msg = "[WEB] Starting deletion request for db=" + db_name + 
                         ", table=" + table;
    if (pk_count > 0) {
      log_msg += ", primaryKeys=" + std::to_string(pk_count) + " items";
    }
    if (!filter.empty()) {
      log_msg += ", filter=[" + filter + "]";
    }
    OATPP_LOGD("WebController", "%s", log_msg.c_str());
    
    auto status = db_server->Delete(db_name, table, pks, filter);
    if (status.ok()) {
      // Log successful deletion
      OATPP_LOGD("WebController", "[WEB] Deletion completed successfully for db=%s, table=%s, result=%s", 
                 db_name->c_str(), table.c_str(), status.message().c_str());
      
      vectordb::Json response;
      response.LoadFromString("{\"result\": " + status.message() + "}");
      response.SetInt("statusCode", Status::CODE_200.code);
      response.SetString("message", "Delete data from " + table + " successfully.");
      auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
    } else {
      // Log deletion failure
      OATPP_LOGE("WebController", "[WEB] Deletion failed for db=%s, table=%s, error=%s", 
                 db_name->c_str(), table.c_str(), status.message().c_str());
      
      dto->statusCode = Status::CODE_400.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_400, dto);
    }
  }

  // TODO: implement with corresponding function later.
  ADD_CORS(LoadCSV)

  ENDPOINT("POST", "/api/{db_name}/data/load", LoadCSV,
           PATH(String, db_name, "db_name"),
           BODY_STRING(String, body)) {
    auto dto = StatusDto::createShared();
    dto->statusCode = Status::CODE_200.code;
    dto->message = "Loading csv to " + db_name + ".";
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(GetStatistics)

  ENDPOINT("GET", "/api/{db_name}/statistics", GetStatistics,
           PATH(String, db_name, "db_name")) {
    vectordb::Json response;
    response.LoadFromString("{\"result\": []}");
    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "");
    vectordb::Status statistics_status = db_server->GetStatistics(db_name, response);

    if (!statistics_status.ok()) {
      auto status_dto = StatusDto::createShared();
      status_dto->statusCode = Status::CODE_500.code;
      status_dto->message = statistics_status.message();
      return createDtoResponse(Status::CODE_500, status_dto);
    }

    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }
  
  // Get record count for database and/or table
  ADD_CORS(GetRecordCount)
  
  ENDPOINT("GET", "/api/{db_name}/records/count", GetRecordCount,
           PATH(String, db_name, "db_name"),
           QUERY(String, table_name, "table", "")) {
    vectordb::Json response;
    response.LoadFromString("{}");
    
    vectordb::Status count_status = db_server->GetRecordCount(
        db_name, 
        table_name ? std::string(table_name) : "", 
        response);

    if (!count_status.ok()) {
      auto status_dto = StatusDto::createShared();
      status_dto->statusCode = Status::CODE_500.code;
      status_dto->message = count_status.message();
      return createDtoResponse(Status::CODE_500, status_dto);
    }

    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Success");
    
    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }
  
  // Get record count for all databases
  ADD_CORS(GetAllRecordCount)
  
  ENDPOINT("GET", "/api/records/count", GetAllRecordCount,
           QUERY(String, table_name, "table", "")) {
    vectordb::Json response;
    response.LoadFromString("{}");
    
    vectordb::Status count_status = db_server->GetRecordCount(
        "", 
        table_name ? std::string(table_name) : "", 
        response);

    if (!count_status.ok()) {
      auto status_dto = StatusDto::createShared();
      status_dto->statusCode = Status::CODE_500.code;
      status_dto->message = count_status.message();
      return createDtoResponse(Status::CODE_500, status_dto);
    }

    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Success");
    
    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(Query)

  ENDPOINT("POST", "/api/{db_name}/data/query", Query,
           PATH(String, db_name, "db_name"),
           BODY_STRING(String, body),
           REQUEST(std::shared_ptr<IncomingRequest>, request)) {
    RECORD_VECTOR_API(Query);  // Record API statistics
    auto status_dto = StatusDto::createShared();

    vectordb::Json parsedBody;
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    std::string table_name;
    if (!parsedBody.HasMember("table")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "table is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    } else {
      table_name = parsedBody.GetString("table");
    }

    // If field_name is empty, in query there must only have 1 vector field / index.
    std::string field_name = "";
    if (parsedBody.HasMember("queryField")) {
      field_name = parsedBody.GetString("queryField");
      if (parsedBody.HasMember("queryIndex")) {
        status_dto->statusCode = Status::CODE_400.code;
        status_dto->message = "Can only specify either queryField or queryIndex, but not both.";
        return createDtoResponse(Status::CODE_400, status_dto);
      }
    }
    if (parsedBody.HasMember("queryIndex")) {
      field_name = parsedBody.GetString("queryIndex");
    }

    if (!parsedBody.HasMember("limit")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "limit is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    std::vector<std::string> query_fields;
    if (parsedBody.HasMember("response")) {
      size_t field_size = parsedBody.GetArraySize("response");
      for (size_t i = 0; i < field_size; i++) {
        auto field = parsedBody.GetArrayElement("response", i);
        query_fields.push_back(field.GetString());
      }
    }

    int64_t limit = parsedBody.GetInt("limit");

    std::string filter;
    if (parsedBody.HasMember("filter")) {
      filter = parsedBody.GetString("filter");
    }

    bool with_distance = false;
    if (parsedBody.HasMember("withDistance")) {
      with_distance = parsedBody.GetBool("withDistance");
    }

    // Collect request headers
    std::unordered_map<std::string, std::string> headers;
    auto headerValue = request->getHeader(OPENAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[OPENAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(JINAAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[JINAAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(VOYAGEAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[VOYAGEAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(MIXEDBREADAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[MIXEDBREADAI_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(NOMIC_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[NOMIC_KEY_HEADER] = headerValue->c_str();
    }
    headerValue = request->getHeader(MISTRALAI_KEY_HEADER);
    if (headerValue != nullptr) {
      headers[MISTRALAI_KEY_HEADER] = headerValue->c_str();
    }

    vectordb::Json facetsConfig;
    if (parsedBody.HasMember("facets")) {
      facetsConfig = parsedBody.GetArray("facets");
    } else {
      facetsConfig.LoadFromString("[]");
    }

    vectordb::Json projects;
    vectordb::Json facets;
    vectordb::Status search_status;
    if (parsedBody.HasMember("queryVector")) {
      // Query by provided vector.
      engine::VectorPtr query;
      size_t dense_vector_size = 0;  // used by dense vector only
      std::vector<engine::DenseVectorElement> denseQueryVec;
      auto querySparseVecPtr = std::make_shared<engine::SparseVector>();
      auto queryVecJson = parsedBody.Get("queryVector");
      if (queryVecJson.IsArray()) {
        dense_vector_size = queryVecJson.GetSize();
        denseQueryVec.resize(dense_vector_size);
        for (size_t i = 0; i < dense_vector_size; i++) {
          auto elem = queryVecJson.GetArrayElement(i);
          denseQueryVec[i] = static_cast<engine::DenseVectorElement>(elem.GetDouble());
        }
        query = denseQueryVec.data();
      } else if (queryVecJson.IsObject()) {
        if (!queryVecJson.HasMember("indices")) {
          status_dto->statusCode = Status::CODE_400.code;
          status_dto->message = "missing indices field for sparse vector";
          return createDtoResponse(Status::CODE_400, status_dto);
        }
        if (!queryVecJson.HasMember("values")) {
          status_dto->statusCode = Status::CODE_400.code;
          status_dto->message = "missing values field for sparse vector";
          return createDtoResponse(Status::CODE_400, status_dto);
        }
        auto numIdxElem = queryVecJson.GetArray("indices").GetSize();
        auto numValElem = queryVecJson.GetArray("values").GetSize();
        if (numIdxElem != numValElem) {
          status_dto->statusCode = Status::CODE_400.code;
          status_dto->message = "sparse vector indices and values array are of different sizes.";
          return createDtoResponse(Status::CODE_400, status_dto);
        }
        querySparseVecPtr->resize(numIdxElem);
        for (size_t i = 0; i < numIdxElem; i++) {
          auto idx = queryVecJson.GetArrayElement("indices", i);
          querySparseVecPtr->at(i).index = idx.GetInt();
          auto val = queryVecJson.GetArrayElement("values", i);
          querySparseVecPtr->at(i).value = static_cast<float>(val.GetDouble());
        }
        query = querySparseVecPtr;
      }

      // Search by vector.
      search_status = db_server->Search(
        db_name,
        table_name,
        field_name,
        query_fields,
        dense_vector_size,
        query,
        limit,
        projects,
        filter,
        with_distance,
        facetsConfig,
        facets);
    } else if (parsedBody.HasMember("query")) {
      // Query by provided content.
      std::string query_content = parsedBody.GetString("query");
      search_status = db_server->SearchByContent(
        db_name,
        table_name,
        field_name,
        query_fields,
        query_content,
        limit,
        projects,
        filter,
        with_distance,
        facetsConfig,
        facets,
        headers);
    } else {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "query or queryVector must be provided.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    if (!search_status.ok()) {
      oatpp::web::protocol::http::Status status;
      switch (search_status.code()) {
        case INVALID_EXPR:
        case INVALID_PAYLOAD:
          status = Status::CODE_400;
          break;
        case NOT_IMPLEMENTED_ERROR:
          status = Status::CODE_501;
          break;
        default:
          status = Status::CODE_500;
          break;
      }
      status_dto->statusCode = status.code;
      status_dto->message = search_status.message();
      return createDtoResponse(status, status_dto);
    }

    vectordb::Json response;
    response.LoadFromString("{}");
    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Query search successfully.");
    if (facetsConfig.GetSize() == 0) {
      // Projection only
      response.SetObject("result", projects);
    } else if (query_fields.size() == 0) {
      // No response given, only facets
      response.SetObject("result", facets);
    } else {
      // Both projection and facets
      vectordb::Json final_result;
      final_result.LoadFromString("{}");
      final_result.SetObject("records", projects);
      final_result.SetObject("facets", facets);
      response.SetObject("result", final_result);
    }
    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(Project)

  ENDPOINT("POST", "/api/{db_name}/data/get", Project,
           PATH(String, db_name, "db_name"),
           BODY_STRING(String, body)) {
    auto status_dto = StatusDto::createShared();

    vectordb::Json parsedBody;
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    if (!parsedBody.HasMember("table")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "table is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    std::string table_name = parsedBody.GetString("table");
    std::vector<std::string> query_fields;
    if (parsedBody.HasMember("response")) {
      size_t field_size = parsedBody.GetArraySize("response");
      for (size_t i = 0; i < field_size; i++) {
        auto field = parsedBody.GetArrayElement("response", i);
        query_fields.push_back(field.GetString());
      }
    }

    std::string filter;
    if (parsedBody.HasMember("filter")) {
      filter = parsedBody.GetString("filter");
    }

    // Set a large number (larger than maximal segment size)
    // so by default project out everything.
    int64_t limit = 1 << 30;
    if (parsedBody.HasMember("limit")) {
      limit = parsedBody.GetInt("limit");
    }

    int64_t skip = 0;
    if (parsedBody.HasMember("skip")) {
      skip = parsedBody.GetInt("skip");
    }

    vectordb::Json pks;
    pks.LoadFromString("[]");
    if (parsedBody.HasMember("primaryKeys")) {
      pks = parsedBody.GetArray("primaryKeys");
      if (pks.GetSize() == 0) {
        status_dto->statusCode = Status::CODE_400.code;
        status_dto->message = "If the primaryKeys field is provided, it cannot be empty.";
        return createDtoResponse(Status::CODE_400, status_dto);
      }
    }

    vectordb::Json facetsConfig;
    if (parsedBody.HasMember("facets")) {
      facetsConfig = parsedBody.GetArray("facets");
    } else {
      facetsConfig.LoadFromString("[]");
    }

    vectordb::Json projects;
    vectordb::Json facets;

    vectordb::Status get_status = db_server->Project(
        db_name, table_name, query_fields, pks, filter, skip, limit, projects, facetsConfig, facets);
    if (!get_status.ok()) {
      status_dto->statusCode = Status::CODE_500.code;
      status_dto->message = get_status.message();
      return createDtoResponse(Status::CODE_500, status_dto);
    }

    vectordb::Json response;
    response.LoadFromString("{}");
    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Query get successfully.");
    if (facetsConfig.GetSize() == 0) {
      // Projection only
      response.SetObject("result", projects);
    } else if (query_fields.size() == 0) {
      // No response given, only facets
      response.SetObject("result", facets);
    } else {
      // Both projection and facets
      vectordb::Json final_result;
      final_result.LoadFromString("{}");
      final_result.SetObject("records", projects);
      final_result.SetObject("facets", facets);
      response.SetObject("result", final_result);
    }
    
    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(Rebuild)

  ENDPOINT("POST", "/api/rebuild", Rebuild) {
    vectordb::Status status = db_server->RebuildOndemand();

    auto dto = StatusDto::createShared();
    if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }

    dto->statusCode = Status::CODE_200.code;
    dto->message = "Rebuild finished!";
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(Compact)

  ENDPOINT("POST", "/api/{db_name}/compact", Compact, PATH(String, db_name, "db_name"), BODY_STRING(String, body)) {
    vectordb::Json parsedBody;
    auto dto = StatusDto::createShared();
    
    // Handle empty body
    if (body->empty()) {
      parsedBody.LoadFromString("{}");
    } else {
      auto valid = parsedBody.LoadFromString(body);
      if (!valid) {
        dto->statusCode = Status::CODE_400.code;
        dto->message = "Invalid JSON payload.";
        return createDtoResponse(Status::CODE_400, dto);
      }
    }
    
    std::string table_name = "";
    if (parsedBody.HasMember("tableName")) {
      table_name = parsedBody.GetString("tableName");
    }
    
    double threshold = 0.3;  // Default 30% deleted records threshold
    if (parsedBody.HasMember("threshold")) {
      threshold = parsedBody.GetDouble("threshold");
      if (threshold < 0.0 || threshold > 1.0) {
        dto->statusCode = Status::CODE_400.code;
        dto->message = "Threshold must be between 0.0 and 1.0";
        return createDtoResponse(Status::CODE_400, dto);
      }
    }
    
    vectordb::Status status = db_server->Compact(db_name, table_name, threshold);
    if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }
    dto->statusCode = Status::CODE_200.code;
    dto->message = status.message();
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(CompactAll)

  ENDPOINT("POST", "/api/compact", CompactAll, BODY_STRING(String, body)) {
    vectordb::Json parsedBody;
    auto dto = StatusDto::createShared();
    
    // Handle empty body
    if (body->empty()) {
      parsedBody.LoadFromString("{}");
    } else {
      auto valid = parsedBody.LoadFromString(body);
      if (!valid) {
        dto->statusCode = Status::CODE_400.code;
        dto->message = "Invalid JSON payload.";
        return createDtoResponse(Status::CODE_400, dto);
      }
    }
    
    double threshold = 0.3;  // Default 30% deleted records threshold
    if (parsedBody.HasMember("threshold")) {
      threshold = parsedBody.GetDouble("threshold");
      if (threshold < 0.0 || threshold > 1.0) {
        dto->statusCode = Status::CODE_400.code;
        dto->message = "Threshold must be between 0.0 and 1.0";
        return createDtoResponse(Status::CODE_400, dto);
      }
    }
    
    vectordb::Status status = db_server->Compact("", "", threshold);
    if (!status.ok()) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = status.message();
      return createDtoResponse(Status::CODE_500, dto);
    }
    dto->statusCode = Status::CODE_200.code;
    dto->message = status.message();
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(SetLeader)

  ENDPOINT("POST", "api/setleader", SetLeader, BODY_STRING(String, body)) {
    vectordb::Json parsedBody;
    auto dto = StatusDto::createShared();
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }

    bool is_leader = parsedBody.GetBool("leader");
    db_server->SetLeader(is_leader);

    dto->statusCode = Status::CODE_200.code;
    dto->message = std::string("Set leader as ") + (is_leader ? "true" : "false") + " successfully.";
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(GetConfig)

  ENDPOINT_INFO(GetConfig) {
    info->summary = "Get Runtime Configuration";
    info->description = "Retrieve the current runtime configuration of VectorDB including thread settings, queue sizes, deletion mode, WAL configuration, and environment variables";
    info->addResponse<Object<StatusDto>>(Status::CODE_200, "application/json", "Configuration retrieved successfully");
    info->addTag("Configuration");
  }
  ENDPOINT("GET", "api/config", GetConfig) {
    vectordb::Json response;
    response.LoadFromString("{}");

    // Get runtime configuration
    vectordb::Json config = globalConfig.getConfigAsJson();

    // Add environment variable information
    vectordb::Json envVars;
    envVars.LoadFromString("{}");

    // Check and add environment variables
    const char* soft_delete_env = std::getenv("SOFT_DELETE");
    envVars.SetString("SOFT_DELETE", soft_delete_env ? soft_delete_env : "(not set)");

    const char* wal_interval_env = std::getenv("WAL_FLUSH_INTERVAL");
    envVars.SetString("WAL_FLUSH_INTERVAL", wal_interval_env ? wal_interval_env : "(not set)");

    const char* wal_auto_env = std::getenv("WAL_AUTO_FLUSH");
    envVars.SetString("WAL_AUTO_FLUSH", wal_auto_env ? wal_auto_env : "(not set)");

    const char* threads_env = std::getenv("EPSILLA_INTRA_QUERY_THREADS");
    envVars.SetString("EPSILLA_INTRA_QUERY_THREADS", threads_env ? threads_env : "(not set)");

    // Build response
    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Get configuration successfully.");
    response.SetObject("config", config);
    response.SetObject("environmentVariables", envVars);

    // Add hardware info
    vectordb::Json hardwareInfo;
    hardwareInfo.LoadFromString("{}");
    unsigned int hw_threads = std::thread::hardware_concurrency();
    hardwareInfo.SetInt("hardwareThreads", hw_threads ? hw_threads : 4);
    response.SetObject("hardware", hardwareInfo);

    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(GetMemoryStats)

  ENDPOINT_INFO(GetMemoryStats) {
    info->summary = "Get Memory Statistics";
    info->description = "Returns current memory usage statistics and allocation strategy";
  }

  ENDPOINT("GET", "api/memory/stats", GetMemoryStats) {
    // Get memory statistics from the pool manager
    auto& memoryManager = vectordb::engine::MemoryPoolManager::GetInstance();
    vectordb::Json response = memoryManager.GetStatsAsJson();

    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(UpdateConfig)

  ENDPOINT_INFO(UpdateConfig) {
    info->summary = "Update Runtime Configuration";
    info->description = "Update the runtime configuration of VectorDB. Supports dynamic configuration changes for thread counts, queue sizes, and other operational parameters";
    info->addConsumes<String>("application/json");
    info->addResponse<Object<StatusDto>>(Status::CODE_200, "application/json", "Configuration updated successfully");
    info->addResponse<Object<StatusDto>>(Status::CODE_400, "application/json", "Invalid configuration payload");
    info->addResponse<Object<StatusDto>>(Status::CODE_500, "application/json", "Configuration update failed");
    info->addTag("Configuration");
  }
  ENDPOINT("POST", "api/config", UpdateConfig, BODY_STRING(String, body)) {
    vectordb::Json parsedBody;
    auto dto = StatusDto::createShared();
    auto valid = parsedBody.LoadFromString(body);
    if (!valid) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Invalid payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }

    try {
      bool needSwapExecutors = false;
      globalConfig.updateConfig(parsedBody, needSwapExecutors);
      if (needSwapExecutors) {
        // Swap executors if necessary.
        db_server->SwapExecutors();
      }
    } catch (std::exception& ex) {
      dto->statusCode = Status::CODE_500.code;
      dto->message = std::string(ex.what());
      return createDtoResponse(Status::CODE_500, dto);
    }

    dto->statusCode = Status::CODE_200.code;
    dto->message = std::string("Config updated successfully.");
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(GetStats)

  ENDPOINT_INFO(GetStats) {
    info->summary = "Get Compaction Statistics";
    info->description = "Retrieve compaction statistics including deleted vector ratios, compaction counts, and memory freed";
    info->addResponse<String>(Status::CODE_200, "application/json", "Statistics retrieved successfully");
    info->addTag("Compaction");
  }
  ENDPOINT("GET", "api/stats", GetStats) {
    vectordb::Json response;
    response.LoadFromString("{}");

    // Get compaction statistics from CompactionManager
    auto& compactionMgr = vectordb::engine::CompactionManager::GetInstance();
    vectordb::Json stats = compactionMgr.GetAllStatsAsJson();

    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Get statistics successfully.");
    response.SetObject("stats", stats);

    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(TriggerCompaction)

  ENDPOINT_INFO(TriggerCompaction) {
    info->summary = "Trigger Manual Compaction";
    info->description = "Manually trigger compaction for a specific table or all tables to reclaim memory from soft-deleted vectors";
    info->addConsumes<String>("application/json");
    info->addResponse<Object<StatusDto>>(Status::CODE_200, "application/json", "Compaction triggered successfully");
    info->addResponse<Object<StatusDto>>(Status::CODE_400, "application/json", "Invalid request");
    info->addResponse<Object<StatusDto>>(Status::CODE_500, "application/json", "Compaction failed");
    info->addTag("Compaction");
  }
  ENDPOINT("POST", "api/compact", TriggerCompaction, BODY_STRING(String, body)) {
    auto dto = StatusDto::createShared();

    if (!body || body->length() == 0) {
      // Compact all tables if no body provided
      auto& compactionMgr = vectordb::engine::CompactionManager::GetInstance();
      auto status = compactionMgr.CompactAllTables();

      dto->statusCode = status.ok() ? Status::CODE_200.code : Status::CODE_500.code;
      dto->message = status.ok() ? "All tables compacted successfully" : status.message();
    } else {
      // Parse body for specific table
      vectordb::Json parsedBody;
      auto valid = parsedBody.LoadFromString(body);
      if (!valid) {
        dto->statusCode = Status::CODE_400.code;
        dto->message = "Invalid JSON payload";
        return createDtoResponse(Status::CODE_400, dto);
      }

      std::string table_name = parsedBody.GetString("table");
      auto& compactionMgr = vectordb::engine::CompactionManager::GetInstance();

      if (table_name.empty()) {
        auto status = compactionMgr.CompactAllTables();
        dto->statusCode = status.ok() ? Status::CODE_200.code : Status::CODE_500.code;
        dto->message = status.ok() ? "All tables compacted successfully" : status.message();
      } else {
        auto status = compactionMgr.CompactTable(table_name);
        dto->statusCode = status.ok() ? Status::CODE_200.code : Status::CODE_500.code;
        dto->message = status.ok() ? "Table " + table_name + " compacted successfully" : status.message();
      }
    }

    return createDtoResponse(dto->statusCode == 200 ? Status::CODE_200 : Status::CODE_500, dto);
  }

  ADD_CORS(GetWALStats)

  ENDPOINT_INFO(GetWALStats) {
    info->summary = "Get WAL Flush Statistics";
    info->description = "Retrieve Write-Ahead Log flush statistics including total flushes, success/failure counts, and average flush duration";
    info->addResponse<String>(Status::CODE_200, "application/json", "WAL statistics retrieved successfully");
    info->addTag("Monitoring");
  }
  ENDPOINT("GET", "api/wal/stats", GetWALStats) {
    vectordb::Json response;
    response.LoadFromString("{}");

    try {
      auto wal_stats = db_server->GetWALFlushStats();

      response.SetInt("statusCode", Status::CODE_200.code);
      response.SetString("message", "WAL statistics retrieved successfully");

      vectordb::Json stats;
      stats.LoadFromString("{}");
      stats.SetInt("totalFlushes", wal_stats.total_flushes);
      stats.SetInt("successfulFlushes", wal_stats.successful_flushes);
      stats.SetInt("failedFlushes", wal_stats.failed_flushes);
      stats.SetInt("lastFlushTime", wal_stats.last_flush_time);
      stats.SetInt("totalFlushDurationMs", wal_stats.total_flush_duration_ms);

      // Calculate derived metrics
      double failure_rate = 0.0;
      if (wal_stats.total_flushes > 0) {
        failure_rate = (double)wal_stats.failed_flushes / wal_stats.total_flushes * 100.0;
      }
      stats.SetDouble("failureRatePercent", failure_rate);

      double avg_duration_ms = 0.0;
      if (wal_stats.successful_flushes > 0) {
        avg_duration_ms = (double)wal_stats.total_flush_duration_ms / wal_stats.successful_flushes;
      }
      stats.SetDouble("avgFlushDurationMs", avg_duration_ms);

      response.SetObject("stats", stats);

    } catch (const std::exception& e) {
      response.SetInt("statusCode", Status::CODE_500.code);
      response.SetString("message", std::string("Failed to retrieve WAL statistics: ") + e.what());
    }

    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(GetWorkerPoolStats)

  ENDPOINT_INFO(GetWorkerPoolStats) {
    info->summary = "Get Worker Pool Statistics";
    info->description = "Retrieve CPU and IO worker pool statistics including tasks submitted/completed/failed, queue sizes, and performance metrics";
    info->addResponse<String>(Status::CODE_200, "application/json", "Worker pool statistics retrieved successfully");
    info->addTag("Monitoring");
  }
  ENDPOINT("GET", "api/workers/stats", GetWorkerPoolStats) {
    std::string stats_json = vectordb::server::web::WorkerPoolStatsHandler::GetStats();

    auto httpResponse = createResponse(Status::CODE_200, stats_json);
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(GetVectorStats)

  ENDPOINT_INFO(GetVectorStats) {
    info->summary = "Get Vector Statistics";
    info->description = "Retrieve vector statistics for a specific table including total vectors, active vectors, deleted vectors, and deletion ratio";
    info->pathParams["db_name"].description = "Database name";
    info->pathParams["table_name"].description = "Table name";
    info->addResponse<String>(Status::CODE_200, "application/json", "Vector statistics retrieved successfully");
    info->addResponse<String>(Status::CODE_404, "application/json", "Database or table not found");
    info->addTag("Monitoring");
  }
  ENDPOINT("GET", "api/{db_name}/{table_name}/vectors/stats", GetVectorStats,
           PATH(String, db_name, "db_name"),
           PATH(String, table_name, "table_name")) {
    vectordb::Json response;
    response.LoadFromString("{}");

    try {
      auto db = db_server->GetDB(db_name);
      if (!db) {
        response.SetInt("statusCode", Status::CODE_404.code);
        response.SetString("message", "Database not found: " + db_name);
        auto httpResponse = createResponse(Status::CODE_404, response.DumpToString());
        httpResponse->putHeader("Content-Type", "application/json");
        return httpResponse;
      }

      auto table = db->GetTable(table_name);
      if (!table) {
        response.SetInt("statusCode", Status::CODE_404.code);
        response.SetString("message", "Table not found: " + table_name);
        auto httpResponse = createResponse(Status::CODE_404, response.DumpToString());
        httpResponse->putHeader("Content-Type", "application/json");
        return httpResponse;
      }

      auto stats = table->GetVectorStats();

      response.SetInt("statusCode", Status::CODE_200.code);
      response.SetString("message", "Vector statistics retrieved successfully");

      vectordb::Json vector_stats;
      vector_stats.LoadFromString("{}");
      vector_stats.SetInt("totalVectors", stats.total_vectors);
      vector_stats.SetInt("activeVectors", stats.active_vectors);
      vector_stats.SetInt("deletedVectors", stats.deleted_vectors);
      vector_stats.SetDouble("deletedRatio", stats.deleted_ratio);
      vector_stats.SetInt("capacity", stats.capacity);

      response.SetObject("stats", vector_stats);

    } catch (const std::exception& e) {
      response.SetInt("statusCode", Status::CODE_500.code);
      response.SetString("message", std::string("Failed to retrieve vector statistics: ") + e.what());
    }

    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

  ADD_CORS(GetCompactionStatus)

  ENDPOINT_INFO(GetCompactionStatus) {
    info->summary = "Get Compaction Status";
    info->description = "Check if compaction is currently running";
    info->addResponse<String>(Status::CODE_200, "application/json", "Status retrieved successfully");
    info->addTag("Compaction");
  }
  ENDPOINT("GET", "api/compact/status", GetCompactionStatus) {
    vectordb::Json response;
    response.LoadFromString("{}");

    auto& compactionMgr = vectordb::engine::CompactionManager::GetInstance();
    bool isCompacting = compactionMgr.IsCompacting();

    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Get compaction status successfully.");
    response.SetBool("isCompacting", isCompacting);

    auto httpResponse = createResponse(Status::CODE_200, response.DumpToString());
    httpResponse->putHeader("Content-Type", "application/json");
    return httpResponse;
  }

/**
 *  Finish ENDPOINTs generation ('ApiController' codegen)
 */
#include OATPP_CODEGEN_END(ApiController)
};

}  // namespace web
}  // namespace server
}  // namespace vectordb
