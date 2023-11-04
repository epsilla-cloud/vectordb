#pragma once

#include <iostream>
#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <string>
#include <vector>

#include "db/catalog/basic_meta_impl.hpp"
#include "db/catalog/meta.hpp"
#include "db/db_mvp.hpp"
#include "db/db_server.hpp"
#include "db/table_mvp.hpp"
#include "server/web_server/dto/db_dto.hpp"
#include "server/web_server/dto/status_dto.hpp"
#include "server/web_server/handler/web_request_handler.hpp"
#include "server/web_server/utils/util.hpp"
#include "utils/json.hpp"
#include "utils/status.hpp"

#define WEB_LOG_PREFIX "[Web] "

namespace vectordb {
namespace server {
namespace web {

constexpr const int64_t InitTableScale = 150000;

class WebController : public oatpp::web::server::api::ApiController {
 public:
  WebController(const std::shared_ptr<ObjectMapper>& objectMapper)
      : oatpp::web::server::api::ApiController(objectMapper) {
  }

 public:
  static std::shared_ptr<WebController>
  createShared(OATPP_COMPONENT(std::shared_ptr<ObjectMapper>, objectMapper)) {
    return std::make_shared<WebController>(objectMapper);
  }

  std::shared_ptr<vectordb::engine::DBServer> db_server = std::make_shared<vectordb::engine::DBServer>();
  // vectordb::engine::meta::MetaPtr meta = std::make_shared<vectordb::engine::meta::BasicMetaImpl>();

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
    auto dto = StatusDto::createShared();
    dto->statusCode = 200;
    dto->message = "Server is online!";
    return createDtoResponse(Status::CODE_200, dto);
  }

  ADD_CORS(LoadDB)

  ENDPOINT("POST", "/api/load", LoadDB, BODY_STRING(String, body)) {
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
    int64_t init_table_scale = InitTableScale;
    if (parsedBody.HasMember("vectorScale")) {
      init_table_scale = parsedBody.GetInt("vectorScale");
    }
    bool wal_enabled = true;
    if (parsedBody.HasMember("walEnabled")) {
      wal_enabled = parsedBody.GetBool("walEnabled");
    }
    vectordb::Status status = db_server->LoadDB(db_name, db_path, init_table_scale, wal_enabled);

    if (!status.ok()) {
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

    vectordb::engine::meta::TableSchema table_schema;
    if (!parsedBody.HasMember("name")) {
      dto->statusCode = Status::CODE_400.code;
      dto->message = "Missing table name in your payload.";
      return createDtoResponse(Status::CODE_400, dto);
    }
    table_schema.name_ = parsedBody.GetString("name");

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
      field.name_ = body_field.GetString("name");
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
        field.field_type_ = WebUtil::GetFieldType(d_type.assign(body_field.GetString("dataType")));
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
        field.metric_type_ = WebUtil::GetMetricType(m_type.assign(body_field.GetString("metricType")));
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

    vectordb::Status status = db_server->CreateTable(db_name, table_schema);

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
    dto->statusCode = Status::CODE_200.code;
    dto->message = "Create " + table_schema.name_ + " successfully.";
    return createDtoResponse(Status::CODE_200, dto);
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

  ADD_CORS(InsertRecords)

  ENDPOINT("POST", "/api/{db_name}/data/insert", InsertRecords,
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

    if (!parsedBody.HasMember("data")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "data is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    std::string table_name = parsedBody.GetString("table");
    // vectordb::engine::meta::DatabaseSchema db_schema;
    // vectordb::Status db_status = meta->GetDatabase(db_name, db_schema);
    // if (!db_status.ok()) {
    //     status_dto->statusCode = Status::CODE_500.code;
    //     status_dto->message = db_status.message();
    //     return createDtoResponse(Status::CODE_500, status_dto);
    // }

    // vectordb::engine::meta::TableSchema table_schema;
    // vectordb::Status table_status = meta->GetTable(db_name, table_name, table_schema);
    // if (!table_status.ok()) {
    //     status_dto->statusCode = Status::CODE_500.code;
    //     status_dto->message = table_status.message();
    //     return createDtoResponse(Status::CODE_500, status_dto);
    // }

    // std::shared_ptr<vectordb::engine::TableMVP> table =
    //     std::make_shared<vectordb::engine::TableMVP>(table_schema, db_schema.path_);
    // auto db = std::make_shared<vectordb::engine::DBMVP>(db_schema);
    // auto table = db->GetTable(table_name);

    auto data = parsedBody.GetArray("data");
    vectordb::Status insert_status = db_server->Insert(db_name, table_name, data);
    if (!insert_status.ok()) {
      status_dto->statusCode = Status::CODE_500.code;
      status_dto->message = insert_status.message();
      return createDtoResponse(Status::CODE_500, status_dto);
    }

    status_dto->statusCode = Status::CODE_200.code;
    status_dto->message = "Insert data to " + table_name + " successfully. " + insert_status.message();
    return createDtoResponse(Status::CODE_200, status_dto);
  }

  ADD_CORS(DeleteRecordsByPK)

  ENDPOINT("POST", "/api/{db_name}/data/delete", DeleteRecordsByPK,
           PATH(String, db_name, "db_name"),
           BODY_STRING(String, body)) {
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
    if (requestBody.HasMember("primaryKeys")) {
      pks = requestBody.GetArray("primaryKeys");
      if (pks.GetSize() == 0) {
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
    auto status = db_server->Delete(db_name, table, pks, filter);
    auto responseCode = Status::CODE_200;
    if (status.ok()) {
      dto->statusCode = Status::CODE_200.code;
      dto->message = status.message();
    } else {
      responseCode = Status::CODE_400;
      dto->statusCode = Status::CODE_400.code;
      dto->message = status.message();
    }
    return createDtoResponse(responseCode, dto);
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

  ADD_CORS(Query)

  ENDPOINT("POST", "/api/{db_name}/data/query", Query,
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

    if (!parsedBody.HasMember("queryField")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "queryField is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    if (!parsedBody.HasMember("queryVector")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "queryVector is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    if (!parsedBody.HasMember("limit")) {
      status_dto->statusCode = Status::CODE_400.code;
      status_dto->message = "limit is missing in your payload.";
      return createDtoResponse(Status::CODE_400, status_dto);
    }

    std::string table_name = parsedBody.GetString("table");

    // vectordb::engine::meta::DatabaseSchema db_schema;
    // vectordb::Status db_status = meta->GetDatabase(db_name, db_schema);
    // if (!db_status.ok()) {
    //     status_dto->statusCode = Status::CODE_500.code;
    //     status_dto->message = db_status.message();
    //     return createDtoResponse(Status::CODE_500, status_dto);
    // }

    // vectordb::engine::meta::TableSchema table_schema;
    // vectordb::Status table_status = meta->GetTable(db_name, table_name, table_schema);
    // if (!table_status.ok()) {
    //     status_dto->statusCode = Status::CODE_500.code;
    //     status_dto->message = table_status.message();
    //     return createDtoResponse(Status::CODE_500, status_dto);
    // }

    // std::cout << db_schema.path_ << std::endl;
    // // std::shared_ptr<vectordb::engine::TableMVP> table =
    // //     std::make_shared<vectordb::engine::TableMVP>(table_schema, db_schema.path_);
    // auto db = std::make_shared<vectordb::engine::DBMVP>(db_schema);
    // auto table = db->GetTable(table_name);
    std::string field_name = parsedBody.GetString("queryField");

    std::vector<std::string> query_fields;
    if (parsedBody.HasMember("response")) {
      size_t field_size = parsedBody.GetArraySize("response");
      for (size_t i = 0; i < field_size; i++) {
        auto field = parsedBody.GetArrayElement("response", i);
        query_fields.push_back(field.GetString());
      }
    }

    size_t vector_size = parsedBody.GetArraySize("queryVector");
    float query_vector[vector_size];
    for (size_t i = 0; i < vector_size; i++) {
      auto vector = parsedBody.GetArrayElement("queryVector", i);
      query_vector[i] = (float)vector.GetDouble();
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

    vectordb::Json result;
    vectordb::Status search_status = db_server->Search(
        db_name,
        table_name,
        field_name,
        query_fields,
        vector_size,
        query_vector,
        limit,
        result,
        filter,
        with_distance);

    if (!search_status.ok()) {
      oatpp::web::protocol::http::Status status;
      switch (search_status.code()) {
        case INVALID_EXPR:
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
    response.SetObject("result", result);
    return createResponse(Status::CODE_200, response.DumpToString());
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

    vectordb::Json result;
    vectordb::Status get_status = db_server->Project(
        db_name, table_name, query_fields, pks, filter, skip, limit, result);
    if (!get_status.ok()) {
      status_dto->statusCode = Status::CODE_500.code;
      status_dto->message = get_status.message();
      return createDtoResponse(Status::CODE_500, status_dto);
    }

    vectordb::Json response;
    response.LoadFromString("{}");
    response.SetInt("statusCode", Status::CODE_200.code);
    response.SetString("message", "Query get successfully.");
    response.SetObject("result", result);
    return createResponse(Status::CODE_200, response.DumpToString());
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

/**
 *  Finish ENDPOINTs generation ('ApiController' codegen)
 */
#include OATPP_CODEGEN_END(ApiController)
};

}  // namespace web
}  // namespace server
}  // namespace vectordb
