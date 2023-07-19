#pragma once

#include <iostream>
#include <string>

#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include "utils/json.hpp"
#include "utils/status.hpp"
#include "db/catalog/meta.hpp"
#include "db/catalog/basic_meta_impl.hpp"
#include "server/web_server/dto/status_dto.hpp"
#include "server/web_server/dto/db_dto.hpp"
#include "server/web_server/handler/web_request_handler.hpp"
#include "server/web_server/utils/util.hpp"

#define WEB_LOG_PREFIX "[Web] "

namespace vectordb {
namespace server {
namespace web {

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

/**
 *  Begin ENDPOINTs generation ('ApiController' codegen)
 */
#include OATPP_CODEGEN_BEGIN(ApiController)
    vectordb::engine::meta::MetaPtr meta = std::make_shared<vectordb::engine::meta::BasicMetaImpl>();

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
        parsedBody.LoadFromString(body);
        std::string db_path = parsedBody.GetString("path");
        std::string db_name = parsedBody.GetString("name");
        vectordb::Status status = meta->LoadDatabase(db_path, db_name);
        if (!status.ok()) {
            return createResponse(Status::CODE_500, "Load/Create " + db_name + " failed.");
        }

        return createResponse(Status::CODE_200, "Load/Create " + db_name + " successfully.");
    }

    ADD_CORS(UnloadDB)

    ENDPOINT("POST", "api/{db_name}/unload", UnloadDB, PATH(String, db_name, "db_name")) {
        vectordb::Status status = meta->UnloadDatabase(db_name);
        if (!status.ok()) {
            return createResponse(Status::CODE_500, "Unload " + db_name + " failed.");
        }

        return createResponse(Status::CODE_200, "Unload " + db_name + " successfully.");
    }

    ADD_CORS(DropDB)

    ENDPOINT("DELETE", "api/{db_name}/drop", DropDB, PATH(String, db_name, "db_name")) {
        vectordb::Status status = meta->DropDatabase(db_name);
        if (!status.ok()) {
            return createResponse(Status::CODE_500, "Drop " + db_name + " failed.");
        }

        return createResponse(Status::CODE_200, "Drop " + db_name + " successfully.");
    }

    ADD_CORS(CreateTable)

    ENDPOINT("POST", "/api/{db_name}/schema/tables", CreateTable,
        PATH(String, db_name, "db_name"),
        BODY_STRING(String, body)) {

        vectordb::Json parsedBody;
        parsedBody.LoadFromString(body);
        vectordb::engine::meta::TableSchema table_schema;
        if (!parsedBody.HasMember("name")) {
            return createResponse(Status::CODE_400, "Missing table name in your payload.");
        }
        table_schema.name_ = parsedBody.GetString("name");

        if (!parsedBody.HasMember("fields")) {
            return createResponse(Status::CODE_400, "Missing fields in your payload.");
        }
        size_t fields_size = parsedBody.GetArraySize("fields");
        for (size_t i = 0; i < fields_size; i++) {
            auto body_field = parsedBody.GetArrayElement("fields", i);
            vectordb::engine::meta::FieldSchema field;
            field.name_ = body_field.GetString("name");
            if (body_field.HasMember("primaryKey")) {
                field.is_primary_key_ = body_field.GetBool("primaryKey");
            }
            if (body_field.HasMember("dataType")) {
                std::string d_type;
                field.field_type_ = WebUtil::GetFieldType(d_type.assign(body_field.GetString("dataType")));
            }
            if (body_field.HasMember("dimensions")) {
                field.vector_dimension_ = body_field.GetInt("dimensions");
            }
            if (body_field.HasMember("metricType")) {
                std::string m_type;
                field.metric_type_ = WebUtil::GetMetricType(m_type.assign(body_field.GetString("metricType")));
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

        std::string name;
        vectordb::Status status = meta->CreateTable(name.assign(db_name), table_schema);
        if (!status.ok()) {
            return createResponse(Status::CODE_501, "Create " + table_schema.name_ + " failed.");
        }

        return createResponse(Status::CODE_200, "Create " + table_schema.name_ + " successfully.");
    }

    ADD_CORS(DropTable)

    ENDPOINT("DELETE", "api/{db_name}/schema/tables/{table_name}", DropTable,
        PATH(String, db_name, "db_name"),
        PATH(String, table_name, "table_name")) {

        vectordb::Status status = meta->DropTable(db_name, table_name);
        if (!status.ok()) {
            return createResponse(Status::CODE_501, "Drop " + table_name + " from " + db_name + " failed.");
        }

        return createResponse(Status::CODE_200, "Drop " + table_name + " from " + db_name + " successfully.");
    }

    ADD_CORS(DescribeSchema)

    ENDPOINT("GET", "/api/{db_name}/schema/tables/{table_name}/describe", DescribeSchema,
        PATH(String, db_name, "db_name"),
        PATH(String, table_name, "table_name")) {

        // vectordb::engine::meta::TableSchema table_schema;
        // vectordb::Status status = meta->GetTable(db_name, table_name, table_schema);
        // if (!status.ok()) {
            // return createResponse(Status::CODE_501, "Failed to get " + table_name + ".");
        // }

        auto dto = SchemaInfoDto::createShared();
        dto->message = "Get information of " + table_name + " from " + db_name + " successfully.";
        // dto->result = table_schema;
        return createDtoResponse(Status::CODE_200, dto);
    }

    ADD_CORS(ListTables)

    ENDPOINT("GET", "/api/{db_name}/schema/tables/show", ListTables, PATH(String, db_name, "db_name")) {
        auto dto = TableListDto::createShared();
        dto->message = "Get all tables in " + db_name + " successfully.";
        return createDtoResponse(Status::CODE_200, dto);
    }

    ADD_CORS(InsertRecords)

    ENDPOINT("POST", "/api/{db_name}/data/insert", InsertRecords,
        PATH(String, db_name, "db_name"),
        BODY_DTO(Object<RecordsInsertReqDto>, body)) {
        if (!body->table) {
            return createResponse(Status::CODE_501, "Missing table name in your payload.");
        }
        return createResponse(Status::CODE_200, "I got your db " + db_name + " and your requested table " + body->table);
    }

    ADD_CORS(DeleteRecordsByID)

    ENDPOINT("POST", "/api/{db_name}/data/delete", DeleteRecordsByID,
        PATH(String, db_name, "db_name"),
        BODY_DTO(Object<DeleteRecordsReqDto>, body)) {

        if (!body->table) {
            return createResponse(Status::CODE_400, "Missing table name in your payload.");
        }
        if (!body->ids) {
            return createResponse(Status::CODE_400, "Missing ID list to delete in your payload.");
        }

        const auto& body_ids = body->ids;
        if (body_ids->size() == 0) {
            return createResponse(Status::CODE_400, "No IDs to delete provided.");
        }
        std::vector<std::string> arr;
        for (size_t i = 0; i < body_ids->size(); i++) {
            arr.push_back(body_ids[i]);
        }
        return createResponse(
            Status::CODE_200,
            "Deleted " + WebUtil::JoinStrs(arr, ", ") + " from " + body->table + " in " + db_name + " successfully."
        );
    }

    ADD_CORS(LoadCSV)

    ENDPOINT("POST", "/api/{db_name}/data/load", LoadCSV,
        PATH(String, db_name, "db_name"),
        BODY_STRING(String, body)) {

        return createResponse(Status::CODE_200, "Loading csv to " + db_name + ".");
    }

    ADD_CORS(Query)

    ENDPOINT("POST", "/api/{db_name}/data/query", Query,
        PATH(String, db_name, "db_name"),
        BODY_STRING(String, body)) {

        return createResponse(Status::CODE_200, "Query with " + db_name + ".");
    }

/**
 *  Finish ENDPOINTs generation ('ApiController' codegen)
 */
#include OATPP_CODEGEN_END(ApiController)
};

}
}
}
