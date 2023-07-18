#pragma once

#include <iostream>
#include <string>

#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include "utils/json.hpp"
#include "utils/status.hpp"
#include "server/web_server/dto/db_dto.hpp"
#include "server/web_server/dto/status_dto.hpp"
#include "server/web_server/handler/web_request_handler.hpp"
#include "db/catalog/meta.hpp"
#include "db/catalog/basic_meta_impl.hpp"

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
        std::string dbPath = parsedBody.GetString("path");
        std::string dbName = parsedBody.GetString("name");
        vectordb::engine::meta::MetaPtr meta = std::make_shared<vectordb::engine::meta::BasicMetaImpl>();
        vectordb::Status status = meta->LoadDatabase(dbPath, dbName);
        if (!status.ok()) {
            return createResponse(Status::CODE_501, "Load/Create " + dbName + " failed.");
        }
        return createResponse(Status::CODE_200, "Load/Create " + dbName + " successfully.");
    }

    ADD_CORS(UnloadDB)

    ENDPOINT("POST", "api/{db_name}/unload", UnloadDB, PATH(String, dbName, "db_name")) {
        vectordb::engine::meta::MetaPtr meta = std::make_shared<vectordb::engine::meta::BasicMetaImpl>();
        vectordb::Status status = meta->UnloadDatabase(dbName);
        if (!status.ok()) {
            return createResponse(Status::CODE_501, "Unload " + dbName + " failed.");
        }

        return createResponse(Status::CODE_200, "Unload " + dbName + " successfully.");
    }

    ADD_CORS(DropDB)

    ENDPOINT("DELETE", "api/{db_name}/drop", DropDB, PATH(String, dbName, "db_name")) {
        vectordb::engine::meta::MetaPtr meta = std::make_shared<vectordb::engine::meta::BasicMetaImpl>();
        vectordb::Status status = meta->DropDatabase(dbName);
        if (!status.ok()) {
            return createResponse(Status::CODE_501, "Drop " + dbName + " failed.");
        }

        return createResponse(Status::CODE_200, "Drop " + dbName + " successfully.");
    }

/**
 *  Finish ENDPOINTs generation ('ApiController' codegen)
 */
#include OATPP_CODEGEN_END(ApiController)
};

}
}
}
