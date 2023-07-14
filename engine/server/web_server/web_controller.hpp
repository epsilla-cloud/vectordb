#pragma once

#include <iostream>
#include <string>

#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include "server/web_server/dto/status_dto.hpp"
// #include "server/web_server/handler/web_request_handler.hpp"

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
        return createDtoResponse(Status::CODE_200, StatusDto::createShared());
    }

/**
 *  Finish ENDPOINTs generation ('ApiController' codegen)
 */
#include OATPP_CODEGEN_END(ApiController)
};

}
}
}
