#pragma once

#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <oatpp-swagger/Model.hpp>
#include <oatpp-swagger/Resources.hpp>
#include <oatpp-swagger/Generator.hpp>

namespace vectordb {
namespace server {
namespace web {

/**
 * @brief Custom Swagger Controller with /api/docs endpoints instead of /swagger/ui
 */
class CustomSwaggerController : public oatpp::web::server::api::ApiController {
private:
  oatpp::Object<oatpp::swagger::oas3::Document> m_document;
  std::shared_ptr<oatpp::swagger::Resources> m_resources;

public:
  
  /**
   * Constructor
   */
  CustomSwaggerController(const std::shared_ptr<ObjectMapper>& objectMapper,
                         const oatpp::Object<oatpp::swagger::oas3::Document>& document,
                         const std::shared_ptr<oatpp::swagger::Resources>& resources)
    : oatpp::web::server::api::ApiController(objectMapper)
    , m_document(document)
    , m_resources(resources) {}

  /**
   * Create shared custom controller
   */
  static std::shared_ptr<CustomSwaggerController> createShared(const oatpp::web::server::api::Endpoints& endpointsList,
                                                               OATPP_COMPONENT(std::shared_ptr<oatpp::swagger::DocumentInfo>, documentInfo),
                                                               OATPP_COMPONENT(std::shared_ptr<oatpp::swagger::Resources>, resources)) {
    
    auto serializerConfig = oatpp::parser::json::mapping::Serializer::Config::createShared();
    serializerConfig->includeNullFields = false;
    
    auto deserializerConfig = oatpp::parser::json::mapping::Deserializer::Config::createShared();
    deserializerConfig->allowUnknownFields = false;
    
    auto objectMapper = oatpp::parser::json::mapping::ObjectMapper::createShared(serializerConfig, deserializerConfig);

    std::shared_ptr<oatpp::swagger::Generator::Config> generatorConfig;
    try {
      generatorConfig = OATPP_GET_COMPONENT(std::shared_ptr<oatpp::swagger::Generator::Config>);
    } catch (std::runtime_error e) {
      generatorConfig = std::make_shared<oatpp::swagger::Generator::Config>();
    }

    oatpp::swagger::Generator generator(generatorConfig);
    auto document = generator.generateDocument(documentInfo, endpointsList);

    return std::make_shared<CustomSwaggerController>(objectMapper, document, resources);
  }

#include OATPP_CODEGEN_BEGIN(ApiController)

  ENDPOINT("GET", "/api-docs/oas-3.0.0.json", api) {
    return createDtoResponse(Status::CODE_200, m_document);
  }
  
  ENDPOINT("GET", "/api/docs", getUIRoot) {
    if(m_resources->isStreaming()) {
      auto body = std::make_shared<oatpp::web::protocol::http::outgoing::StreamingBody>(
        m_resources->getResourceStream("index.html")
      );
      return OutgoingResponse::createShared(Status::CODE_200, body);
    }
    return createResponse(Status::CODE_200, m_resources->getResource("index.html"));
  }
  
  ENDPOINT("GET", "/api/docs/{filename}", getUIResource, PATH(String, filename)) {
    if(m_resources->isStreaming()) {
      auto body = std::make_shared<oatpp::web::protocol::http::outgoing::StreamingBody>(
        m_resources->getResourceStream(filename->c_str())
      );
      return OutgoingResponse::createShared(Status::CODE_200, body);
    }
    return createResponse(Status::CODE_200, m_resources->getResource(filename->c_str()));
  }
  
#include OATPP_CODEGEN_END(ApiController)

};

} // namespace web
} // namespace server
} // namespace vectordb