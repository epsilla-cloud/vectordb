#pragma once

#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <oatpp-swagger/Model.hpp>
#include <oatpp-swagger/Resources.hpp>
#include <oatpp-swagger/Generator.hpp>
#include <string>
#include <vector>

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
    // Serve custom index.html with correct paths for /api/docs
    std::string htmlContent = R"(
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8">
    <title>VectorDB API Documentation</title>
    <link rel="stylesheet" type="text/css" href="/api/docs/swagger-ui.css" >
    <link rel="icon" type="image/png" href="/api/docs/favicon-32x32.png" sizes="32x32" />
    <link rel="icon" type="image/png" href="/api/docs/favicon-16x16.png" sizes="16x16" />
    <style>
      html { box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }
      *, *:before, *:after { box-sizing: inherit; }
      body { margin:0; background: #fafafa; }
    </style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="/api/docs/swagger-ui-bundle.js" charset="UTF-8"> </script>
    <script src="/api/docs/swagger-ui-standalone-preset.js" charset="UTF-8"> </script>
    <script>
    window.onload = function() {
      const ui = SwaggerUIBundle({
        url: "/api-docs/oas-3.0.0.json",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [ SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset ],
        plugins: [ SwaggerUIBundle.plugins.DownloadUrl ],
        layout: "StandaloneLayout"
      })
      window.ui = ui
    }
    </script>
  </body>
</html>
)";

    auto response = createResponse(Status::CODE_200, htmlContent);
    response->putHeader("Content-Type", "text/html");
    return response;
  }

  ENDPOINT("GET", "/api/docs/{filename}", getUIResource, PATH(String, filename)) {
    // Set correct content type based on file extension
    oatpp::String contentType = "application/octet-stream";
    if (filename->find(".css") != std::string::npos) {
      contentType = "text/css";
    } else if (filename->find(".js") != std::string::npos) {
      contentType = "application/javascript";
    } else if (filename->find(".png") != std::string::npos) {
      contentType = "image/png";
    } else if (filename->find(".html") != std::string::npos) {
      contentType = "text/html";
    }

    if(m_resources->isStreaming()) {
      auto body = std::make_shared<oatpp::web::protocol::http::outgoing::StreamingBody>(
        m_resources->getResourceStream(filename->c_str())
      );
      auto response = OutgoingResponse::createShared(Status::CODE_200, body);
      response->putHeader("Content-Type", contentType);
      return response;
    }

    auto response = createResponse(Status::CODE_200, m_resources->getResource(filename->c_str()));
    response->putHeader("Content-Type", contentType);
    return response;
  }
  
#include OATPP_CODEGEN_END(ApiController)

};

} // namespace web
} // namespace server
} // namespace vectordb