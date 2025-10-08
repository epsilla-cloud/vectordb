#pragma once

#include <oatpp/core/macro/component.hpp>
#include <oatpp-swagger/Model.hpp>
#include <oatpp-swagger/Resources.hpp>

namespace vectordb {
namespace server {
namespace web {

/**
 * @brief Swagger UI configuration component for VectorDB API documentation
 */
class SwaggerComponent {
public:
  
  /**
   * @brief General API documentation info
   */
  OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::swagger::DocumentInfo>, swaggerDocumentInfo)([] {
    
    oatpp::swagger::DocumentInfo::Builder builder;
    
    builder
      .setTitle("Epsilla VectorDB REST API")
      .setDescription("High-performance vector database with similarity search capabilities. Supports dense and sparse vectors, multiple embedding providers, and real-time indexing.")
      .setVersion("1.2.0")
      .setContactName("Epsilla Team")
      .setContactUrl("https://github.com/epsilla-cloud/vectordb")
      .setLicenseName("Apache License 2.0")
      .setLicenseUrl("https://www.apache.org/licenses/LICENSE-2.0")
      .addServer("http://localhost:8888", "Local development server")
      .addServer("https://vectordb.epsilla.com", "Production server");
    
    return builder.build();
  }());

  /**
   * @brief Swagger UI resources configuration
   */
  OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::swagger::Resources>, swaggerResources)([] {
    // Use the correct path to swagger resources - relative to where the server is run
    return oatpp::swagger::Resources::streamResources("build/dependencies/include/oatpp-1.3.0/bin/oatpp-swagger/res");
  }());

};

} // namespace web
} // namespace server
} // namespace vectordb