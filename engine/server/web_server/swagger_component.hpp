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
      .setDescription(
        "High-performance vector database with similarity search capabilities.\n\n"
        "**Features:**\n"
        "- Dense and sparse vector support\n"
        "- Full-text search with Quickwit integration\n"
        "- Hybrid search (vector + text) with Reciprocal Rank Fusion (RRF)\n"
        "- Multiple embedding providers\n"
        "- Real-time indexing\n"
        "- MVCC transaction support\n"
        "- Write-Ahead Logging (WAL)\n\n"
        "**API Documentation:**\n"
        "For detailed usage examples and integration guides, see:\n"
        "- Full-text Search API Guide: `/docs/FULLTEXT_API_GUIDE.md`\n"
        "- Logging Standards: `/docs/LOGGING_STANDARDS.md`\n"
        "- Concurrency Analysis: `/docs/FULLTEXT_CONCURRENCY_ANALYSIS.md`\n\n"
        "**Configuration:**\n"
        "Full-text search can be enabled via environment variables:\n"
        "- `VECTORDB_FULLTEXT_SEARCH_ENABLE=true` - Enable full-text search (recommended)\n"
        "- `VECTORDB_FULLTEXT_SEARCH_PROVIDER=quickwit` - Search provider (default: quickwit)\n"
        "- `VECTORDB_QUICKWIT_BINARY=/usr/local/bin/quickwit` - Path to Quickwit binary\n"
        "- `VECTORDB_FULLTEXT_PORT=7280` - Full-text search port (default: 7280)\n\n"
        "**Full-Text Search Endpoints:**\n"
        "- `POST /api/{db}/tables/{table}/fulltext/search` - Full-text search\n"
        "- `POST /api/{db}/tables/{table}/fulltext/hybrid-search` - Hybrid vector+text search\n"
        "- `GET /api/fulltext/status` - Check full-text search status\n"
        "- `POST /api/{db}/tables/{table}/fulltext/rebuild` - Rebuild full-text index\n"
        "- `GET /api/{db}/tables/{table}/fulltext/stats` - Get index statistics\n"
      )
      .setVersion("1.4.0")
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