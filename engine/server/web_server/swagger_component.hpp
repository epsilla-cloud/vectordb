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
        "**Health & Monitoring Endpoints:**\n"
        "- `GET /health` - Health check endpoint\n"
        "- `GET /api/health` - API health check endpoint\n"
        "- `GET /metrics` - Prometheus format metrics\n"
        "- `GET /state` - Get current system state\n"
        "- `GET /api/config` - Get runtime configuration\n"
        "- `GET /api/memory/stats` - Get memory statistics\n\n"
        "**Database Management Endpoints:**\n"
        "- `POST /api/load` - Load a database\n"
        "- `POST /api/{db}/unload` - Unload a database\n"
        "- `POST /api/{db}/release` - Release database memory\n"
        "- `DELETE /api/{db}/drop` - Drop a database\n"
        "- `POST /api/dump` - Dump database to file\n"
        "- `GET /api/records/count` - Get record count for all databases\n"
        "- `GET /api/{db}/records/count` - Get record count for specific database\n\n"
        "**Table Schema Endpoints:**\n"
        "- `POST /api/{db}/schema/tables` - Create a new table\n"
        "- `GET /api/{db}/schema/tables` - List all tables (REST)\n"
        "- `GET /api/{db}/schema/tables/show` - List all tables\n"
        "- `GET /api/{db}/schema/tables/{table}/describe` - Describe table schema\n"
        "- `DELETE /api/{db}/schema/tables/{table}` - Drop a table\n\n"
        "**Data Operations Endpoints:**\n"
        "- `POST /api/{db}/data/insert` - Insert records into table\n"
        "- `POST /api/{db}/data/insert_optimized` - Optimized batch insert\n"
        "- `POST /api/{db}/data/insertprepare` - Prepare insert operation\n"
        "- `POST /api/{db}/data/delete` - Delete records by primary key\n"
        "- `POST /api/{db}/data/query` - Query/search records\n"
        "- `POST /api/{db}/data/get` - Get specific records (project)\n"
        "- `POST /api/{db}/data/load` - Load data from CSV\n"
        "- `GET /api/{db}/statistics` - Get database statistics\n\n"
        "**Full-Text Search Endpoints:**\n"
        "- `POST /api/{db}/tables/{table}/fulltext/search` - Full-text search\n"
        "- `POST /api/{db}/tables/{table}/fulltext/hybrid-search` - Hybrid vector+text search\n"
        "- `GET /api/fulltext/status` - Check full-text search status\n"
        "- `POST /api/{db}/tables/{table}/fulltext/rebuild` - Rebuild full-text index\n"
        "- `GET /api/{db}/tables/{table}/fulltext/stats` - Get index statistics\n\n"
        "**Maintenance Endpoints:**\n"
        "- `POST /api/rebuild` - Rebuild indexes\n"
        "- `POST /api/{db}/compact` - Compact specific database\n"
        "- `POST /api/compact` - Compact all databases\n"
        "- `POST /api/setleader` - Set leader node (clustering)\n\n"
        "**Documentation:**\n"
        "- `GET /api/docs` - Interactive Swagger UI documentation\n"
        "- `GET /api-docs/oas-3.0.0.json` - OpenAPI specification\n"
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