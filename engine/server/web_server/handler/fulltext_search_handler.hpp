#pragma once

#include "oatpp/web/server/api/ApiController.hpp"
#include "oatpp/parser/json/mapping/ObjectMapper.hpp"
#include "oatpp/core/macro/codegen.hpp"
#include "oatpp/core/macro/component.hpp"
#include "config/config.hpp"
#include "db/db_server.hpp"
#include "utils/json.hpp"
#include "server/web_server/dto/status_dto.hpp"

#include OATPP_CODEGEN_BEGIN(ApiController)

/**
 * Full-Text Search API Handler
 * Provides dedicated endpoints for full-text search operations
 */
class FullTextSearchHandler : public oatpp::web::server::api::ApiController {
public:
  FullTextSearchHandler(const std::shared_ptr<ObjectMapper>& objectMapper,
                        vectordb::engine::DBServer* db_server)
    : oatpp::web::server::api::ApiController(objectMapper),
      db_server_(db_server) {}

  static std::shared_ptr<FullTextSearchHandler> createShared(
      OATPP_COMPONENT(std::shared_ptr<ObjectMapper>, objectMapper),
      vectordb::engine::DBServer* db_server) {
    return std::make_shared<FullTextSearchHandler>(objectMapper, db_server);
  }

private:
  vectordb::engine::DBServer* db_server_;

  // Helper: Check if full-text search is enabled
  bool IsFullTextEnabled() {
    return vectordb::globalConfig.EnableFullText.load(std::memory_order_acquire);
  }

  // Helper: Create error response when full-text is disabled
  std::shared_ptr<OutgoingResponse> CreateDisabledResponse() {
    using namespace vectordb::server::web;
    auto dto = StatusDto::createShared();
    dto->statusCode = 503;
    dto->message = "Full-text search is not enabled. Set VECTORDB_FULLTEXT_SEARCH_ENABLE=true to enable.";
    return createDtoResponse(Status::CODE_503, dto);
  }

public:

  /**
   * @brief Full-text search endpoint
   * POST /api/{db}/tables/{table}/fulltext/search
   */
  ENDPOINT("POST", "/api/{db}/tables/{table}/fulltext/search",
           fulltextSearch,
           PATH(String, db, "db"),
           PATH(String, table, "table"),
           BODY_STRING(String, body)) {

    // Check if full-text search is enabled
    if (!IsFullTextEnabled()) {
      return CreateDisabledResponse();
    }

    try {
      // Parse request
      vectordb::Json request_json;
      request_json.LoadFromString(body->c_str());

      // Extract query parameters
      std::string query = request_json.GetString("query");
      int limit = request_json.HasMember("limit") ? request_json.GetInt("limit") : 10;

      // TODO: Call full-text search engine
      // For now, return a placeholder response

      vectordb::Json result;
      result.SetInt("statusCode", 200);
      result.SetString("message", "Full-text search executed");
      result.SetObject("request", request_json);

      auto response = createResponse(Status::CODE_200, result.DumpToString().c_str());
      response->putHeader(Header::CONTENT_TYPE, "application/json");
      return response;

    } catch (const std::exception& e) {
      auto response = createResponse(Status::CODE_500, e.what());
      response->putHeader(Header::CONTENT_TYPE, "application/json");
      return response;
    }
  }

  /**
   * @brief Hybrid search endpoint (vector + full-text)
   * POST /api/{db}/tables/{table}/fulltext/hybrid-search
   */
  ENDPOINT("POST", "/api/{db}/tables/{table}/fulltext/hybrid-search",
           hybridSearch,
           PATH(String, db, "db"),
           PATH(String, table, "table"),
           BODY_STRING(String, body)) {

    // Check if full-text search is enabled
    if (!IsFullTextEnabled()) {
      return CreateDisabledResponse();
    }

    try {
      // Parse request
      vectordb::Json request_json;
      request_json.LoadFromString(body->c_str());

      // TODO: Implement hybrid search
      // Combine vector search + full-text search using RRF

      vectordb::Json result;
      result.SetInt("statusCode", 200);
      result.SetString("message", "Hybrid search executed");
      result.SetObject("request", request_json);

      auto response = createResponse(Status::CODE_200, result.DumpToString().c_str());
      response->putHeader(Header::CONTENT_TYPE, "application/json");
      return response;

    } catch (const std::exception& e) {
      auto response = createResponse(Status::CODE_500, e.what());
      response->putHeader(Header::CONTENT_TYPE, "application/json");
      return response;
    }
  }

  /**
   * @brief Get full-text search status
   * GET /api/fulltext/status
   */
  ENDPOINT("GET", "/api/fulltext/status", fulltextStatus) {

    vectordb::Json result;
    result.SetInt("statusCode", 200);
    result.SetBool("enabled", IsFullTextEnabled());

    if (IsFullTextEnabled()) {
      result.SetString("provider", vectordb::globalConfig.FullTextEngine.c_str());
      result.SetString("binary", vectordb::globalConfig.FullTextBinaryPath.c_str());
      result.SetInt("port", vectordb::globalConfig.FullTextPort.load());
    } else {
      result.SetString("message", "Full-text search is not enabled. Set VECTORDB_FULLTEXT_SEARCH_ENABLE=true to enable.");
    }

    auto response = createResponse(Status::CODE_200, result.DumpToString().c_str());
    response->putHeader(Header::CONTENT_TYPE, "application/json");
    return response;
  }

  /**
   * @brief Rebuild full-text index for a table
   * POST /api/{db}/tables/{table}/fulltext/rebuild
   */
  ENDPOINT("POST", "/api/{db}/tables/{table}/fulltext/rebuild",
           rebuildFulltextIndex,
           PATH(String, db, "db"),
           PATH(String, table, "table")) {

    // Check if full-text search is enabled
    if (!IsFullTextEnabled()) {
      return CreateDisabledResponse();
    }

    try {
      // TODO: Implement index rebuild

      vectordb::Json result;
      result.SetInt("statusCode", 200);
      result.SetString("message", "Full-text index rebuild initiated");
      result.SetString("database", db->c_str());
      result.SetString("table", table->c_str());

      auto response = createResponse(Status::CODE_200, result.DumpToString().c_str());
      response->putHeader(Header::CONTENT_TYPE, "application/json");
      return response;

    } catch (const std::exception& e) {
      auto response = createResponse(Status::CODE_500, e.what());
      response->putHeader(Header::CONTENT_TYPE, "application/json");
      return response;
    }
  }

  /**
   * @brief Get full-text index statistics
   * GET /api/{db}/tables/{table}/fulltext/stats
   */
  ENDPOINT("GET", "/api/{db}/tables/{table}/fulltext/stats",
           fulltextStats,
           PATH(String, db, "db"),
           PATH(String, table, "table")) {

    // Check if full-text search is enabled
    if (!IsFullTextEnabled()) {
      return CreateDisabledResponse();
    }

    try {
      // TODO: Get actual stats from Quickwit

      vectordb::Json result;
      result.SetInt("statusCode", 200);
      result.SetString("database", db->c_str());
      result.SetString("table", table->c_str());
      result.SetString("index_id", (std::string(db->c_str()) + "_" + std::string(table->c_str())).c_str());
      result.SetString("status", "available");

      auto response = createResponse(Status::CODE_200, result.DumpToString().c_str());
      response->putHeader(Header::CONTENT_TYPE, "application/json");
      return response;

    } catch (const std::exception& e) {
      auto response = createResponse(Status::CODE_500, e.what());
      response->putHeader(Header::CONTENT_TYPE, "application/json");
      return response;
    }
  }
};

#include OATPP_CODEGEN_END(ApiController)
