#pragma once

#include OATPP_CODEGEN_BEGIN(ApiController)

#include <oatpp/web/server/api/ApiController.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>

#include "security/auth_manager.hpp"
#include "security/input_validator.hpp"
#include "db/db_server.hpp"
#include "utils/json.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace web {

/**
 * @brief Secure Web Controller with authentication and input validation
 */
class SecureWebController : public oatpp::web::server::api::ApiController {
public:
    SecureWebController(const std::shared_ptr<ObjectMapper>& objectMapper)
        : ApiController(objectMapper) {
        
        // Initialize authentication
        security::AuthManager::getInstance().initialize();
    }

    static std::shared_ptr<SecureWebController> createShared(
        OATPP_COMPONENT(std::shared_ptr<ObjectMapper>, objectMapper)) {
        return std::make_shared<SecureWebController>(objectMapper);
    }

    /**
     * @brief Health check endpoint (no auth required)
     */
    ENDPOINT("GET", "/health", health) {
        auto dto = StatusDto::createShared();
        dto->statusCode = 200;
        dto->message = "OK";
        return createDtoResponse(Status::CODE_200, dto);
    }

    /**
     * @brief Load database (requires admin permission)
     */
    ENDPOINT("POST", "/database/{db}/load", loadDatabase,
             PATH(String, db),
             HEADER(String, apiKey, "X-API-Key"),
             BODY_STRING(String, body)) {
        
        auto dto = StatusDto::createShared();
        
        // Authenticate
        auto auth_status = security::AuthManager::getInstance().validateApiKey(apiKey, "admin");
        if (!auth_status.ok()) {
            dto->statusCode = 401;
            dto->message = auth_status.message();
            return createDtoResponse(Status::CODE_401, dto);
        }
        
        // Validate database name
        auto validation_status = security::InputValidator::validateDatabaseName(db);
        if (!validation_status.ok()) {
            dto->statusCode = 400;
            dto->message = validation_status.message();
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        // Validate JSON body
        auto json_status = security::InputValidator::validateJsonQuery(body);
        if (!json_status.ok()) {
            dto->statusCode = 400;
            dto->message = json_status.message();
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        // Parse and process request
        Json parsedBody;
        try {
            parsedBody.LoadFromString(body);
        } catch (const std::exception& e) {
            dto->statusCode = 400;
            dto->message = "Invalid JSON: " + std::string(e.what());
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        // Get database path with validation
        std::string db_path = parsedBody.GetString("path");
        std::string sanitized_path;
        auto path_status = security::InputValidator::validateFilePath(db_path, sanitized_path);
        if (!path_status.ok()) {
            dto->statusCode = 400;
            dto->message = path_status.message();
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        // Get other parameters with validation
        int64_t init_table_scale = 5000;  // Default to 5000, not 150000
        if (parsedBody.HasMember("vectorScale")) {
            init_table_scale = parsedBody.GetInt("vectorScale");
            auto range_status = security::InputValidator::validateNumericRange<int64_t>(
                init_table_scale, 100, 100000000, "vectorScale");
            if (!range_status.ok()) {
                dto->statusCode = 400;
                dto->message = range_status.message();
                return createDtoResponse(Status::CODE_400, dto);
            }
        }
        
        bool wal_enabled = parsedBody.GetBool("walEnabled", true);
        
        // Load database
        std::unordered_map<std::string, std::string> headers;
        auto status = db_server_->LoadDB(db, sanitized_path, init_table_scale, wal_enabled, headers);
        
        if (status.ok()) {
            dto->statusCode = 200;
            dto->message = "Database loaded successfully";
            logger_.Info("Database loaded: " + db + " by API key: " + apiKey.substr(0, 10) + "...");
        } else {
            dto->statusCode = 500;
            dto->message = status.message();
            logger_.Error("Failed to load database: " + status.message());
        }
        
        return createDtoResponse(Status::CODE_200, dto);
    }

    /**
     * @brief Create table (requires write permission)
     */
    ENDPOINT("POST", "/{db}/api/v2/create/table", createTable,
             PATH(String, db),
             HEADER(String, apiKey, "X-API-Key"),
             BODY_STRING(String, body)) {
        
        auto dto = StatusDto::createShared();
        
        // Authenticate
        auto auth_status = security::AuthManager::getInstance().validateApiKey(apiKey, "write");
        if (!auth_status.ok()) {
            dto->statusCode = 401;
            dto->message = auth_status.message();
            return createDtoResponse(Status::CODE_401, dto);
        }
        
        // Check rate limiting
        security::AuthMiddleware middleware;
        if (middleware.isRateLimited(*request)) {
            dto->statusCode = 429;
            dto->message = "Too many requests";
            return createDtoResponse(Status::CODE_429, dto);
        }
        
        // Validate inputs
        auto db_status = security::InputValidator::validateDatabaseName(db);
        if (!db_status.ok()) {
            dto->statusCode = 400;
            dto->message = db_status.message();
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        auto json_status = security::InputValidator::validateJsonQuery(body);
        if (!json_status.ok()) {
            dto->statusCode = 400;
            dto->message = json_status.message();
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        // Parse table schema
        Json parsedBody;
        try {
            parsedBody.LoadFromString(body);
        } catch (const std::exception& e) {
            dto->statusCode = 400;
            dto->message = "Invalid JSON: " + std::string(e.what());
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        // Validate table name
        std::string table_name = parsedBody.GetString("name");
        auto table_status = security::InputValidator::validateTableName(table_name);
        if (!table_status.ok()) {
            dto->statusCode = 400;
            dto->message = table_status.message();
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        // Validate fields
        if (parsedBody.HasMember("fields")) {
            auto fields = parsedBody.GetArray("fields");
            for (const auto& field : fields) {
                std::string field_name = field.GetString("name");
                auto field_status = security::InputValidator::validateFieldName(field_name);
                if (!field_status.ok()) {
                    dto->statusCode = 400;
                    dto->message = "Invalid field name: " + field_name;
                    return createDtoResponse(Status::CODE_400, dto);
                }
                
                // Validate vector dimensions if present
                if (field.HasMember("dimension")) {
                    size_t dims = field.GetInt("dimension");
                    auto dim_status = security::InputValidator::validateVectorDimensions(dims);
                    if (!dim_status.ok()) {
                        dto->statusCode = 400;
                        dto->message = dim_status.message();
                        return createDtoResponse(Status::CODE_400, dto);
                    }
                }
            }
        }
        
        // Create table
        std::unordered_map<std::string, std::string> headers;
        auto status = db_server_->CreateTable(db, parsedBody, headers);
        
        if (status.ok()) {
            dto->statusCode = 200;
            dto->message = "Table created successfully";
            
            // Log sanitized information
            std::string log_msg = "Table created: " + db + "." + table_name;
            logger_.Info(security::InputValidator::sanitizeForLogging(log_msg));
        } else {
            dto->statusCode = 500;
            dto->message = status.message();
        }
        
        // Add security headers to response
        response->putHeader("X-Content-Type-Options", "nosniff");
        response->putHeader("X-Frame-Options", "DENY");
        
        return createDtoResponse(Status::CODE_200, dto);
    }

    /**
     * @brief Query data (requires read permission)
     */
    ENDPOINT("POST", "/{db}/api/v2/search", search,
             PATH(String, db),
             HEADER(String, apiKey, "X-API-Key"),
             BODY_STRING(String, body)) {
        
        auto dto = StatusDto::createShared();
        
        // Authenticate
        auto auth_status = security::AuthManager::getInstance().validateApiKey(apiKey, "read");
        if (!auth_status.ok()) {
            dto->statusCode = 401;
            dto->message = auth_status.message();
            return createDtoResponse(Status::CODE_401, dto);
        }
        
        // Validate and process query
        auto db_status = security::InputValidator::validateDatabaseName(db);
        if (!db_status.ok()) {
            dto->statusCode = 400;
            dto->message = db_status.message();
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        auto json_status = security::InputValidator::validateJsonQuery(body);
        if (!json_status.ok()) {
            dto->statusCode = 400;
            dto->message = json_status.message();
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        // Execute search with sanitized inputs
        Json parsedQuery;
        try {
            parsedQuery.LoadFromString(body);
        } catch (const std::exception& e) {
            dto->statusCode = 400;
            dto->message = "Invalid query JSON";
            return createDtoResponse(Status::CODE_400, dto);
        }
        
        // Perform search (implementation depends on your search logic)
        Json response;
        std::unordered_map<std::string, std::string> headers;
        auto status = db_server_->Search(db, parsedQuery, response, headers);
        
        if (status.ok()) {
            return createResponse(Status::CODE_200, response.DumpToString());
        } else {
            dto->statusCode = 500;
            dto->message = status.message();
            return createDtoResponse(Status::CODE_500, dto);
        }
    }

    /**
     * @brief Get API key statistics (admin only)
     */
    ENDPOINT("GET", "/admin/auth/stats", getAuthStats,
             HEADER(String, apiKey, "X-API-Key")) {
        
        auto dto = StatusDto::createShared();
        
        // Require admin permission
        auto auth_status = security::AuthManager::getInstance().validateApiKey(apiKey, "admin");
        if (!auth_status.ok()) {
            dto->statusCode = 401;
            dto->message = auth_status.message();
            return createDtoResponse(Status::CODE_401, dto);
        }
        
        // Get statistics
        auto stats = security::AuthManager::getInstance().getStats();
        
        Json response;
        response.SetInt("total_keys", stats.total_keys);
        response.SetInt("active_keys", stats.active_keys);
        response.SetInt("failed_attempts", stats.failed_attempts);
        response.SetInt("rate_limited_requests", stats.rate_limited_requests);
        
        return createResponse(Status::CODE_200, response.DumpToString());
    }

private:
    std::shared_ptr<DBServer> db_server_;
    Logger logger_;
};

#include OATPP_CODEGEN_END(ApiController)

} // namespace web
} // namespace vectordb