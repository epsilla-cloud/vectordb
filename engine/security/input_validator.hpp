#pragma once

#include <string>
#include <vector>
#include <regex>
#include <algorithm>
#include <cctype>
#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace security {

/**
 * @brief Input validation utilities to prevent injection attacks
 */
class InputValidator {
public:
    /**
     * @brief Validate database name
     */
    static Status validateDatabaseName(const std::string& name) {
        if (name.empty() || name.length() > 64) {
            return Status(INVALID_ARGUMENT, "Database name must be 1-64 characters");
        }
        
        // Only allow alphanumeric, underscore, and hyphen
        std::regex pattern("^[a-zA-Z][a-zA-Z0-9_-]*$");
        if (!std::regex_match(name, pattern)) {
            return Status(INVALID_ARGUMENT, 
                "Database name must start with letter and contain only alphanumeric, underscore, hyphen");
        }
        
        // Check for SQL keywords
        if (containsSQLKeyword(name)) {
            return Status(INVALID_ARGUMENT, "Database name contains SQL keyword");
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Validate table name
     */
    static Status validateTableName(const std::string& name) {
        if (name.empty() || name.length() > 64) {
            return Status(INVALID_ARGUMENT, "Table name must be 1-64 characters");
        }
        
        std::regex pattern("^[a-zA-Z][a-zA-Z0-9_-]*$");
        if (!std::regex_match(name, pattern)) {
            return Status(INVALID_ARGUMENT, 
                "Table name must start with letter and contain only alphanumeric, underscore, hyphen");
        }
        
        if (containsSQLKeyword(name)) {
            return Status(INVALID_ARGUMENT, "Table name contains SQL keyword");
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Validate field name
     */
    static Status validateFieldName(const std::string& name) {
        if (name.empty() || name.length() > 64) {
            return Status(INVALID_ARGUMENT, "Field name must be 1-64 characters");
        }
        
        std::regex pattern("^[a-zA-Z][a-zA-Z0-9_]*$");
        if (!std::regex_match(name, pattern)) {
            return Status(INVALID_ARGUMENT, 
                "Field name must start with letter and contain only alphanumeric and underscore");
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Validate and sanitize file path
     */
    static Status validateFilePath(const std::string& path, std::string& sanitized) {
        if (path.empty() || path.length() > 4096) {
            return Status(INVALID_ARGUMENT, "Invalid path length");
        }
        
        // Check for directory traversal attempts
        if (path.find("..") != std::string::npos) {
            return Status(INVALID_ARGUMENT, "Path traversal detected");
        }
        
        // Check for null bytes
        if (path.find('\0') != std::string::npos) {
            return Status(INVALID_ARGUMENT, "Null byte in path");
        }
        
        // Remove dangerous characters
        sanitized = path;
        
        // Replace backslashes with forward slashes
        std::replace(sanitized.begin(), sanitized.end(), '\\', '/');
        
        // Remove duplicate slashes
        size_t pos = 0;
        while ((pos = sanitized.find("//", pos)) != std::string::npos) {
            sanitized.erase(pos, 1);
        }
        
        // Must be relative path or absolute path within data directory
        if (sanitized[0] == '/' && !isWithinDataDirectory(sanitized)) {
            return Status(INVALID_ARGUMENT, "Path must be within data directory");
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Validate numeric value
     */
    template<typename T>
    static Status validateNumericRange(T value, T min_val, T max_val, const std::string& field_name) {
        if (value < min_val || value > max_val) {
            return Status(INVALID_ARGUMENT, 
                field_name + " must be between " + std::to_string(min_val) + 
                " and " + std::to_string(max_val));
        }
        return Status::OK();
    }
    
    /**
     * @brief Validate vector dimensions
     */
    static Status validateVectorDimensions(size_t dims) {
        if (dims < 1 || dims > 65536) {
            return Status(INVALID_ARGUMENT, "Vector dimensions must be between 1 and 65536");
        }
        return Status::OK();
    }
    
    /**
     * @brief Validate JSON query
     */
    static Status validateJsonQuery(const std::string& json) {
        if (json.empty() || json.length() > 1048576) {  // 1MB limit
            return Status(INVALID_ARGUMENT, "JSON query size invalid");
        }
        
        // Basic JSON validation (check for balanced braces)
        int brace_count = 0;
        int bracket_count = 0;
        bool in_string = false;
        char prev = '\0';
        
        for (char c : json) {
            if (c == '"' && prev != '\\') {
                in_string = !in_string;
            }
            
            if (!in_string) {
                if (c == '{') brace_count++;
                else if (c == '}') brace_count--;
                else if (c == '[') bracket_count++;
                else if (c == ']') bracket_count--;
                
                if (brace_count < 0 || bracket_count < 0) {
                    return Status(INVALID_ARGUMENT, "Invalid JSON structure");
                }
            }
            
            prev = c;
        }
        
        if (brace_count != 0 || bracket_count != 0) {
            return Status(INVALID_ARGUMENT, "Unbalanced JSON braces/brackets");
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Sanitize string for logging (remove sensitive data)
     */
    static std::string sanitizeForLogging(const std::string& str) {
        std::string result = str;
        
        // Remove potential passwords
        std::regex pwd_pattern("(password|passwd|pwd|secret|token|key)\\s*[=:]\\s*['\"]?([^'\"\\s]+)");
        result = std::regex_replace(result, pwd_pattern, "$1=***REDACTED***");
        
        // Remove potential API keys
        std::regex key_pattern("(epsk_[a-f0-9]{64})");
        result = std::regex_replace(result, key_pattern, "epsk_***REDACTED***");
        
        // Truncate if too long
        if (result.length() > 1000) {
            result = result.substr(0, 1000) + "...[truncated]";
        }
        
        return result;
    }
    
    /**
     * @brief Validate HTTP header value
     */
    static Status validateHeaderValue(const std::string& value) {
        // Check for header injection
        if (value.find('\r') != std::string::npos || 
            value.find('\n') != std::string::npos) {
            return Status(INVALID_ARGUMENT, "Header injection detected");
        }
        
        // Check length
        if (value.length() > 8192) {
            return Status(INVALID_ARGUMENT, "Header value too long");
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Escape special characters for safe output
     */
    static std::string escapeHtml(const std::string& str) {
        std::string result;
        result.reserve(str.length() * 1.2);
        
        for (char c : str) {
            switch (c) {
                case '<': result += "&lt;"; break;
                case '>': result += "&gt;"; break;
                case '&': result += "&amp;"; break;
                case '"': result += "&quot;"; break;
                case '\'': result += "&#39;"; break;
                default: result += c; break;
            }
        }
        
        return result;
    }
    
    /**
     * @brief Validate query parameters
     */
    static Status validateQueryParam(const std::string& name, const std::string& value) {
        // Validate parameter name
        if (name.empty() || name.length() > 128) {
            return Status(INVALID_ARGUMENT, "Invalid parameter name");
        }
        
        // Validate parameter value
        if (value.length() > 4096) {
            return Status(INVALID_ARGUMENT, "Parameter value too long");
        }
        
        // Check for common injection patterns
        std::vector<std::string> dangerous_patterns = {
            "<script", "javascript:", "onerror=", "onclick=",
            "DROP TABLE", "DELETE FROM", "INSERT INTO", "UPDATE SET",
            "../", "..\\", "%2e%2e", "0x", "\\x"
        };
        
        std::string lower_value = value;
        std::transform(lower_value.begin(), lower_value.end(), lower_value.begin(), ::tolower);
        
        for (const auto& pattern : dangerous_patterns) {
            if (lower_value.find(pattern) != std::string::npos) {
                Logger().Warning("Potential injection attempt detected: " + pattern);
                return Status(INVALID_ARGUMENT, "Dangerous pattern detected");
            }
        }
        
        return Status::OK();
    }
    
private:
    static bool containsSQLKeyword(const std::string& str) {
        static const std::vector<std::string> sql_keywords = {
            "SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
            "TABLE", "DATABASE", "INDEX", "VIEW", "PROCEDURE", "FUNCTION",
            "TRIGGER", "UNION", "JOIN", "WHERE", "FROM", "INTO", "VALUES",
            "EXEC", "EXECUTE", "SCRIPT", "JAVASCRIPT", "EVAL"
        };
        
        std::string upper = str;
        std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
        
        for (const auto& keyword : sql_keywords) {
            if (upper == keyword) {
                return true;
            }
        }
        
        return false;
    }
    
    static bool isWithinDataDirectory(const std::string& path) {
        // Get configured data directory
        const char* data_dir = std::getenv("EPSILLA_DATA_DIR");
        if (!data_dir) {
            data_dir = "/var/lib/epsilla";
        }
        
        // Resolve absolute path and check if it starts with data directory
        return path.find(data_dir) == 0;
    }
};

/**
 * @brief Request sanitizer for web endpoints
 */
class RequestSanitizer {
public:
    template<typename Request>
    static Status sanitizeRequest(Request& request) {
        // Sanitize path parameters
        auto path = request.getPathPattern();
        if (path.length() > 2048) {
            return Status(INVALID_ARGUMENT, "Path too long");
        }
        
        // Sanitize query parameters
        auto query_params = request.getQueryParameters();
        for (const auto& [name, value] : query_params) {
            auto status = InputValidator::validateQueryParam(name, value);
            if (!status.ok()) {
                return status;
            }
        }
        
        // Sanitize headers
        auto headers = request.getHeaders();
        for (const auto& [name, value] : headers) {
            auto status = InputValidator::validateHeaderValue(value);
            if (!status.ok()) {
                return status;
            }
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Sanitize response before sending
     */
    template<typename Response>
    static void sanitizeResponse(Response& response) {
        // Remove sensitive headers
        response.removeHeader("Server");
        response.removeHeader("X-Powered-By");
        
        // Add security headers
        response.putHeader("X-Content-Type-Options", "nosniff");
        response.putHeader("X-Frame-Options", "DENY");
        response.putHeader("X-XSS-Protection", "1; mode=block");
        response.putHeader("Content-Security-Policy", "default-src 'self'");
    }
};

} // namespace security
} // namespace vectordb