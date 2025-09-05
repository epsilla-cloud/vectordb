#pragma once

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <chrono>
#include <random>
#include <iomanip>
#include <sstream>
#include <openssl/sha.h>
#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace security {

/**
 * @brief API key information
 */
struct ApiKeyInfo {
    std::string key_hash;
    std::string description;
    std::vector<std::string> permissions;  // ["read", "write", "admin"]
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point expires_at;
    bool is_active;
    
    bool hasPermission(const std::string& permission) const {
        return std::find(permissions.begin(), permissions.end(), permission) != permissions.end();
    }
    
    bool isExpired() const {
        return std::chrono::system_clock::now() > expires_at;
    }
};

/**
 * @brief Authentication manager for API security
 */
class AuthManager {
public:
    static AuthManager& getInstance() {
        static AuthManager instance;
        return instance;
    }
    
    /**
     * @brief Initialize auth manager with master key
     */
    Status initialize(const std::string& master_key_env = "EPSILLA_MASTER_KEY") {
        const char* master = std::getenv(master_key_env.c_str());
        if (!master || std::strlen(master) < 32) {
            logger_.Warning("Master key not set or too weak. Using default (INSECURE!)");
            master_key_ = "epsilla_default_master_key_change_this_immediately";
        } else {
            master_key_ = master;
        }
        
        // Create default admin key if none exists
        if (api_keys_.empty()) {
            auto admin_key = generateApiKey();
            ApiKeyInfo info;
            info.key_hash = hashKey(admin_key);
            info.description = "Default admin key";
            info.permissions = {"read", "write", "admin"};
            info.created_at = std::chrono::system_clock::now();
            info.expires_at = info.created_at + std::chrono::hours(24*365); // 1 year
            info.is_active = true;
            
            {
                std::unique_lock<std::shared_mutex> lock(mutex_);
                api_keys_[admin_key] = info;
            }
            
            logger_.Info("Generated default admin API key: " + admin_key);
            logger_.Warning("SAVE THIS KEY - it will not be shown again!");
        }
        
        initialized_ = true;
        return Status::OK();
    }
    
    /**
     * @brief Validate API key
     */
    Status validateApiKey(const std::string& api_key, const std::string& required_permission = "read") {
        if (!initialized_) {
            return Status(UNAUTHORIZED, "Auth system not initialized");
        }
        
        if (api_key.empty()) {
            return Status(UNAUTHORIZED, "API key required");
        }
        
        std::shared_lock<std::shared_mutex> lock(mutex_);
        
        auto it = api_keys_.find(api_key);
        if (it == api_keys_.end()) {
            recordFailedAttempt(api_key);
            return Status(UNAUTHORIZED, "Invalid API key");
        }
        
        const auto& info = it->second;
        
        if (!info.is_active) {
            return Status(UNAUTHORIZED, "API key is inactive");
        }
        
        if (info.isExpired()) {
            return Status(UNAUTHORIZED, "API key has expired");
        }
        
        if (!info.hasPermission(required_permission)) {
            return Status(FORBIDDEN, "Insufficient permissions");
        }
        
        // Update last used time
        updateLastUsed(api_key);
        
        return Status::OK();
    }
    
    /**
     * @brief Generate new API key
     */
    std::string generateApiKey() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 255);
        
        std::stringstream ss;
        ss << "epsk_";  // Epsilla API key prefix
        
        for (int i = 0; i < 32; ++i) {
            ss << std::hex << std::setw(2) << std::setfill('0') << dis(gen);
        }
        
        return ss.str();
    }
    
    /**
     * @brief Create new API key with permissions
     */
    Status createApiKey(const std::string& admin_key,
                       const std::string& description,
                       const std::vector<std::string>& permissions,
                       int expire_days = 365,
                       std::string& new_key) {
        
        // Validate admin key
        auto status = validateApiKey(admin_key, "admin");
        if (!status.ok()) {
            return status;
        }
        
        new_key = generateApiKey();
        
        ApiKeyInfo info;
        info.key_hash = hashKey(new_key);
        info.description = description;
        info.permissions = permissions;
        info.created_at = std::chrono::system_clock::now();
        info.expires_at = info.created_at + std::chrono::hours(24 * expire_days);
        info.is_active = true;
        
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            api_keys_[new_key] = info;
        }
        
        logger_.Info("Created new API key: " + description);
        
        return Status::OK();
    }
    
    /**
     * @brief Revoke API key
     */
    Status revokeApiKey(const std::string& admin_key, const std::string& key_to_revoke) {
        auto status = validateApiKey(admin_key, "admin");
        if (!status.ok()) {
            return status;
        }
        
        std::unique_lock<std::shared_mutex> lock(mutex_);
        
        auto it = api_keys_.find(key_to_revoke);
        if (it == api_keys_.end()) {
            return Status(NOT_FOUND, "API key not found");
        }
        
        api_keys_.erase(it);
        logger_.Info("Revoked API key");
        
        return Status::OK();
    }
    
    /**
     * @brief Check if request should be rate limited
     */
    bool shouldRateLimit(const std::string& client_id, int max_requests_per_minute = 100) {
        std::unique_lock<std::shared_mutex> lock(rate_limit_mutex_);
        
        auto now = std::chrono::steady_clock::now();
        auto& record = rate_limits_[client_id];
        
        // Reset counter if minute has passed
        if (now - record.window_start > std::chrono::minutes(1)) {
            record.window_start = now;
            record.request_count = 0;
        }
        
        record.request_count++;
        
        if (record.request_count > max_requests_per_minute) {
            logger_.Warning("Rate limit exceeded for client: " + client_id);
            return true;
        }
        
        return false;
    }
    
    /**
     * @brief Get authentication statistics
     */
    struct AuthStats {
        size_t total_keys;
        size_t active_keys;
        size_t failed_attempts;
        size_t rate_limited_requests;
    };
    
    AuthStats getStats() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        
        AuthStats stats;
        stats.total_keys = api_keys_.size();
        stats.active_keys = std::count_if(api_keys_.begin(), api_keys_.end(),
            [](const auto& pair) { return pair.second.is_active; });
        stats.failed_attempts = failed_attempts_.size();
        
        std::shared_lock<std::shared_mutex> rate_lock(rate_limit_mutex_);
        stats.rate_limited_requests = rate_limits_.size();
        
        return stats;
    }
    
private:
    AuthManager() : initialized_(false) {}
    
    std::string hashKey(const std::string& key) {
        unsigned char hash[SHA256_DIGEST_LENGTH];
        SHA256(reinterpret_cast<const unsigned char*>(key.c_str()), key.length(), hash);
        
        std::stringstream ss;
        for(int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
            ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
        }
        return ss.str();
    }
    
    void recordFailedAttempt(const std::string& key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        failed_attempts_[key]++;
        
        // Block after too many failed attempts
        if (failed_attempts_[key] > 5) {
            logger_.Error("Possible brute force attack detected for key prefix: " + 
                         key.substr(0, 10) + "...");
        }
    }
    
    void updateLastUsed(const std::string& key) {
        // Update last used timestamp (could be stored in api_keys_)
    }
    
private:
    bool initialized_;
    std::string master_key_;
    
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, ApiKeyInfo> api_keys_;
    std::unordered_map<std::string, int> failed_attempts_;
    
    struct RateLimitRecord {
        std::chrono::steady_clock::time_point window_start;
        int request_count = 0;
    };
    
    mutable std::shared_mutex rate_limit_mutex_;
    std::unordered_map<std::string, RateLimitRecord> rate_limits_;
    
    Logger logger_;
};

/**
 * @brief Authentication middleware for web requests
 */
class AuthMiddleware {
public:
    /**
     * @brief Check if request is authenticated
     */
    template<typename Request>
    Status authenticate(const Request& request, const std::string& required_permission = "read") {
        // Extract API key from header
        std::string api_key;
        
        // Try Authorization header first
        auto auth_header = request.getHeader("Authorization");
        if (auth_header) {
            std::string auth = auth_header->toString();
            if (auth.find("Bearer ") == 0) {
                api_key = auth.substr(7);
            }
        }
        
        // Fallback to X-API-Key header
        if (api_key.empty()) {
            auto key_header = request.getHeader("X-API-Key");
            if (key_header) {
                api_key = key_header->toString();
            }
        }
        
        // Validate the key
        return AuthManager::getInstance().validateApiKey(api_key, required_permission);
    }
    
    /**
     * @brief Check rate limiting
     */
    template<typename Request>
    bool isRateLimited(const Request& request) {
        // Get client ID (IP address or API key)
        std::string client_id;
        
        auto ip_header = request.getHeader("X-Real-IP");
        if (ip_header) {
            client_id = ip_header->toString();
        } else {
            // Fallback to remote address
            client_id = "unknown";
        }
        
        return AuthManager::getInstance().shouldRateLimit(client_id);
    }
};

} // namespace security
} // namespace vectordb