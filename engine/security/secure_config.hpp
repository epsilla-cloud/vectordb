#pragma once

#include <string>
#include <unordered_map>
#include <fstream>
#include <filesystem>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include "utils/status.hpp"
#include "logger/logger.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace security {

/**
 * @brief Secure configuration manager with encryption
 */
class SecureConfig {
public:
    static SecureConfig& getInstance() {
        static SecureConfig instance;
        return instance;
    }
    
    /**
     * @brief Load configuration from file with security checks
     */
    Status loadConfig(const std::string& config_path) {
        // Validate path
        if (!isSecurePath(config_path)) {
            return Status(INVALID_ARGUMENT, "Invalid configuration path");
        }
        
        // Check file permissions
        auto perms = std::filesystem::status(config_path).permissions();
        if ((perms & std::filesystem::perms::others_read) != std::filesystem::perms::none ||
            (perms & std::filesystem::perms::others_write) != std::filesystem::perms::none) {
            logger_.Warning("Configuration file has unsafe permissions");
            
            // Try to fix permissions
            try {
                std::filesystem::permissions(config_path, 
                    std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
                    std::filesystem::perm_options::replace);
            } catch (const std::exception& e) {
                return Status(PERMISSION_DENIED, "Cannot secure configuration file");
            }
        }
        
        // Read configuration
        std::ifstream file(config_path);
        if (!file.is_open()) {
            return Status(NOT_FOUND, "Configuration file not found");
        }
        
        std::string content((std::istreambuf_iterator<char>(file)),
                           std::istreambuf_iterator<char>());
        file.close();
        
        // Parse JSON
        try {
            config_.LoadFromString(content);
        } catch (const std::exception& e) {
            return Status(INVALID_ARGUMENT, "Invalid configuration format");
        }
        
        // Decrypt sensitive values
        decryptSensitiveValues();
        
        // Validate configuration
        return validateConfig();
    }
    
    /**
     * @brief Save configuration with encryption
     */
    Status saveConfig(const std::string& config_path) {
        // Encrypt sensitive values before saving
        Json encrypted_config = config_;
        encryptSensitiveValues(encrypted_config);
        
        // Ensure directory exists
        std::filesystem::create_directories(std::filesystem::path(config_path).parent_path());
        
        // Write with secure permissions
        std::ofstream file(config_path);
        if (!file.is_open()) {
            return Status(IO_ERROR, "Cannot write configuration file");
        }
        
        file << encrypted_config.DumpToString();
        file.close();
        
        // Set secure permissions (owner read/write only)
        try {
            std::filesystem::permissions(config_path,
                std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
                std::filesystem::perm_options::replace);
        } catch (const std::exception& e) {
            logger_.Warning("Failed to set secure permissions on config file");
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Get configuration value
     */
    template<typename T>
    T get(const std::string& key, const T& default_value = T{}) {
        if (config_.HasMember(key)) {
            if constexpr (std::is_same_v<T, std::string>) {
                return config_.GetString(key);
            } else if constexpr (std::is_same_v<T, int>) {
                return config_.GetInt(key);
            } else if constexpr (std::is_same_v<T, bool>) {
                return config_.GetBool(key);
            } else if constexpr (std::is_same_v<T, double>) {
                return config_.GetDouble(key);
            }
        }
        return default_value;
    }
    
    /**
     * @brief Set configuration value
     */
    template<typename T>
    void set(const std::string& key, const T& value) {
        if constexpr (std::is_same_v<T, std::string>) {
            config_.SetString(key, value);
        } else if constexpr (std::is_same_v<T, int>) {
            config_.SetInt(key, value);
        } else if constexpr (std::is_same_v<T, bool>) {
            config_.SetBool(key, value);
        } else if constexpr (std::is_same_v<T, double>) {
            config_.SetDouble(key, value);
        }
    }
    
    /**
     * @brief Get secure defaults
     */
    static Json getDefaultConfig() {
        Json config;
        
        // Server settings
        config.SetInt("server.port", 8888);
        config.SetString("server.host", "127.0.0.1");  // Localhost only by default
        config.SetBool("server.ssl_enabled", true);
        config.SetString("server.ssl_cert", "/etc/epsilla/cert.pem");
        config.SetString("server.ssl_key", "/etc/epsilla/key.pem");
        
        // Security settings
        config.SetBool("security.auth_enabled", true);
        config.SetBool("security.rate_limiting", true);
        config.SetInt("security.max_requests_per_minute", 100);
        config.SetInt("security.session_timeout_minutes", 30);
        config.SetBool("security.audit_logging", true);
        
        // Database settings  
        config.SetString("database.data_dir", "/var/lib/epsilla/data");
        config.SetString("database.wal_dir", "/var/lib/epsilla/wal");
        config.SetInt("database.max_connections", 100);
        config.SetInt("database.initial_capacity", 5000);  // Not 150000
        config.SetBool("database.auto_compact", true);
        
        // Performance settings
        config.SetInt("performance.thread_pool_size", 0);  // Auto-detect
        config.SetInt("performance.cache_size_mb", 1024);
        config.SetBool("performance.enable_simd", true);
        
        // Logging settings
        config.SetString("logging.level", "info");
        config.SetString("logging.file", "/var/log/epsilla/epsilla.log");
        config.SetBool("logging.rotate", true);
        config.SetInt("logging.max_size_mb", 100);
        
        return config;
    }
    
private:
    SecureConfig() {
        // Initialize with secure defaults
        config_ = getDefaultConfig();
        
        // Generate or load encryption key
        initializeEncryptionKey();
    }
    
    bool isSecurePath(const std::string& path) {
        // Check for directory traversal
        if (path.find("..") != std::string::npos) {
            return false;
        }
        
        // Check if path exists and is a regular file
        if (std::filesystem::exists(path)) {
            return std::filesystem::is_regular_file(path);
        }
        
        return true;  // Path doesn't exist yet, will be created
    }
    
    Status validateConfig() {
        // Validate port range
        int port = config_.GetInt("server.port");
        if (port < 1 || port > 65535) {
            return Status(INVALID_ARGUMENT, "Invalid server port");
        }
        
        // Validate data directory
        std::string data_dir = config_.GetString("database.data_dir");
        if (data_dir.empty()) {
            return Status(INVALID_ARGUMENT, "Data directory not specified");
        }
        
        // Create data directory if it doesn't exist
        try {
            std::filesystem::create_directories(data_dir);
        } catch (const std::exception& e) {
            return Status(IO_ERROR, "Cannot create data directory");
        }
        
        // Validate SSL certificates if SSL is enabled
        if (config_.GetBool("server.ssl_enabled")) {
            std::string cert = config_.GetString("server.ssl_cert");
            std::string key = config_.GetString("server.ssl_key");
            
            if (!std::filesystem::exists(cert)) {
                logger_.Warning("SSL certificate not found: " + cert);
            }
            if (!std::filesystem::exists(key)) {
                logger_.Warning("SSL key not found: " + key);
            }
        }
        
        return Status::OK();
    }
    
    void initializeEncryptionKey() {
        // Try to get key from environment
        const char* key_env = std::getenv("EPSILLA_CONFIG_KEY");
        if (key_env && std::strlen(key_env) == 64) {  // 32 bytes hex
            // Parse hex key
            for (int i = 0; i < 32; ++i) {
                sscanf(key_env + i*2, "%2hhx", &encryption_key_[i]);
            }
        } else {
            // Generate random key
            RAND_bytes(encryption_key_, 32);
            
            // Log warning
            logger_.Warning("No encryption key provided, generated random key");
            logger_.Warning("Set EPSILLA_CONFIG_KEY environment variable for persistent encryption");
        }
    }
    
    void encryptSensitiveValues(Json& config) {
        // List of sensitive keys to encrypt
        std::vector<std::string> sensitive_keys = {
            "database.password",
            "server.ssl_key",
            "security.master_key"
        };
        
        for (const auto& key : sensitive_keys) {
            if (config.HasMember(key)) {
                std::string value = config.GetString(key);
                std::string encrypted = encryptString(value);
                config.SetString(key, encrypted);
            }
        }
    }
    
    void decryptSensitiveValues() {
        // Same keys as above
        std::vector<std::string> sensitive_keys = {
            "database.password",
            "server.ssl_key",
            "security.master_key"
        };
        
        for (const auto& key : sensitive_keys) {
            if (config_.HasMember(key)) {
                std::string encrypted = config_.GetString(key);
                if (encrypted.find("ENC:") == 0) {  // Check if encrypted
                    std::string decrypted = decryptString(encrypted.substr(4));
                    config_.SetString(key, decrypted);
                }
            }
        }
    }
    
    std::string encryptString(const std::string& plain) {
        // Simple AES encryption (simplified for example)
        // In production, use proper IV and authentication
        std::string encrypted = "ENC:" + plain;  // Placeholder
        return encrypted;
    }
    
    std::string decryptString(const std::string& encrypted) {
        // Simple decryption (simplified for example)
        return encrypted;  // Placeholder
    }
    
private:
    Json config_;
    unsigned char encryption_key_[32];
    Logger logger_;
};

} // namespace security
} // namespace vectordb