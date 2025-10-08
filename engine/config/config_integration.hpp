#pragma once

#include "config/config.hpp"
#include "config/advanced_config.hpp"
#include "utils/enhanced_error.hpp"

namespace vectordb {
namespace integration {

/**
 * @brief Integration adapter that bridges old and new config systems
 */
class ConfigIntegration {
private:
  Config& legacy_config_;
  config::AdvancedConfigManager& advanced_config_;
  vectordb::engine::Logger logger_;
  
public:
  explicit ConfigIntegration(Config& legacy_config) 
    : legacy_config_(legacy_config), advanced_config_(config::GetAdvancedConfig()) {
    setupIntegration();
  }

  /**
   * @brief Setup bidirectional synchronization between old and new config systems
   */
  void setupIntegration() {
    // Register callbacks to sync from advanced config to legacy config
    registerAdvancedToLegacyCallbacks();
    
    // Initialize legacy config values from advanced config
    syncAdvancedToLegacy();
    
    logger_.Info("Configuration integration initialized");
  }

  /**
   * @brief Push legacy config changes to advanced config
   */
  void pushLegacyToAdvanced() {
    try {
      advanced_config_.setValue("IntraQueryThreads", legacy_config_.IntraQueryThreads.load());
      advanced_config_.setValue("RebuildThreads", legacy_config_.RebuildThreads.load());
      advanced_config_.setValue("NumExecutorPerField", legacy_config_.NumExecutorPerField.load());
      advanced_config_.setValue("MasterQueueSize", legacy_config_.MasterQueueSize.load());
      advanced_config_.setValue("LocalQueueSize", legacy_config_.LocalQueueSize.load());
      advanced_config_.setValue("GlobalSyncInterval", legacy_config_.GlobalSyncInterval.load());
      advanced_config_.setValue("MinimalGraphSize", legacy_config_.MinimalGraphSize.load());
      advanced_config_.setValue("PreFilter", legacy_config_.PreFilter.load());
      
      logger_.Info("Pushed legacy configuration values to advanced config");
    } catch (const std::exception& e) {
      logger_.Error("Failed to push legacy values to advanced config: " + std::string(e.what()));
    }
  }

  /**
   * @brief Create example configuration files
   */
  void createExampleConfigs() {
    createExampleYaml();
    createExampleToml();
    createExampleJson();
  }

private:
  void registerAdvancedToLegacyCallbacks() {
    // IntraQueryThreads
    advanced_config_.registerCallback("IntraQueryThreads", 
      [this](const config::ConfigChangeEvent& event) {
        try {
          int value = std::stoi(event.new_value);
          legacy_config_.setIntraQueryThreads(value);
          logger_.Info("Synchronized IntraQueryThreads: " + event.new_value);
        } catch (const std::exception& e) {
          logger_.Error("Failed to sync IntraQueryThreads: " + std::string(e.what()));
        }
      });

    // RebuildThreads
    advanced_config_.registerCallback("RebuildThreads",
      [this](const config::ConfigChangeEvent& event) {
        try {
          int value = std::stoi(event.new_value);
          legacy_config_.setRebuildThreads(value);
          logger_.Info("Synchronized RebuildThreads: " + event.new_value);
        } catch (const std::exception& e) {
          logger_.Error("Failed to sync RebuildThreads: " + std::string(e.what()));
        }
      });

    // NumExecutorPerField
    advanced_config_.registerCallback("NumExecutorPerField",
      [this](const config::ConfigChangeEvent& event) {
        try {
          int value = std::stoi(event.new_value);
          legacy_config_.setNumExecutorPerField(value);
          logger_.Info("Synchronized NumExecutorPerField: " + event.new_value);
        } catch (const std::exception& e) {
          logger_.Error("Failed to sync NumExecutorPerField: " + std::string(e.what()));
        }
      });

    // SearchQueueSize (affects both MasterQueueSize and LocalQueueSize)
    advanced_config_.registerCallback("SearchQueueSize",
      [this](const config::ConfigChangeEvent& event) {
        try {
          int value = std::stoi(event.new_value);
          legacy_config_.setSearchQueueSize(value);
          logger_.Info("Synchronized SearchQueueSize: " + event.new_value);
        } catch (const std::exception& e) {
          logger_.Error("Failed to sync SearchQueueSize: " + std::string(e.what()));
        }
      });

    // MasterQueueSize
    advanced_config_.registerCallback("MasterQueueSize",
      [this](const config::ConfigChangeEvent& event) {
        try {
          int value = std::stoi(event.new_value);
          legacy_config_.MasterQueueSize.store(value);
          logger_.Info("Synchronized MasterQueueSize: " + event.new_value);
        } catch (const std::exception& e) {
          logger_.Error("Failed to sync MasterQueueSize: " + std::string(e.what()));
        }
      });

    // LocalQueueSize
    advanced_config_.registerCallback("LocalQueueSize",
      [this](const config::ConfigChangeEvent& event) {
        try {
          int value = std::stoi(event.new_value);
          legacy_config_.LocalQueueSize.store(value);
          logger_.Info("Synchronized LocalQueueSize: " + event.new_value);
        } catch (const std::exception& e) {
          logger_.Error("Failed to sync LocalQueueSize: " + std::string(e.what()));
        }
      });

    // GlobalSyncInterval
    advanced_config_.registerCallback("GlobalSyncInterval",
      [this](const config::ConfigChangeEvent& event) {
        try {
          int value = std::stoi(event.new_value);
          legacy_config_.GlobalSyncInterval.store(value);
          logger_.Info("Synchronized GlobalSyncInterval: " + event.new_value);
        } catch (const std::exception& e) {
          logger_.Error("Failed to sync GlobalSyncInterval: " + std::string(e.what()));
        }
      });

    // MinimalGraphSize
    advanced_config_.registerCallback("MinimalGraphSize",
      [this](const config::ConfigChangeEvent& event) {
        try {
          int value = std::stoi(event.new_value);
          legacy_config_.MinimalGraphSize.store(value);
          logger_.Info("Synchronized MinimalGraphSize: " + event.new_value);
        } catch (const std::exception& e) {
          logger_.Error("Failed to sync MinimalGraphSize: " + std::string(e.what()));
        }
      });

    // PreFilter
    advanced_config_.registerCallback("PreFilter",
      [this](const config::ConfigChangeEvent& event) {
        try {
          bool value = (event.new_value == "true" || event.new_value == "1");
          legacy_config_.PreFilter.store(value);
          logger_.Info("Synchronized PreFilter: " + event.new_value);
        } catch (const std::exception& e) {
          logger_.Error("Failed to sync PreFilter: " + std::string(e.what()));
        }
      });
  }

  void syncAdvancedToLegacy() {
    // Sync current values from advanced config to legacy config
    try {
      // Core thread configuration
      int threads = advanced_config_.getValue<int>("IntraQueryThreads", 
                                                  legacy_config_.IntraQueryThreads.load());
      legacy_config_.setIntraQueryThreads(threads);
      
      int rebuild = advanced_config_.getValue<int>("RebuildThreads", 
                                                  legacy_config_.RebuildThreads.load());
      legacy_config_.setRebuildThreads(rebuild);
      
      int executors = advanced_config_.getValue<int>("NumExecutorPerField", 
                                                    legacy_config_.NumExecutorPerField.load());
      legacy_config_.setNumExecutorPerField(executors);
      
      // Queue configuration
      int masterQueue = advanced_config_.getValue<int>("MasterQueueSize", 
                                                       legacy_config_.MasterQueueSize.load());
      legacy_config_.MasterQueueSize.store(masterQueue);
      
      int localQueue = advanced_config_.getValue<int>("LocalQueueSize", 
                                                      legacy_config_.LocalQueueSize.load());
      legacy_config_.LocalQueueSize.store(localQueue);
      
      // Performance tuning
      int syncInterval = advanced_config_.getValue<int>("GlobalSyncInterval", 
                                                        legacy_config_.GlobalSyncInterval.load());
      legacy_config_.GlobalSyncInterval.store(syncInterval);
      
      int minGraphSize = advanced_config_.getValue<int>("MinimalGraphSize", 
                                                        legacy_config_.MinimalGraphSize.load());
      legacy_config_.MinimalGraphSize.store(minGraphSize);
      
      bool prefilter = advanced_config_.getValue<bool>("PreFilter", 
                                                      legacy_config_.PreFilter.load());
      legacy_config_.PreFilter.store(prefilter);
      
      logger_.Info("Successfully synchronized all configuration values from advanced to legacy config");
      
    } catch (const std::exception& e) {
      logger_.Error("Failed to sync initial values: " + std::string(e.what()));
    }
  }

  void createExampleYaml() {
    const std::string yaml_content = R"(# VectorDB Configuration (YAML format)
# This file demonstrates all available configuration options

# Core Thread Configuration
IntraQueryThreads: 16          # Number of threads for query processing (1-128)
RebuildThreads: 4              # Number of threads for index rebuilding (1-128)
NumExecutorPerField: 16        # Number of executors per vector field (1-128)

# Queue Configuration  
MasterQueueSize: 1000          # Master queue size (100-10000000)
LocalQueueSize: 1000           # Local queue size (100-10000000)
SearchQueueSize: 1000          # Combined search queue size (500-10000000)

# Performance Tuning
GlobalSyncInterval: 15         # Global synchronization interval in seconds
MinimalGraphSize: 100          # Minimal graph size for NSG algorithm
PreFilter: false               # Enable/disable pre-filtering

# Memory Management
InitialCapacity: 1000          # Initial table capacity
MaxCapacity: 100000000         # Maximum table capacity
GrowthFactor: 2.0              # Capacity growth factor
ExpandThreshold: 0.9           # Threshold to trigger expansion
AllowShrink: true              # Allow memory shrinking

# Network Configuration
ServerPort: 8888               # Server port
MaxConnections: 1000           # Maximum concurrent connections
ConnectionTimeout: 30          # Connection timeout in seconds
ReadTimeout: 60                # Read timeout in seconds
WriteTimeout: 60               # Write timeout in seconds

# Database Configuration
DataPath: "/data"              # Data storage path
WalEnabled: true               # Write-ahead logging enabled
SyncToStorage: true            # Sync to storage
BackupEnabled: false           # Automatic backup enabled
BackupInterval: 3600           # Backup interval in seconds

# Logging Configuration
LogLevel: "INFO"               # Log level: DEBUG, INFO, WARNING, ERROR
LogToFile: true                # Log to file
LogFilePath: "/var/log/vectordb.log"
LogMaxSize: 100                # Max log file size in MB
LogMaxFiles: 10                # Max number of log files

# Security Configuration
AuthEnabled: false             # Authentication enabled
TlsEnabled: false              # TLS encryption enabled
TlsCertPath: ""                # TLS certificate path
TlsKeyPath: ""                 # TLS key path

# Monitoring Configuration  
MetricsEnabled: true           # Metrics collection enabled
MetricsPort: 9090              # Metrics server port
HealthCheckEnabled: true       # Health check endpoint enabled
)";

    std::ofstream file("vectordb_example.yaml");
    if (file.is_open()) {
      file << yaml_content;
      file.close();
      logger_.Info("Created example YAML configuration file: vectordb_example.yaml");
    }
  }

  void createExampleToml() {
    const std::string toml_content = R"(# VectorDB Configuration (TOML format)
# This file demonstrates all available configuration options

[config]
# Core Thread Configuration
IntraQueryThreads = 16
RebuildThreads = 4
NumExecutorPerField = 16

# Queue Configuration
MasterQueueSize = 1000
LocalQueueSize = 1000
SearchQueueSize = 1000

# Performance Tuning
GlobalSyncInterval = 15
MinimalGraphSize = 100
PreFilter = false

[memory]
InitialCapacity = 1000
MaxCapacity = 100000000
GrowthFactor = 2.0
ExpandThreshold = 0.9
AllowShrink = true

[network]
ServerPort = 8888
MaxConnections = 1000
ConnectionTimeout = 30
ReadTimeout = 60
WriteTimeout = 60

[database]
DataPath = "/data"
WalEnabled = true
SyncToStorage = true
BackupEnabled = false
BackupInterval = 3600

[logging]
LogLevel = "INFO"
LogToFile = true
LogFilePath = "/var/log/vectordb.log"
LogMaxSize = 100
LogMaxFiles = 10

[security]
AuthEnabled = false
TlsEnabled = false
TlsCertPath = ""
TlsKeyPath = ""

[monitoring]
MetricsEnabled = true
MetricsPort = 9090
HealthCheckEnabled = true
)";

    std::ofstream file("vectordb_example.toml");
    if (file.is_open()) {
      file << toml_content;
      file.close();
      logger_.Info("Created example TOML configuration file: vectordb_example.toml");
    }
  }

  void createExampleJson() {
    vectordb::Json json;
    json.SetInt("IntraQueryThreads", 16);
    json.SetInt("RebuildThreads", 4);
    json.SetInt("NumExecutorPerField", 16);
    json.SetInt("MasterQueueSize", 1000);
    json.SetInt("LocalQueueSize", 1000);
    json.SetInt("SearchQueueSize", 1000);
    json.SetInt("GlobalSyncInterval", 15);
    json.SetInt("MinimalGraphSize", 100);
    json.SetBool("PreFilter", false);
    json.SetInt("ServerPort", 8888);
    json.SetString("DataPath", "/data");
    json.SetBool("WalEnabled", true);
    json.SetString("LogLevel", "INFO");
    json.SetBool("MetricsEnabled", true);

    std::ofstream file("vectordb_example.json");
    if (file.is_open()) {
      file << json.DumpToString();
      file.close();
      logger_.Info("Created example JSON configuration file: vectordb_example.json");
    }
  }
};

/**
 * @brief Error system integration for backward compatibility
 */
class ErrorIntegration {
public:
  /**
   * @brief Convert legacy error codes to enhanced error codes
   */
  static error::EnhancedStatus convertLegacyStatus(const vectordb::Status& legacy_status) {
    if (legacy_status.ok()) {
      return error::EnhancedStatus::OK();
    }
    
    // Map legacy error codes to enhanced error codes
    // Note: This assumes the legacy Status class returns int32_t code
    // We need to adapt this based on the actual Status class implementation
    int32_t legacy_code = 0; // TODO: Extract actual error code from legacy_status
    error::EnhancedErrorCode enhanced_code = mapLegacyToEnhanced(legacy_code);
    
    return error::EnhancedStatus(enhanced_code, legacy_status.message());
  }
  
  /**
   * @brief Convert enhanced status to legacy status for compatibility
   */
  static vectordb::Status convertToLegacyStatus(const error::EnhancedStatus& enhanced_status) {
    if (enhanced_status.ok()) {
      return vectordb::Status();
    }
    
    return vectordb::Status(enhanced_status.legacyCode(), enhanced_status.message());
  }

private:
  static error::EnhancedErrorCode mapLegacyToEnhanced(int32_t legacy_code) {
    // Map common legacy error codes to enhanced error categories
    if (legacy_code >= 50000 && legacy_code < 60000) {
      // Database errors
      uint16_t specific_code = static_cast<uint16_t>(legacy_code - 50000);
      return error::EnhancedErrorCode(error::ErrorCategory::DATABASE, 0, specific_code);
    } else if (legacy_code >= 40000 && legacy_code < 50000) {
      // Infrastructure errors
      uint16_t specific_code = static_cast<uint16_t>(legacy_code - 40000);
      return error::EnhancedErrorCode(error::ErrorCategory::INFRASTRUCTURE, 0, specific_code);
    } else if (legacy_code >= 30000 && legacy_code < 40000) {
      // User errors -> Validation errors
      uint16_t specific_code = static_cast<uint16_t>(legacy_code - 30000);
      return error::EnhancedErrorCode(error::ErrorCategory::VALIDATION, 0, specific_code);
    }
    
    // Default mapping for unknown codes
    return error::EnhancedErrorCode(error::ErrorCategory::INTERNAL, 0, static_cast<uint16_t>(legacy_code));
  }
};

} // namespace integration
} // namespace vectordb