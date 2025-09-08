#pragma once

#include <atomic>
#include <chrono>
#include <fstream>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <memory>
#include <filesystem>
#include <cstdlib>
#include <exception>
#include <algorithm>
#include <type_traits>

#include "utils/json.hpp"
#include "logger/logger.hpp"
#include "thirdparty/nlohmann/json.hpp"

namespace vectordb {
namespace config {

/**
 * @brief Configuration source types
 */
enum class ConfigSource {
  ENVIRONMENT,
  YAML_FILE,
  TOML_FILE,
  JSON_FILE,
  RUNTIME
};

/**
 * @brief Configuration change event
 */
struct ConfigChangeEvent {
  std::string key;
  std::string old_value;
  std::string new_value;
  ConfigSource source;
  std::chrono::system_clock::time_point timestamp;
};

/**
 * @brief Configuration validation result
 */
struct ValidationResult {
  bool valid;
  std::string error_message;
  
  ValidationResult(bool v = true, const std::string& msg = "") : valid(v), error_message(msg) {}
  
  static ValidationResult Success() { return ValidationResult(true); }
  static ValidationResult Error(const std::string& msg) { return ValidationResult(false, msg); }
};

/**
 * @brief Configuration validator interface
 */
class ConfigValidator {
public:
  virtual ~ConfigValidator() = default;
  virtual ValidationResult validate(const std::string& key, const std::string& value) = 0;
};

/**
 * @brief Range validator for numeric values
 */
template<typename T>
class RangeValidator : public ConfigValidator {
private:
  T min_value_;
  T max_value_;
  
public:
  RangeValidator(T min_val, T max_val) : min_value_(min_val), max_value_(max_val) {}
  
  ValidationResult validate(const std::string& key, const std::string& value) override {
    try {
      T val;
      if constexpr (std::is_integral_v<T>) {
        val = static_cast<T>(std::stoll(value));
      } else {
        val = static_cast<T>(std::stod(value));
      }
      
      if (val < min_value_ || val > max_value_) {
        return ValidationResult::Error("Value " + value + " is out of range [" + 
                                     std::to_string(min_value_) + ", " + 
                                     std::to_string(max_value_) + "]");
      }
      return ValidationResult::Success();
    } catch (const std::exception& e) {
      return ValidationResult::Error("Invalid numeric value: " + value);
    }
  }
};

/**
 * @brief Advanced configuration manager with hot-reload, YAML/TOML support
 */
class AdvancedConfigManager {
private:
  // Core configuration values
  mutable std::shared_mutex config_mutex_;
  std::unordered_map<std::string, std::string> config_values_;
  
  // File watching
  std::string config_file_path_;
  std::filesystem::file_time_type last_write_time_;
  std::atomic<bool> watching_;
  std::thread watch_thread_;
  std::chrono::milliseconds watch_interval_;
  std::chrono::steady_clock::time_point last_reload_time_;
  std::chrono::milliseconds debounce_interval_;
  std::atomic<size_t> reload_count_;
  mutable std::mutex reload_mutex_;
  
  // Callbacks and validators
  std::unordered_map<std::string, std::vector<std::function<void(const ConfigChangeEvent&)>>> change_callbacks_;
  std::unordered_map<std::string, std::shared_ptr<ConfigValidator>> validators_;
  
  // Logging
  mutable vectordb::engine::Logger logger_;
  
public:
  explicit AdvancedConfigManager(const std::string& config_file = "", 
                               std::chrono::milliseconds watch_interval = std::chrono::milliseconds(5000),
                               std::chrono::milliseconds debounce_interval = std::chrono::milliseconds(1000))
    : config_file_path_(config_file), watching_(false), watch_interval_(watch_interval), 
      debounce_interval_(debounce_interval), reload_count_(0) {
    
    // Initialize default validators
    setupDefaultValidators();
    
    // Load initial configuration
    loadInitialConfig();
    
    // Start file watching if config file is specified
    if (!config_file_path_.empty()) {
      startWatching();
    }
  }
  
  ~AdvancedConfigManager() {
    stopWatching();
  }

  /**
   * @brief Get configuration value with type conversion
   */
  template<typename T>
  T getValue(const std::string& key, const T& default_value = T{}) const {
    std::shared_lock<std::shared_mutex> lock(config_mutex_);
    
    auto it = config_values_.find(key);
    if (it == config_values_.end()) {
      return default_value;
    }
    
    try {
      if constexpr (std::is_same_v<T, std::string>) {
        return it->second;
      } else if constexpr (std::is_same_v<T, bool>) {
        const std::string& val = it->second;
        return val == "true" || val == "1" || val == "yes" || val == "on";
      } else if constexpr (std::is_integral_v<T>) {
        return static_cast<T>(std::stoll(it->second));
      } else if constexpr (std::is_floating_point_v<T>) {
        return static_cast<T>(std::stod(it->second));
      } else {
        // For complex types, try to parse as JSON
        vectordb::Json json;
        if (json.LoadFromString(it->second)) {
          // Custom deserialization logic can be added here
          return default_value;
        }
      }
    } catch (const std::exception& e) {
      logger_.Warning("Failed to convert config value for key '" + key + "': " + e.what());
      return default_value;
    }
    
    return default_value;
  }

  /**
   * @brief Set configuration value with validation
   */
  template<typename T>
  bool setValue(const std::string& key, const T& value, ConfigSource source = ConfigSource::RUNTIME) {
    std::string str_value;
    if constexpr (std::is_same_v<T, std::string>) {
      str_value = value;
    } else if constexpr (std::is_same_v<T, const char*>) {
      str_value = std::string(value);
    } else if constexpr (std::is_same_v<T, bool>) {
      str_value = value ? "true" : "false";
    } else if constexpr (std::is_arithmetic_v<T>) {
      str_value = std::to_string(value);
    } else {
      // For other types, try to convert to string
      std::ostringstream oss;
      oss << value;
      str_value = oss.str();
    }
    
    // Validate the value
    auto validator_it = validators_.find(key);
    if (validator_it != validators_.end()) {
      auto validation = validator_it->second->validate(key, str_value);
      if (!validation.valid) {
        logger_.Error("Validation failed for key '" + key + "': " + validation.error_message);
        return false;
      }
    }
    
    std::string old_value;
    {
      std::unique_lock<std::shared_mutex> lock(config_mutex_);
      auto it = config_values_.find(key);
      if (it != config_values_.end()) {
        old_value = it->second;
      }
      config_values_[key] = str_value;
    }
    
    // Notify callbacks
    ConfigChangeEvent event{
      key,
      old_value,
      str_value,
      source,
      std::chrono::system_clock::now()
    };
    
    notifyCallbacks(key, event);
    
    logger_.Info("Configuration updated: " + key + " = " + str_value);
    return true;
  }

  /**
   * @brief Register change callback for specific key
   */
  void registerCallback(const std::string& key, std::function<void(const ConfigChangeEvent&)> callback) {
    std::unique_lock<std::shared_mutex> lock(config_mutex_);
    change_callbacks_[key].push_back(callback);
  }

  /**
   * @brief Register validator for specific key
   */
  void registerValidator(const std::string& key, std::shared_ptr<ConfigValidator> validator) {
    validators_[key] = validator;
  }

  /**
   * @brief Load configuration from file (YAML, TOML, or JSON)
   */
  bool loadFromFile(const std::string& file_path) {
    try {
      std::filesystem::path path(file_path);
      if (!std::filesystem::exists(path)) {
        logger_.Warning("Configuration file does not exist: " + file_path);
        return false;
      }

      std::string extension = path.extension().string();
      std::transform(extension.begin(), extension.end(), extension.begin(), ::tolower);
      
      if (extension == ".yaml" || extension == ".yml") {
        return loadFromYaml(file_path);
      } else if (extension == ".toml") {
        return loadFromToml(file_path);
      } else if (extension == ".json") {
        return loadFromJson(file_path);
      } else {
        logger_.Warning("Unsupported configuration file format: " + extension);
        return false;
      }
    } catch (const std::exception& e) {
      logger_.Error("Failed to load configuration from file: " + std::string(e.what()));
      return false;
    }
  }

  /**
   * @brief Save current configuration to file
   */
  bool saveToFile(const std::string& file_path) const {
    try {
      std::filesystem::path path(file_path);
      std::string extension = path.extension().string();
      std::transform(extension.begin(), extension.end(), extension.begin(), ::tolower);
      
      if (extension == ".yaml" || extension == ".yml") {
        return saveToYaml(file_path);
      } else if (extension == ".json") {
        return saveToJson(file_path);
      } else {
        logger_.Warning("Unsupported configuration file format for saving: " + extension);
        return false;
      }
    } catch (const std::exception& e) {
      logger_.Error("Failed to save configuration to file: " + std::string(e.what()));
      return false;
    }
  }

  /**
   * @brief Get all configuration values as JSON
   */
  vectordb::Json getAllAsJson() const {
    std::shared_lock<std::shared_mutex> lock(config_mutex_);
    
    vectordb::Json result;
    for (const auto& [key, value] : config_values_) {
      result.SetString(key, value);
    }
    return result;
  }

  /**
   * @brief Get configuration statistics
   */
  struct ConfigStats {
    size_t total_keys = 0;
    size_t total_callbacks = 0;
    size_t total_validators = 0;
    size_t reload_count = 0;
    bool watching_enabled = false;
    std::string config_file_path;
    std::chrono::system_clock::time_point last_reload_time;
  };

  ConfigStats getStats() const {
    std::shared_lock<std::shared_mutex> lock(config_mutex_);
    
    ConfigStats stats;
    stats.total_keys = config_values_.size();
    stats.total_validators = validators_.size();
    stats.reload_count = reload_count_.load();
    stats.watching_enabled = watching_.load();
    stats.config_file_path = config_file_path_;
    
    // Count total callbacks
    for (const auto& [key, callbacks] : change_callbacks_) {
      stats.total_callbacks += callbacks.size();
    }
    
    return stats;
  }

  /**
   * @brief Check if a key exists in configuration
   */
  bool hasKey(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(config_mutex_);
    return config_values_.find(key) != config_values_.end();
  }

  /**
   * @brief Remove a configuration key
   */
  bool removeKey(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(config_mutex_);
    auto it = config_values_.find(key);
    if (it != config_values_.end()) {
      std::string old_value = it->second;
      config_values_.erase(it);
      
      // Notify callbacks about removal
      ConfigChangeEvent event{
        key, old_value, "", ConfigSource::RUNTIME,
        std::chrono::system_clock::now()
      };
      lock.unlock();
      notifyCallbacks(key, event);
      
      logger_.Info("Configuration key removed: " + key);
      return true;
    }
    return false;
  }

private:
  void setupDefaultValidators() {
    // Thread count validators
    registerValidator("IntraQueryThreads", std::make_shared<RangeValidator<int>>(1, 128));
    registerValidator("RebuildThreads", std::make_shared<RangeValidator<int>>(1, 128));
    registerValidator("NumExecutorPerField", std::make_shared<RangeValidator<int>>(1, 128));
    
    // Queue size validators
    registerValidator("MasterQueueSize", std::make_shared<RangeValidator<int>>(100, 10000000));
    registerValidator("LocalQueueSize", std::make_shared<RangeValidator<int>>(100, 10000000));
    registerValidator("SearchQueueSize", std::make_shared<RangeValidator<int>>(500, 10000000));
  }

  void loadInitialConfig() {
    // Load from environment variables first
    loadFromEnvironment();
    
    // Then load from file if specified
    if (!config_file_path_.empty()) {
      loadFromFile(config_file_path_);
    }
  }

  void loadFromEnvironment() {
    const std::vector<std::string> env_vars = {
      "EPSILLA_INTRA_QUERY_THREADS",
      "EPSILLA_REBUILD_THREADS",
      "EPSILLA_NUM_EXECUTOR_PER_FIELD",
      "EPSILLA_MASTER_QUEUE_SIZE",
      "EPSILLA_LOCAL_QUEUE_SIZE",
      "EPSILLA_SEARCH_QUEUE_SIZE",
      "EPSILLA_PRE_FILTER",
      "EPSILLA_GLOBAL_SYNC_INTERVAL",
      "EPSILLA_MINIMAL_GRAPH_SIZE"
    };

    for (const auto& env_var : env_vars) {
      const char* value = std::getenv(env_var.c_str());
      if (value != nullptr) {
        // Convert environment variable name to config key
        std::string key = envVarToConfigKey(env_var);
        setValue(key, std::string(value), ConfigSource::ENVIRONMENT);
      }
    }
  }

  std::string envVarToConfigKey(const std::string& env_var) {
    // Convert EPSILLA_INTRA_QUERY_THREADS to IntraQueryThreads
    std::string result = env_var;
    if (result.substr(0, 8) == "EPSILLA_") {
      result = result.substr(8); // Remove "EPSILLA_"
    }
    
    // Convert UPPER_CASE to PascalCase
    std::string pascal_case;
    bool next_upper = true;
    for (char c : result) {
      if (c == '_') {
        next_upper = true;
      } else if (next_upper) {
        pascal_case += c;
        next_upper = false;
      } else {
        pascal_case += std::tolower(c);
      }
    }
    return pascal_case;
  }

  bool loadFromYaml(const std::string& file_path) {
    // Simple YAML parser (for basic key-value pairs)
    std::ifstream file(file_path);
    if (!file.is_open()) {
      logger_.Error("Cannot open YAML file: " + file_path);
      return false;
    }

    std::string line;
    while (std::getline(file, line)) {
      // Remove comments and trim whitespace
      size_t comment_pos = line.find('#');
      if (comment_pos != std::string::npos) {
        line = line.substr(0, comment_pos);
      }
      
      line.erase(0, line.find_first_not_of(" \t"));
      line.erase(line.find_last_not_of(" \t") + 1);
      
      if (line.empty()) continue;
      
      // Parse key: value format
      size_t colon_pos = line.find(':');
      if (colon_pos != std::string::npos) {
        std::string key = line.substr(0, colon_pos);
        std::string value = line.substr(colon_pos + 1);
        
        // Trim key and value
        key.erase(0, key.find_first_not_of(" \t"));
        key.erase(key.find_last_not_of(" \t") + 1);
        value.erase(0, value.find_first_not_of(" \t"));
        value.erase(value.find_last_not_of(" \t") + 1);
        
        // Remove quotes from value if present
        if ((value.front() == '"' && value.back() == '"') ||
            (value.front() == '\'' && value.back() == '\'')) {
          value = value.substr(1, value.length() - 2);
        }
        
        setValue(key, value, ConfigSource::YAML_FILE);
      }
    }
    
    logger_.Info("Loaded configuration from YAML file: " + file_path);
    return true;
  }

  bool loadFromToml(const std::string& file_path) {
    // Simple TOML parser (for basic key-value pairs under [config] section)
    std::ifstream file(file_path);
    if (!file.is_open()) {
      logger_.Error("Cannot open TOML file: " + file_path);
      return false;
    }

    std::string line;
    bool in_config_section = false;
    
    while (std::getline(file, line)) {
      // Remove comments and trim whitespace
      size_t comment_pos = line.find('#');
      if (comment_pos != std::string::npos) {
        line = line.substr(0, comment_pos);
      }
      
      line.erase(0, line.find_first_not_of(" \t"));
      line.erase(line.find_last_not_of(" \t") + 1);
      
      if (line.empty()) continue;
      
      // Check for section headers
      if (line.front() == '[' && line.back() == ']') {
        std::string section = line.substr(1, line.length() - 2);
        in_config_section = (section == "config" || section == "vectordb");
        continue;
      }
      
      if (!in_config_section) continue;
      
      // Parse key = value format
      size_t equals_pos = line.find('=');
      if (equals_pos != std::string::npos) {
        std::string key = line.substr(0, equals_pos);
        std::string value = line.substr(equals_pos + 1);
        
        // Trim key and value
        key.erase(0, key.find_first_not_of(" \t"));
        key.erase(key.find_last_not_of(" \t") + 1);
        value.erase(0, value.find_first_not_of(" \t"));
        value.erase(value.find_last_not_of(" \t") + 1);
        
        // Remove quotes from value if present
        if ((value.front() == '"' && value.back() == '"') ||
            (value.front() == '\'' && value.back() == '\'')) {
          value = value.substr(1, value.length() - 2);
        }
        
        setValue(key, value, ConfigSource::TOML_FILE);
      }
    }
    
    logger_.Info("Loaded configuration from TOML file: " + file_path);
    return true;
  }

  bool loadFromJson(const std::string& file_path) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
      logger_.Error("Cannot open JSON file: " + file_path);
      return false;
    }

    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    
    vectordb::Json json;
    if (!json.LoadFromString(content)) {
      logger_.Error("Failed to parse JSON file: " + file_path);
      return false;
    }

    // Extract values directly using a simpler approach
    // Parse the JSON again using nlohmann::json directly for more control
    try {
      nlohmann::json doc = nlohmann::json::parse(content);
      
      std::unique_lock<std::shared_mutex> lock(config_mutex_);
      
      // Iterate through all key-value pairs in the JSON
      for (auto& [key, value] : doc.items()) {
        if (value.is_string()) {
          config_values_[key] = value.get<std::string>();
        } else if (value.is_number_integer()) {
          config_values_[key] = std::to_string(value.get<int64_t>());
        } else if (value.is_boolean()) {
          config_values_[key] = value.get<bool>() ? "true" : "false";
        } else if (value.is_number_float()) {
          config_values_[key] = std::to_string(value.get<double>());
        } else {
          // For other types, convert to string representation
          config_values_[key] = value.dump();
        }
      }
      
    } catch (const std::exception& e) {
      logger_.Error("Failed to parse JSON content: " + std::string(e.what()));
      return false;
    }

    logger_.Info("Loaded configuration from JSON file: " + file_path);
    return true;
  }

  bool saveToYaml(const std::string& file_path) const {
    std::ofstream file(file_path);
    if (!file.is_open()) {
      logger_.Error("Cannot open file for writing: " + file_path);
      return false;
    }

    file << "# VectorDB Configuration\n";
    file << "# Generated at: " << std::chrono::system_clock::now().time_since_epoch().count() << "\n\n";
    
    std::shared_lock<std::shared_mutex> lock(config_mutex_);
    for (const auto& [key, value] : config_values_) {
      file << key << ": " << value << "\n";
    }
    
    logger_.Info("Saved configuration to YAML file: " + file_path);
    return true;
  }

  bool saveToJson(const std::string& file_path) const {
    vectordb::Json json = getAllAsJson();
    std::string content = json.DumpToString();
    
    std::ofstream file(file_path);
    if (!file.is_open()) {
      logger_.Error("Cannot open file for writing: " + file_path);
      return false;
    }
    
    file << content;
    logger_.Info("Saved configuration to JSON file: " + file_path);
    return true;
  }

  void startWatching() {
    if (watching_.load() || config_file_path_.empty()) {
      return;
    }

    try {
      if (std::filesystem::exists(config_file_path_)) {
        last_write_time_ = std::filesystem::last_write_time(config_file_path_);
        last_reload_time_ = std::chrono::steady_clock::now();
      }
    } catch (const std::exception& e) {
      logger_.Warning("Cannot get file modification time: " + std::string(e.what()));
      return;
    }

    watching_.store(true);
    watch_thread_ = std::thread([this]() {
      while (watching_.load()) {
        try {
          if (std::filesystem::exists(config_file_path_)) {
            auto current_time = std::filesystem::last_write_time(config_file_path_);
            if (current_time != last_write_time_) {
              // Implement debouncing to prevent rapid reloads
              auto now = std::chrono::steady_clock::now();
              if (now - last_reload_time_ >= debounce_interval_) {
                std::lock_guard<std::mutex> lock(reload_mutex_);
                
                logger_.Info("Configuration file changed, reloading...");
                if (reloadConfiguration()) {
                  last_write_time_ = current_time;
                  last_reload_time_ = now;
                  reload_count_.fetch_add(1);
                  logger_.Info("Configuration successfully reloaded (reload #" + 
                              std::to_string(reload_count_.load()) + ")");
                } else {
                  logger_.Error("Failed to reload configuration, keeping current settings");
                  // Reset the file time to try again next iteration
                  try {
                    last_write_time_ = std::filesystem::last_write_time(config_file_path_);
                  } catch (const std::exception&) {
                    // Ignore file time update errors
                  }
                }
              } else {
                // File changed too recently, wait for debounce period
                logger_.Debug("Configuration file change detected, waiting for debounce period");
              }
            }
          } else {
            // File was deleted, log warning but continue watching
            logger_.Warning("Configuration file no longer exists: " + config_file_path_);
          }
        } catch (const std::exception& e) {
          logger_.Warning("Error watching config file: " + std::string(e.what()));
          // Brief pause before retrying to avoid tight error loops
          std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
        std::this_thread::sleep_for(watch_interval_);
      }
      logger_.Info("Configuration file watching stopped");
    });
    
    logger_.Info("Started configuration file watching: " + config_file_path_);
  }

  void stopWatching() {
    if (watching_.load()) {
      watching_.store(false);
      if (watch_thread_.joinable()) {
        watch_thread_.join();
      }
    }
  }

  void notifyCallbacks(const std::string& key, const ConfigChangeEvent& event) {
    auto it = change_callbacks_.find(key);
    if (it != change_callbacks_.end()) {
      for (const auto& callback : it->second) {
        try {
          callback(event);
        } catch (const std::exception& e) {
          logger_.Warning("Config callback failed for key '" + key + "': " + e.what());
        }
      }
    }
  }

  /**
   * @brief Safely reload configuration from file with validation
   */
  bool reloadConfiguration() {
    if (config_file_path_.empty()) {
      return false;
    }

    try {
      // Create a backup of current configuration
      std::unordered_map<std::string, std::string> backup_config;
      {
        std::shared_lock<std::shared_mutex> lock(config_mutex_);
        backup_config = config_values_;
      }

      // Try to load new configuration
      if (loadFromFile(config_file_path_)) {
        return true;
      } else {
        // Restore backup if loading failed
        {
          std::unique_lock<std::shared_mutex> lock(config_mutex_);
          config_values_ = backup_config;
        }
        logger_.Error("Failed to reload configuration, restored previous settings");
        return false;
      }
    } catch (const std::exception& e) {
      logger_.Error("Exception during configuration reload: " + std::string(e.what()));
      return false;
    }
  }
};

// Global instance
extern AdvancedConfigManager* g_advanced_config;

/**
 * @brief Initialize advanced config manager
 */
inline void InitializeAdvancedConfig(const std::string& config_file = "") {
  if (g_advanced_config == nullptr) {
    g_advanced_config = new AdvancedConfigManager(config_file);
  }
}

/**
 * @brief Get advanced config manager instance
 */
inline AdvancedConfigManager& GetAdvancedConfig() {
  if (g_advanced_config == nullptr) {
    InitializeAdvancedConfig();
  }
  return *g_advanced_config;
}

} // namespace config
} // namespace vectordb