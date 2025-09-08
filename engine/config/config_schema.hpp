#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <regex>
#include <string>
#include <unordered_map>
#include <vector>
#include <variant>
#include <optional>

#include "config/advanced_config.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace config {

/**
 * @brief Configuration value types
 */
enum class ConfigValueType {
  INTEGER,
  FLOAT,
  BOOLEAN,
  STRING,
  ARRAY,
  OBJECT
};

/**
 * @brief Configuration constraint types
 */
enum class ConstraintType {
  RANGE,        // Numeric range constraint
  ENUM,         // Enumerated values constraint
  REGEX,        // Regular expression constraint
  REQUIRED,     // Required field constraint
  DEPENDS_ON,   // Dependency constraint
  CUSTOM        // Custom validation constraint
};

/**
 * @brief Configuration constraint definition
 */
struct ConfigConstraint {
  ConstraintType type;
  std::variant<
    std::pair<double, double>,              // RANGE: min, max
    std::vector<std::string>,               // ENUM: allowed values
    std::regex,                             // REGEX: pattern
    bool,                                   // REQUIRED: is required
    std::string,                            // DEPENDS_ON: dependent key
    std::function<bool(const std::string&)> // CUSTOM: validation function
  > value;
  
  std::string error_message;
  
  ConfigConstraint(ConstraintType t, const std::string& error = "") 
    : type(t), error_message(error) {}
};

/**
 * @brief Configuration field schema
 */
struct ConfigFieldSchema {
  std::string name;
  std::string description;
  ConfigValueType value_type;
  std::optional<std::string> default_value;
  std::vector<ConfigConstraint> constraints;
  bool deprecated = false;
  std::string deprecated_message;
  std::vector<std::string> aliases; // Alternative names for this field
  
  ConfigFieldSchema(const std::string& n, ConfigValueType type, const std::string& desc = "")
    : name(n), description(desc), value_type(type) {}
};

/**
 * @brief Configuration schema definition
 */
class ConfigSchema {
private:
  std::unordered_map<std::string, ConfigFieldSchema> fields_;
  std::unordered_map<std::string, std::string> aliases_map_; // alias -> canonical name
  mutable vectordb::engine::Logger logger_;
  
public:
  ConfigSchema() = default;
  
  /**
   * @brief Add a field to the schema
   */
  ConfigSchema& addField(const ConfigFieldSchema& field) {
    fields_[field.name] = field;
    
    // Register aliases
    for (const auto& alias : field.aliases) {
      aliases_map_[alias] = field.name;
    }
    
    return *this;
  }
  
  /**
   * @brief Add a constraint to an existing field
   */
  ConfigSchema& addConstraint(const std::string& field_name, const ConfigConstraint& constraint) {
    auto it = fields_.find(field_name);
    if (it != fields_.end()) {
      it->second.constraints.push_back(constraint);
    } else {
      logger_.Warning("Attempted to add constraint to non-existent field: " + field_name);
    }
    return *this;
  }
  
  /**
   * @brief Validate a configuration against this schema
   */
  ValidationResult validate(const std::unordered_map<std::string, std::string>& config) const {
    std::vector<std::string> errors;
    
    // Check all fields in schema
    for (const auto& [field_name, schema] : fields_) {
      auto config_it = config.find(field_name);
      
      // Check aliases if primary name not found
      if (config_it == config.end()) {
        for (const auto& alias : schema.aliases) {
          config_it = config.find(alias);
          if (config_it != config.end()) {
            break;
          }
        }
      }
      
      // Validate field
      auto field_result = validateField(schema, config_it != config.end() ? config_it->second : "");
      if (!field_result.valid) {
        errors.push_back(field_name + ": " + field_result.error_message);
      }
    }
    
    // Check for unknown fields
    for (const auto& [config_key, config_value] : config) {
      if (fields_.find(config_key) == fields_.end() && aliases_map_.find(config_key) == aliases_map_.end()) {
        logger_.Warning("Unknown configuration key: " + config_key);
      }
    }
    
    if (errors.empty()) {
      return ValidationResult::Success();
    } else {
      std::string combined_errors = "Validation errors: ";
      for (size_t i = 0; i < errors.size(); ++i) {
        if (i > 0) combined_errors += "; ";
        combined_errors += errors[i];
      }
      return ValidationResult::Error(combined_errors);
    }
  }
  
  /**
   * @brief Get canonical field name (resolves aliases)
   */
  std::string getCanonicalName(const std::string& name) const {
    auto alias_it = aliases_map_.find(name);
    return alias_it != aliases_map_.end() ? alias_it->second : name;
  }
  
  /**
   * @brief Get field schema by name
   */
  std::optional<ConfigFieldSchema> getFieldSchema(const std::string& name) const {
    std::string canonical_name = getCanonicalName(name);
    auto it = fields_.find(canonical_name);
    return it != fields_.end() ? std::optional<ConfigFieldSchema>{it->second} : std::nullopt;
  }
  
  /**
   * @brief Get all field names
   */
  std::vector<std::string> getFieldNames() const {
    std::vector<std::string> names;
    names.reserve(fields_.size());
    for (const auto& [name, _] : fields_) {
      names.push_back(name);
    }
    return names;
  }
  
  /**
   * @brief Generate configuration documentation
   */
  std::string generateDocumentation() const {
    std::string doc = "# Configuration Schema\n\n";
    
    for (const auto& [field_name, schema] : fields_) {
      doc += "## " + field_name + "\n";
      doc += "- **Type**: " + valueTypeToString(schema.value_type) + "\n";
      doc += "- **Description**: " + schema.description + "\n";
      
      if (schema.default_value.has_value()) {
        doc += "- **Default**: " + schema.default_value.value() + "\n";
      }
      
      if (!schema.aliases.empty()) {
        doc += "- **Aliases**: ";
        for (size_t i = 0; i < schema.aliases.size(); ++i) {
          if (i > 0) doc += ", ";
          doc += schema.aliases[i];
        }
        doc += "\n";
      }
      
      if (schema.deprecated) {
        doc += "- **⚠️ DEPRECATED**: " + schema.deprecated_message + "\n";
      }
      
      // Document constraints
      if (!schema.constraints.empty()) {
        doc += "- **Constraints**:\n";
        for (const auto& constraint : schema.constraints) {
          doc += "  - " + constraintToString(constraint) + "\n";
        }
      }
      
      doc += "\n";
    }
    
    return doc;
  }

private:
  ValidationResult validateField(const ConfigFieldSchema& schema, const std::string& value) const {
    bool has_value = !value.empty();
    
    // Check constraints
    for (const auto& constraint : schema.constraints) {
      switch (constraint.type) {
        case ConstraintType::REQUIRED:
          if (std::get<bool>(constraint.value) && !has_value) {
            return ValidationResult::Error(constraint.error_message.empty() ? 
              "Field is required" : constraint.error_message);
          }
          break;
          
        case ConstraintType::RANGE:
          if (has_value && (schema.value_type == ConfigValueType::INTEGER || schema.value_type == ConfigValueType::FLOAT)) {
            try {
              double num_value = std::stod(value);
              auto range = std::get<std::pair<double, double>>(constraint.value);
              if (num_value < range.first || num_value > range.second) {
                return ValidationResult::Error(constraint.error_message.empty() ?
                  "Value out of range [" + std::to_string(range.first) + ", " + std::to_string(range.second) + "]" :
                  constraint.error_message);
              }
            } catch (const std::exception&) {
              return ValidationResult::Error("Invalid numeric value");
            }
          }
          break;
          
        case ConstraintType::ENUM:
          if (has_value) {
            const auto& allowed_values = std::get<std::vector<std::string>>(constraint.value);
            if (std::find(allowed_values.begin(), allowed_values.end(), value) == allowed_values.end()) {
              return ValidationResult::Error(constraint.error_message.empty() ?
                "Value not in allowed set" : constraint.error_message);
            }
          }
          break;
          
        case ConstraintType::REGEX:
          if (has_value) {
            const auto& pattern = std::get<std::regex>(constraint.value);
            if (!std::regex_match(value, pattern)) {
              return ValidationResult::Error(constraint.error_message.empty() ?
                "Value does not match required pattern" : constraint.error_message);
            }
          }
          break;
          
        case ConstraintType::CUSTOM:
          if (has_value) {
            const auto& validator = std::get<std::function<bool(const std::string&)>>(constraint.value);
            if (!validator(value)) {
              return ValidationResult::Error(constraint.error_message.empty() ?
                "Custom validation failed" : constraint.error_message);
            }
          }
          break;
          
        case ConstraintType::DEPENDS_ON:
          // Dependency validation would require access to full config - skip for now
          break;
      }
    }
    
    return ValidationResult::Success();
  }
  
  std::string valueTypeToString(ConfigValueType type) const {
    switch (type) {
      case ConfigValueType::INTEGER: return "Integer";
      case ConfigValueType::FLOAT: return "Float";
      case ConfigValueType::BOOLEAN: return "Boolean";
      case ConfigValueType::STRING: return "String";
      case ConfigValueType::ARRAY: return "Array";
      case ConfigValueType::OBJECT: return "Object";
    }
    return "Unknown";
  }
  
  std::string constraintToString(const ConfigConstraint& constraint) const {
    switch (constraint.type) {
      case ConstraintType::REQUIRED:
        return std::get<bool>(constraint.value) ? "Required" : "Optional";
      case ConstraintType::RANGE: {
        auto range = std::get<std::pair<double, double>>(constraint.value);
        return "Range: [" + std::to_string(range.first) + ", " + std::to_string(range.second) + "]";
      }
      case ConstraintType::ENUM: {
        const auto& values = std::get<std::vector<std::string>>(constraint.value);
        std::string result = "Allowed values: ";
        for (size_t i = 0; i < values.size(); ++i) {
          if (i > 0) result += ", ";
          result += values[i];
        }
        return result;
      }
      case ConstraintType::REGEX:
        return "Pattern validation";
      case ConstraintType::CUSTOM:
        return "Custom validation";
      case ConstraintType::DEPENDS_ON:
        return "Depends on: " + std::get<std::string>(constraint.value);
    }
    return "Unknown constraint";
  }
};

/**
 * @brief Factory for creating VectorDB configuration schema
 */
class VectorDBConfigSchemaFactory {
public:
  static ConfigSchema createSchema() {
    ConfigSchema schema;
    
    // Core thread configuration
    schema.addField(ConfigFieldSchema("IntraQueryThreads", ConfigValueType::INTEGER, 
                                     "Number of threads for query processing"))
          .addConstraint("IntraQueryThreads", ConfigConstraint(ConstraintType::REQUIRED, true))
          .addConstraint("IntraQueryThreads", ConfigConstraint(ConstraintType::RANGE, "Thread count must be 1-128"))
          .fields_["IntraQueryThreads"].constraints.back().value = std::make_pair(1.0, 128.0);
    
    schema.addField(ConfigFieldSchema("RebuildThreads", ConfigValueType::INTEGER,
                                     "Number of threads for index rebuilding"))
          .addConstraint("RebuildThreads", ConfigConstraint(ConstraintType::RANGE, "Thread count must be 1-128"))
          .fields_["RebuildThreads"].constraints.back().value = std::make_pair(1.0, 128.0);
    
    schema.addField(ConfigFieldSchema("NumExecutorPerField", ConfigValueType::INTEGER,
                                     "Number of executors per vector field"))
          .addConstraint("NumExecutorPerField", ConfigConstraint(ConstraintType::RANGE, "Executor count must be 1-128"))
          .fields_["NumExecutorPerField"].constraints.back().value = std::make_pair(1.0, 128.0);
    
    // Queue configuration
    schema.addField(ConfigFieldSchema("MasterQueueSize", ConfigValueType::INTEGER,
                                     "Master queue size for operations"))
          .addConstraint("MasterQueueSize", ConfigConstraint(ConstraintType::RANGE, "Queue size must be 100-10000000"))
          .fields_["MasterQueueSize"].constraints.back().value = std::make_pair(100.0, 10000000.0);
    
    schema.addField(ConfigFieldSchema("LocalQueueSize", ConfigValueType::INTEGER,
                                     "Local queue size for operations"))
          .addConstraint("LocalQueueSize", ConfigConstraint(ConstraintType::RANGE, "Queue size must be 100-10000000"))
          .fields_["LocalQueueSize"].constraints.back().value = std::make_pair(100.0, 10000000.0);
    
    schema.addField(ConfigFieldSchema("SearchQueueSize", ConfigValueType::INTEGER,
                                     "Combined search queue size (affects both master and local queues)"))
          .addConstraint("SearchQueueSize", ConfigConstraint(ConstraintType::RANGE, "Queue size must be 500-10000000"))
          .fields_["SearchQueueSize"].constraints.back().value = std::make_pair(500.0, 10000000.0);
    
    // Performance tuning
    schema.addField(ConfigFieldSchema("GlobalSyncInterval", ConfigValueType::INTEGER,
                                     "Global synchronization interval in seconds"))
          .addConstraint("GlobalSyncInterval", ConfigConstraint(ConstraintType::RANGE, "Sync interval must be 1-3600 seconds"))
          .fields_["GlobalSyncInterval"].constraints.back().value = std::make_pair(1.0, 3600.0);
    
    schema.addField(ConfigFieldSchema("MinimalGraphSize", ConfigValueType::INTEGER,
                                     "Minimal graph size for NSG algorithm"))
          .addConstraint("MinimalGraphSize", ConfigConstraint(ConstraintType::RANGE, "Graph size must be 10-100000"))
          .fields_["MinimalGraphSize"].constraints.back().value = std::make_pair(10.0, 100000.0);
    
    schema.addField(ConfigFieldSchema("PreFilter", ConfigValueType::BOOLEAN,
                                     "Enable/disable pre-filtering optimization"));
    
    return schema;
  }
};

/**
 * @brief Schema-aware configuration validator
 */
class SchemaConfigValidator : public ConfigValidator {
private:
  ConfigSchema schema_;
  
public:
  explicit SchemaConfigValidator(const ConfigSchema& schema) : schema_(schema) {}
  
  ValidationResult validate(const std::string& key, const std::string& value) override {
    auto field_schema = schema_.getFieldSchema(key);
    if (!field_schema.has_value()) {
      return ValidationResult::Error("Unknown configuration key: " + key);
    }
    
    // Create a minimal config map for this single field validation
    std::unordered_map<std::string, std::string> config_map;
    config_map[key] = value;
    
    return schema_.validate(config_map);
  }
};

} // namespace config
} // namespace vectordb