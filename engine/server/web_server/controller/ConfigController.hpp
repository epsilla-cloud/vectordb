#pragma once

#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include <oatpp-swagger/Types.hpp>

#include "config/advanced_config.hpp"
#include "config/config_integration.hpp"
#include "config/config_schema.hpp"
#include "logger/logger.hpp"

#include OATPP_CODEGEN_BEGIN(ApiController)

namespace vectordb {
namespace server {

/**
 * @brief Configuration management API controller
 */
class ConfigController : public oatpp::web::server::api::ApiController {
private:
  config::AdvancedConfigManager& config_manager_;
  config::ConfigSchema schema_;
  mutable vectordb::engine::Logger logger_;
  
public:
  explicit ConfigController(const std::shared_ptr<ObjectMapper>& objectMapper)
    : oatpp::web::server::api::ApiController(objectMapper),
      config_manager_(config::GetAdvancedConfig()),
      schema_(config::VectorDBConfigSchemaFactory::createSchema()) {}

  static std::shared_ptr<ConfigController> createShared(
    OATPP_COMPONENT(std::shared_ptr<ObjectMapper>, objectMapper)
  ) {
    return std::make_shared<ConfigController>(objectMapper);
  }

  /**
   * @brief Get all configuration values
   */
  ENDPOINT_INFO(getAllConfig) {
    info->summary = "Get all configuration values";
    info->description = "Retrieves all current configuration values and system statistics including total keys, callbacks, reload count, and file watching status.";
    info->tags->push_back("Configuration");
    info->addResponse<Object<ConfigResponseDto>>(Status::CODE_200, "application/json", "Configuration retrieved successfully");
    info->addResponse<Object<ErrorResponseDto>>(Status::CODE_500, "application/json", "Internal server error");
  }
  ENDPOINT("GET", "/api/config", getAllConfig) {
    try {
      auto stats = config_manager_.getStats();
      auto config_json = config_manager_.getAllAsJson();
      
      auto dto = oatpp::Object<ConfigResponseDto>::createShared();
      dto->success = true;
      dto->message = "Configuration retrieved successfully";
      dto->data = oatpp::Object<ConfigDataDto>::createShared();
      
      // Convert Json to oatpp object
      auto config_map = std::unordered_map<oatpp::String, oatpp::String>();
      // Note: This would need proper JSON to oatpp conversion
      dto->data->config = config_map;
      dto->data->stats = oatpp::Object<ConfigStatsDto>::createShared();
      dto->data->stats->totalKeys = stats.total_keys;
      dto->data->stats->totalCallbacks = stats.total_callbacks;
      dto->data->stats->reloadCount = stats.reload_count;
      dto->data->stats->watchingEnabled = stats.watching_enabled;
      
      return createDtoResponse(Status::CODE_200, dto);
    } catch (const std::exception& e) {
      logger_.Error("Failed to get configuration: " + std::string(e.what()));
      
      auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
      errorDto->success = false;
      errorDto->error = "Failed to retrieve configuration";
      errorDto->details = e.what();
      
      return createDtoResponse(Status::CODE_500, errorDto);
    }
  }

  /**
   * @brief Get specific configuration value
   */
  ENDPOINT_INFO(getConfig) {
    info->summary = "Get specific configuration value";
    info->description = "Retrieves the value of a specific configuration key along with schema information if available.";
    info->tags->push_back("Configuration");
    info->pathParams["key"].description = "Configuration key name";
    info->pathParams["key"].required = true;
    info->addResponse<Object<ConfigValueResponseDto>>(Status::CODE_200, "application/json", "Configuration value retrieved successfully");
    info->addResponse<Object<ErrorResponseDto>>(Status::CODE_404, "application/json", "Configuration key not found");
    info->addResponse<Object<ErrorResponseDto>>(Status::CODE_500, "application/json", "Internal server error");
  }
  ENDPOINT("GET", "/api/config/{key}", getConfig,
           PATH(String, key)) {
    try {
      if (!config_manager_.hasKey(key)) {
        auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
        errorDto->success = false;
        errorDto->error = "Configuration key not found";
        errorDto->details = "Key '" + key + "' does not exist";
        
        return createDtoResponse(Status::CODE_404, errorDto);
      }
      
      std::string value = config_manager_.getValue<std::string>(key);
      
      auto dto = oatpp::Object<ConfigValueResponseDto>::createShared();
      dto->success = true;
      dto->key = key;
      dto->value = value;
      dto->message = "Configuration value retrieved successfully";
      
      // Add schema information if available
      auto field_schema = schema_.getFieldSchema(key);
      if (field_schema.has_value()) {
        dto->schema_info = oatpp::Object<ConfigSchemaDto>::createShared();
        dto->schema_info->description = field_schema->description;
        dto->schema_info->type = valueTypeToString(field_schema->value_type);
        dto->schema_info->deprecated = field_schema->deprecated;
      }
      
      return createDtoResponse(Status::CODE_200, dto);
    } catch (const std::exception& e) {
      logger_.Error("Failed to get configuration key '" + key + "': " + std::string(e.what()));
      
      auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
      errorDto->success = false;
      errorDto->error = "Failed to retrieve configuration value";
      errorDto->details = e.what();
      
      return createDtoResponse(Status::CODE_500, errorDto);
    }
  }

  /**
   * @brief Set configuration value
   */
  ENDPOINT_INFO(setConfig) {
    info->summary = "Set configuration value";
    info->description = "Updates a specific configuration value with validation against the schema. Changes are applied immediately and may trigger callbacks.";
    info->tags->push_back("Configuration");
    info->pathParams["key"].description = "Configuration key name";
    info->pathParams["key"].required = true;
    info->addConsumes<Object<ConfigUpdateDto>>("application/json", "Configuration update data");
    info->addResponse<Object<ConfigUpdateResponseDto>>(Status::CODE_200, "application/json", "Configuration updated successfully");
    info->addResponse<Object<ErrorResponseDto>>(Status::CODE_400, "application/json", "Validation error or invalid request");
    info->addResponse<Object<ErrorResponseDto>>(Status::CODE_500, "application/json", "Internal server error");
  }
  ENDPOINT("PUT", "/api/config/{key}", setConfig,
           PATH(String, key),
           BODY_DTO(Object<ConfigUpdateDto>, updateDto)) {
    try {
      // Validate the key and value using schema
      auto validation_result = schema_.validate({{key, updateDto->value}});
      if (!validation_result.valid) {
        auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
        errorDto->success = false;
        errorDto->error = "Configuration validation failed";
        errorDto->details = validation_result.error_message;
        
        return createDtoResponse(Status::CODE_400, errorDto);
      }
      
      // Set the configuration value
      bool success = config_manager_.setValue(key, updateDto->value, config::ConfigSource::RUNTIME);
      
      if (success) {
        auto dto = oatpp::Object<ConfigUpdateResponseDto>::createShared();
        dto->success = true;
        dto->key = key;
        dto->oldValue = ""; // TODO: Track old values
        dto->newValue = updateDto->value;
        dto->message = "Configuration updated successfully";
        
        logger_.Info("Configuration updated via API: " + key + " = " + updateDto->value);
        return createDtoResponse(Status::CODE_200, dto);
      } else {
        auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
        errorDto->success = false;
        errorDto->error = "Failed to update configuration";
        errorDto->details = "Configuration validation or update failed";
        
        return createDtoResponse(Status::CODE_400, errorDto);
      }
    } catch (const std::exception& e) {
      logger_.Error("Failed to set configuration key '" + key + "': " + std::string(e.what()));
      
      auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
      errorDto->success = false;
      errorDto->error = "Failed to update configuration value";
      errorDto->details = e.what();
      
      return createDtoResponse(Status::CODE_500, errorDto);
    }
  }

  /**
   * @brief Update multiple configuration values
   */
  ENDPOINT("POST", "/api/config/batch", batchUpdateConfig,
           BODY_DTO(Object<ConfigBatchUpdateDto>, batchDto)) {
    try {
      std::vector<std::string> successful_updates;
      std::vector<std::string> failed_updates;
      
      // Validate all values first
      std::unordered_map<std::string, std::string> config_updates;
      for (const auto& update : *batchDto->updates) {
        config_updates[update->key] = update->value;
      }
      
      auto validation_result = schema_.validate(config_updates);
      if (!validation_result.valid) {
        auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
        errorDto->success = false;
        errorDto->error = "Batch configuration validation failed";
        errorDto->details = validation_result.error_message;
        
        return createDtoResponse(Status::CODE_400, errorDto);
      }
      
      // Apply updates
      for (const auto& update : *batchDto->updates) {
        try {
          bool success = config_manager_.setValue(update->key, update->value, config::ConfigSource::RUNTIME);
          if (success) {
            successful_updates.push_back(update->key);
          } else {
            failed_updates.push_back(update->key);
          }
        } catch (const std::exception& e) {
          failed_updates.push_back(update->key + " (" + e.what() + ")");
        }
      }
      
      auto dto = oatpp::Object<ConfigBatchResponseDto>::createShared();
      dto->success = failed_updates.empty();
      dto->successfulUpdates = oatpp::List<oatpp::String>::createShared();
      for (const auto& key : successful_updates) {
        dto->successfulUpdates->push_back(key);
      }
      
      if (!failed_updates.empty()) {
        dto->failedUpdates = oatpp::List<oatpp::String>::createShared();
        for (const auto& key : failed_updates) {
          dto->failedUpdates->push_back(key);
        }
      }
      
      dto->message = "Batch update completed with " + std::to_string(successful_updates.size()) + 
                     " successes and " + std::to_string(failed_updates.size()) + " failures";
      
      return createDtoResponse(failed_updates.empty() ? Status::CODE_200 : Status::CODE_207, dto);
    } catch (const std::exception& e) {
      logger_.Error("Failed to process batch configuration update: " + std::string(e.what()));
      
      auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
      errorDto->success = false;
      errorDto->error = "Failed to process batch update";
      errorDto->details = e.what();
      
      return createDtoResponse(Status::CODE_500, errorDto);
    }
  }

  /**
   * @brief Delete configuration value
   */
  ENDPOINT("DELETE", "/api/config/{key}", deleteConfig,
           PATH(String, key)) {
    try {
      bool success = config_manager_.removeKey(key);
      
      if (success) {
        auto dto = oatpp::Object<ConfigDeleteResponseDto>::createShared();
        dto->success = true;
        dto->key = key;
        dto->message = "Configuration key deleted successfully";
        
        return createDtoResponse(Status::CODE_200, dto);
      } else {
        auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
        errorDto->success = false;
        errorDto->error = "Configuration key not found";
        errorDto->details = "Key '" + key + "' does not exist";
        
        return createDtoResponse(Status::CODE_404, errorDto);
      }
    } catch (const std::exception& e) {
      logger_.Error("Failed to delete configuration key '" + key + "': " + std::string(e.what()));
      
      auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
      errorDto->success = false;
      errorDto->error = "Failed to delete configuration value";
      errorDto->details = e.what();
      
      return createDtoResponse(Status::CODE_500, errorDto);
    }
  }

  /**
   * @brief Get configuration schema
   */
  ENDPOINT_INFO(getSchema) {
    info->summary = "Get configuration schema";
    info->description = "Retrieves the complete configuration schema including field definitions, types, constraints, and validation rules.";
    info->tags->push_back("Configuration");
    info->addResponse<Object<ConfigSchemaResponseDto>>(Status::CODE_200, "application/json", "Configuration schema retrieved successfully");
    info->addResponse<Object<ErrorResponseDto>>(Status::CODE_500, "application/json", "Internal server error");
  }
  ENDPOINT("GET", "/api/config/schema", getSchema) {
    try {
      auto dto = oatpp::Object<ConfigSchemaResponseDto>::createShared();
      dto->success = true;
      dto->message = "Configuration schema retrieved successfully";
      
      // Convert schema to DTO format
      dto->fields = oatpp::List<Object<ConfigFieldSchemaDto>>::createShared();
      
      for (const auto& field_name : schema_.getFieldNames()) {
        auto field_schema = schema_.getFieldSchema(field_name);
        if (field_schema.has_value()) {
          auto field_dto = oatpp::Object<ConfigFieldSchemaDto>::createShared();
          field_dto->name = field_name;
          field_dto->description = field_schema->description;
          field_dto->type = valueTypeToString(field_schema->value_type);
          field_dto->deprecated = field_schema->deprecated;
          if (field_schema->deprecated) {
            field_dto->deprecatedMessage = field_schema->deprecated_message;
          }
          if (field_schema->default_value.has_value()) {
            field_dto->defaultValue = field_schema->default_value.value();
          }
          
          dto->fields->push_back(field_dto);
        }
      }
      
      return createDtoResponse(Status::CODE_200, dto);
    } catch (const std::exception& e) {
      logger_.Error("Failed to get configuration schema: " + std::string(e.what()));
      
      auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
      errorDto->success = false;
      errorDto->error = "Failed to retrieve configuration schema";
      errorDto->details = e.what();
      
      return createDtoResponse(Status::CODE_500, errorDto);
    }
  }

  /**
   * @brief Export configuration to file
   */
  ENDPOINT_INFO(exportConfig) {
    info->summary = "Export configuration to file";
    info->description = "Exports current configuration to a file in the specified format (YAML, JSON, or TOML). The file format is determined by the file extension.";
    info->tags->push_back("Configuration");
    info->addConsumes<Object<ConfigExportDto>>("application/json", "Export configuration request");
    info->addResponse<Object<ConfigExportResponseDto>>(Status::CODE_200, "application/json", "Configuration exported successfully");
    info->addResponse<Object<ErrorResponseDto>>(Status::CODE_500, "application/json", "Export failed");
  }
  ENDPOINT("POST", "/api/config/export", exportConfig,
           BODY_DTO(Object<ConfigExportDto>, exportDto)) {
    try {
      bool success = config_manager_.saveToFile(exportDto->filePath);
      
      if (success) {
        auto dto = oatpp::Object<ConfigExportResponseDto>::createShared();
        dto->success = true;
        dto->filePath = exportDto->filePath;
        dto->message = "Configuration exported successfully";
        
        return createDtoResponse(Status::CODE_200, dto);
      } else {
        auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
        errorDto->success = false;
        errorDto->error = "Failed to export configuration";
        errorDto->details = "Could not write to file: " + exportDto->filePath;
        
        return createDtoResponse(Status::CODE_500, errorDto);
      }
    } catch (const std::exception& e) {
      logger_.Error("Failed to export configuration: " + std::string(e.what()));
      
      auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
      errorDto->success = false;
      errorDto->error = "Failed to export configuration";
      errorDto->details = e.what();
      
      return createDtoResponse(Status::CODE_500, errorDto);
    }
  }

  /**
   * @brief Import configuration from file
   */
  ENDPOINT("POST", "/api/config/import", importConfig,
           BODY_DTO(Object<ConfigImportDto>, importDto)) {
    try {
      bool success = config_manager_.loadFromFile(importDto->filePath);
      
      if (success) {
        auto dto = oatpp::Object<ConfigImportResponseDto>::createShared();
        dto->success = true;
        dto->filePath = importDto->filePath;
        dto->message = "Configuration imported successfully";
        
        return createDtoResponse(Status::CODE_200, dto);
      } else {
        auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
        errorDto->success = false;
        errorDto->error = "Failed to import configuration";
        errorDto->details = "Could not read from file: " + importDto->filePath;
        
        return createDtoResponse(Status::CODE_500, errorDto);
      }
    } catch (const std::exception& e) {
      logger_.Error("Failed to import configuration: " + std::string(e.what()));
      
      auto errorDto = oatpp::Object<ErrorResponseDto>::createShared();
      errorDto->success = false;
      errorDto->error = "Failed to import configuration";
      errorDto->details = e.what();
      
      return createDtoResponse(Status::CODE_500, errorDto);
    }
  }

private:
  std::string valueTypeToString(config::ConfigValueType type) const {
    switch (type) {
      case config::ConfigValueType::INTEGER: return "integer";
      case config::ConfigValueType::FLOAT: return "float";
      case config::ConfigValueType::BOOLEAN: return "boolean";
      case config::ConfigValueType::STRING: return "string";
      case config::ConfigValueType::ARRAY: return "array";
      case config::ConfigValueType::OBJECT: return "object";
    }
    return "unknown";
  }
};

} // namespace server
} // namespace vectordb

#include OATPP_CODEGEN_END(ApiController)