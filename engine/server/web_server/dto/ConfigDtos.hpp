#pragma once

#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>

#include OATPP_CODEGEN_BEGIN(DTO)

namespace vectordb {
namespace server {

/**
 * @brief Configuration statistics DTO
 */
class ConfigStatsDto : public oatpp::DTO {
  DTO_INIT(ConfigStatsDto, DTO);
  
  DTO_FIELD(UInt64, totalKeys);
  DTO_FIELD(UInt64, totalCallbacks);
  DTO_FIELD(UInt64, reloadCount);
  DTO_FIELD(Boolean, watchingEnabled);
  DTO_FIELD(String, configFilePath);
};

/**
 * @brief Configuration data container DTO
 */
class ConfigDataDto : public oatpp::DTO {
  DTO_INIT(ConfigDataDto, DTO);
  
  DTO_FIELD(UnorderedFields<String>, config);
  DTO_FIELD(Object<ConfigStatsDto>, stats);
};

/**
 * @brief Configuration response DTO
 */
class ConfigResponseDto : public oatpp::DTO {
  DTO_INIT(ConfigResponseDto, DTO);
  
  DTO_FIELD(Boolean, success);
  DTO_FIELD(String, message);
  DTO_FIELD(Object<ConfigDataDto>, data);
};

/**
 * @brief Configuration schema information DTO
 */
class ConfigSchemaDto : public oatpp::DTO {
  DTO_INIT(ConfigSchemaDto, DTO);
  
  DTO_FIELD(String, description);
  DTO_FIELD(String, type);
  DTO_FIELD(Boolean, deprecated);
  DTO_FIELD(String, deprecatedMessage);
};

/**
 * @brief Single configuration value response DTO
 */
class ConfigValueResponseDto : public oatpp::DTO {
  DTO_INIT(ConfigValueResponseDto, DTO);
  
  DTO_FIELD(Boolean, success);
  DTO_FIELD(String, key);
  DTO_FIELD(String, value);
  DTO_FIELD(String, message);
  DTO_FIELD(Object<ConfigSchemaDto>, schema_info);
};

/**
 * @brief Configuration update request DTO
 */
class ConfigUpdateDto : public oatpp::DTO {
  DTO_INIT(ConfigUpdateDto, DTO);
  
  DTO_FIELD(String, value);
  DTO_FIELD(String, comment); // Optional comment for the update
};

/**
 * @brief Configuration update response DTO
 */
class ConfigUpdateResponseDto : public oatpp::DTO {
  DTO_INIT(ConfigUpdateResponseDto, DTO);
  
  DTO_FIELD(Boolean, success);
  DTO_FIELD(String, key);
  DTO_FIELD(String, oldValue);
  DTO_FIELD(String, newValue);
  DTO_FIELD(String, message);
};

/**
 * @brief Single configuration update for batch operations
 */
class ConfigBatchUpdateItemDto : public oatpp::DTO {
  DTO_INIT(ConfigBatchUpdateItemDto, DTO);
  
  DTO_FIELD(String, key);
  DTO_FIELD(String, value);
};

/**
 * @brief Batch configuration update request DTO
 */
class ConfigBatchUpdateDto : public oatpp::DTO {
  DTO_INIT(ConfigBatchUpdateDto, DTO);
  
  DTO_FIELD(List<Object<ConfigBatchUpdateItemDto>>, updates);
  DTO_FIELD(String, comment); // Optional comment for the batch update
};

/**
 * @brief Batch configuration update response DTO
 */
class ConfigBatchResponseDto : public oatpp::DTO {
  DTO_INIT(ConfigBatchResponseDto, DTO);
  
  DTO_FIELD(Boolean, success);
  DTO_FIELD(String, message);
  DTO_FIELD(List<String>, successfulUpdates);
  DTO_FIELD(List<String>, failedUpdates);
};

/**
 * @brief Configuration deletion response DTO
 */
class ConfigDeleteResponseDto : public oatpp::DTO {
  DTO_INIT(ConfigDeleteResponseDto, DTO);
  
  DTO_FIELD(Boolean, success);
  DTO_FIELD(String, key);
  DTO_FIELD(String, message);
};

/**
 * @brief Configuration field schema DTO
 */
class ConfigFieldSchemaDto : public oatpp::DTO {
  DTO_INIT(ConfigFieldSchemaDto, DTO);
  
  DTO_FIELD(String, name);
  DTO_FIELD(String, description);
  DTO_FIELD(String, type);
  DTO_FIELD(String, defaultValue);
  DTO_FIELD(Boolean, deprecated);
  DTO_FIELD(String, deprecatedMessage);
  DTO_FIELD(List<String>, aliases);
};

/**
 * @brief Configuration schema response DTO
 */
class ConfigSchemaResponseDto : public oatpp::DTO {
  DTO_INIT(ConfigSchemaResponseDto, DTO);
  
  DTO_FIELD(Boolean, success);
  DTO_FIELD(String, message);
  DTO_FIELD(List<Object<ConfigFieldSchemaDto>>, fields);
};

/**
 * @brief Configuration export request DTO
 */
class ConfigExportDto : public oatpp::DTO {
  DTO_INIT(ConfigExportDto, DTO);
  
  DTO_FIELD(String, filePath);
  DTO_FIELD(String, format, "yaml"); // Default to YAML format
};

/**
 * @brief Configuration export response DTO
 */
class ConfigExportResponseDto : public oatpp::DTO {
  DTO_INIT(ConfigExportResponseDto, DTO);
  
  DTO_FIELD(Boolean, success);
  DTO_FIELD(String, filePath);
  DTO_FIELD(String, message);
};

/**
 * @brief Configuration import request DTO
 */
class ConfigImportDto : public oatpp::DTO {
  DTO_INIT(ConfigImportDto, DTO);
  
  DTO_FIELD(String, filePath);
  DTO_FIELD(Boolean, merge, true); // Whether to merge with existing config or replace
};

/**
 * @brief Configuration import response DTO
 */
class ConfigImportResponseDto : public oatpp::DTO {
  DTO_INIT(ConfigImportResponseDto, DTO);
  
  DTO_FIELD(Boolean, success);
  DTO_FIELD(String, filePath);
  DTO_FIELD(String, message);
  DTO_FIELD(List<String>, importedKeys); // List of keys that were imported
};

/**
 * @brief Generic error response DTO
 */
class ErrorResponseDto : public oatpp::DTO {
  DTO_INIT(ErrorResponseDto, DTO);
  
  DTO_FIELD(Boolean, success);
  DTO_FIELD(String, error);
  DTO_FIELD(String, details);
};

} // namespace server
} // namespace vectordb

#include OATPP_CODEGEN_END(DTO)