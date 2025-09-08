#pragma once

#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>

#include OATPP_CODEGEN_BEGIN(DTO)

namespace vectordb {
namespace server {

/**
 * @brief API Health Status DTO for health check endpoints
 */
class ApiHealthStatusDto : public oatpp::DTO {
  DTO_INIT(ApiHealthStatusDto, DTO);
  
  DTO_FIELD_INFO(status) {
    info->description = "Overall health status of the service";
    info->required = true;
    info->example = oatpp::String("healthy");
  }
  DTO_FIELD(String, status);
  
  DTO_FIELD_INFO(uptime) {
    info->description = "Service uptime in seconds";
    info->required = true;
    info->example = oatpp::Int64(3600);
  }
  DTO_FIELD(Int64, uptime);
  
  DTO_FIELD_INFO(version) {
    info->description = "VectorDB version";
    info->required = true;
    info->example = oatpp::String("1.0.0");
  }
  DTO_FIELD(String, version);
  
  DTO_FIELD_INFO(timestamp) {
    info->description = "Current server timestamp";
    info->required = true;
    info->example = oatpp::String("2024-01-01T00:00:00Z");
  }
  DTO_FIELD(String, timestamp);
  
  DTO_FIELD_INFO(database_status) {
    info->description = "Database connection status";
    info->required = true;
    info->example = oatpp::String("connected");
  }
  DTO_FIELD(String, database_status);
};

/**
 * @brief API Metrics DTO for monitoring endpoints
 */
class ApiMetricsDto : public oatpp::DTO {
  DTO_INIT(ApiMetricsDto, DTO);
  
  DTO_FIELD_INFO(total_requests) {
    info->description = "Total number of API requests processed";
    info->required = true;
    info->example = oatpp::Int64(12345);
  }
  DTO_FIELD(Int64, total_requests);
  
  DTO_FIELD_INFO(successful_requests) {
    info->description = "Number of successful API requests";
    info->required = true;
    info->example = oatpp::Int64(12000);
  }
  DTO_FIELD(Int64, successful_requests);
  
  DTO_FIELD_INFO(failed_requests) {
    info->description = "Number of failed API requests";
    info->required = true;
    info->example = oatpp::Int64(345);
  }
  DTO_FIELD(Int64, failed_requests);
  
  DTO_FIELD_INFO(average_response_time_ms) {
    info->description = "Average response time in milliseconds";
    info->required = true;
    info->example = oatpp::Float64(123.45);
  }
  DTO_FIELD(Float64, average_response_time_ms);
  
  DTO_FIELD_INFO(active_connections) {
    info->description = "Number of currently active connections";
    info->required = true;
    info->example = oatpp::Int32(42);
  }
  DTO_FIELD(Int32, active_connections);
  
  DTO_FIELD_INFO(memory_usage_mb) {
    info->description = "Current memory usage in megabytes";
    info->required = true;
    info->example = oatpp::Float64(256.7);
  }
  DTO_FIELD(Float64, memory_usage_mb);
};

/**
 * @brief Vector Information DTO for describing vector data
 */
class VectorInfoDto : public oatpp::DTO {
  DTO_INIT(VectorInfoDto, DTO);
  
  DTO_FIELD_INFO(dimension) {
    info->description = "Vector dimension (number of components)";
    info->required = true;
    info->example = oatpp::Int32(128);
  }
  DTO_FIELD(Int32, dimension);
  
  DTO_FIELD_INFO(metric_type) {
    info->description = "Distance metric used for similarity calculation";
    info->required = true;
    info->example = oatpp::String("EUCLIDEAN");
  }
  DTO_FIELD(String, metric_type);
  
  DTO_FIELD_INFO(vector_count) {
    info->description = "Total number of vectors in the collection";
    info->required = false;
    info->example = oatpp::Int64(10000);
  }
  DTO_FIELD(Int64, vector_count);
};

/**
 * @brief Field Schema Information DTO for table schema
 */
class FieldSchemaDto : public oatpp::DTO {
  DTO_INIT(FieldSchemaDto, DTO);
  
  DTO_FIELD_INFO(name) {
    info->description = "Field name";
    info->required = true;
    info->example = oatpp::String("embedding");
  }
  DTO_FIELD(String, name);
  
  DTO_FIELD_INFO(data_type) {
    info->description = "Field data type";
    info->required = true;
    info->example = oatpp::String("VECTOR_FLOAT");
  }
  DTO_FIELD(String, data_type);
  
  DTO_FIELD_INFO(dimension) {
    info->description = "Vector dimension (only for vector fields)";
    info->required = false;
    info->example = oatpp::Int32(128);
  }
  DTO_FIELD(Int32, dimension);
  
  DTO_FIELD_INFO(metric_type) {
    info->description = "Distance metric type (only for vector fields)";
    info->required = false;
    info->example = oatpp::String("EUCLIDEAN");
  }
  DTO_FIELD(String, metric_type);
  
  DTO_FIELD_INFO(is_primary_key) {
    info->description = "Whether this field is the primary key";
    info->required = false;
    info->example = oatpp::Boolean(false);
  }
  DTO_FIELD(Boolean, is_primary_key);
  
  DTO_FIELD_INFO(is_indexed) {
    info->description = "Whether this field is indexed for fast retrieval";
    info->required = false;
    info->example = oatpp::Boolean(true);
  }
  DTO_FIELD(Boolean, is_indexed);
};

/**
 * @brief Query Result Metadata DTO
 */
class QueryMetadataDto : public oatpp::DTO {
  DTO_INIT(QueryMetadataDto, DTO);
  
  DTO_FIELD_INFO(total_results) {
    info->description = "Total number of matching results";
    info->required = true;
    info->example = oatpp::Int64(1500);
  }
  DTO_FIELD(Int64, total_results);
  
  DTO_FIELD_INFO(returned_results) {
    info->description = "Number of results returned in this response";
    info->required = true;
    info->example = oatpp::Int32(10);
  }
  DTO_FIELD(Int32, returned_results);
  
  DTO_FIELD_INFO(query_time_ms) {
    info->description = "Query execution time in milliseconds";
    info->required = true;
    info->example = oatpp::Float64(23.45);
  }
  DTO_FIELD(Float64, query_time_ms);
  
  DTO_FIELD_INFO(has_more) {
    info->description = "Whether there are more results available";
    info->required = false;
    info->example = oatpp::Boolean(true);
  }
  DTO_FIELD(Boolean, has_more);
  
  DTO_FIELD_INFO(next_cursor) {
    info->description = "Cursor for pagination (if applicable)";
    info->required = false;
    info->example = oatpp::String("eyJvZmZzZXQiOjEwfQ==");
  }
  DTO_FIELD(String, next_cursor);
};

/**
 * @brief Distance Result DTO for similarity search results
 */
class DistanceResultDto : public oatpp::DTO {
  DTO_INIT(DistanceResultDto, DTO);
  
  DTO_FIELD_INFO(id) {
    info->description = "Record identifier";
    info->required = true;
    info->example = oatpp::String("doc_123");
  }
  DTO_FIELD(String, id);
  
  DTO_FIELD_INFO(distance) {
    info->description = "Distance score (lower means more similar for most metrics)";
    info->required = true;
    info->example = oatpp::Float32(0.25);
  }
  DTO_FIELD(Float32, distance);
  
  DTO_FIELD_INFO(similarity) {
    info->description = "Similarity score (higher means more similar)";
    info->required = false;
    info->example = oatpp::Float32(0.85);
  }
  DTO_FIELD(Float32, similarity);
};

/**
 * @brief Pagination Parameters DTO
 */
class PaginationDto : public oatpp::DTO {
  DTO_INIT(PaginationDto, DTO);
  
  DTO_FIELD_INFO(offset) {
    info->description = "Number of records to skip";
    info->required = false;
    info->example = oatpp::Int32(0);
  }
  DTO_FIELD(Int32, offset, "offset") = 0;
  
  DTO_FIELD_INFO(limit) {
    info->description = "Maximum number of records to return";
    info->required = false;
    info->example = oatpp::Int32(10);
  }
  DTO_FIELD(Int32, limit, "limit") = 10;
  
  DTO_FIELD_INFO(cursor) {
    info->description = "Pagination cursor for efficient large result set traversal";
    info->required = false;
    info->example = oatpp::String("eyJvZmZzZXQiOjEwfQ==");
  }
  DTO_FIELD(String, cursor);
};

/**
 * @brief Filter Condition DTO
 */
class FilterConditionDto : public oatpp::DTO {
  DTO_INIT(FilterConditionDto, DTO);
  
  DTO_FIELD_INFO(field) {
    info->description = "Field name to filter on";
    info->required = true;
    info->example = oatpp::String("category");
  }
  DTO_FIELD(String, field);
  
  DTO_FIELD_INFO(operator_type) {
    info->description = "Filter operator: =, !=, >, <, >=, <=, IN, NOT_IN, LIKE";
    info->required = true;
    info->example = oatpp::String("=");
  }
  DTO_FIELD(String, operator_type, "operator");
  
  DTO_FIELD_INFO(value) {
    info->description = "Value to compare against";
    info->required = true;
    info->example = oatpp::String("electronics");
  }
  DTO_FIELD(Any, value);
  
  DTO_FIELD_INFO(values) {
    info->description = "List of values for IN/NOT_IN operators";
    info->required = false;
  }
  DTO_FIELD(List<Any>, values);
};

} // namespace server
} // namespace vectordb

#include OATPP_CODEGEN_END(DTO)