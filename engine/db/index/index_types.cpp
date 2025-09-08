#include "db/index/index_types.hpp"
#include <unordered_map>
#include <algorithm>

namespace vectordb {
namespace engine {
namespace index {

IndexCharacteristics GetIndexCharacteristics(IndexType type) {
  static const std::unordered_map<IndexType, IndexCharacteristics> characteristics = {
    {IndexType::NSG, {
      .build_time_complexity_factor = 7,  // Relatively slow to build
      .memory_usage_factor = 6,           // Moderate memory usage
      .search_time_complexity_factor = 3, // Fast search
      .accuracy_factor = 0.99,            // Very high accuracy
      .min_recommended_vectors = 1000,
      .max_recommended_vectors = 10000000,
      .supports_incremental_update = false,
      .supports_deletion = false,
      .memory_mappable = true
    }},
    
    {IndexType::IVF, {
      .build_time_complexity_factor = 4,  // Moderate build time
      .memory_usage_factor = 5,           // Moderate memory usage
      .search_time_complexity_factor = 4, // Good search speed
      .accuracy_factor = 0.95,            // Good accuracy
      .min_recommended_vectors = 10000,
      .max_recommended_vectors = 100000000,
      .supports_incremental_update = true,
      .supports_deletion = true,
      .memory_mappable = true
    }},
    
    {IndexType::PQ, {
      .build_time_complexity_factor = 5,  // Moderate build time
      .memory_usage_factor = 2,           // Very memory efficient
      .search_time_complexity_factor = 6, // Slower search due to decoding
      .accuracy_factor = 0.85,            // Lower accuracy due to compression
      .min_recommended_vectors = 50000,
      .max_recommended_vectors = 1000000000,
      .supports_incremental_update = true,
      .supports_deletion = true,
      .memory_mappable = true
    }},
    
    {IndexType::IVF_PQ, {
      .build_time_complexity_factor = 6,  // Higher build time  
      .memory_usage_factor = 3,           // Good memory efficiency
      .search_time_complexity_factor = 5, // Balanced search speed
      .accuracy_factor = 0.90,            // Good accuracy with compression
      .min_recommended_vectors = 100000,
      .max_recommended_vectors = 1000000000,
      .supports_incremental_update = true,
      .supports_deletion = true,
      .memory_mappable = true
    }},
    
    {IndexType::FLAT, {
      .build_time_complexity_factor = 1,  // Instant build
      .memory_usage_factor = 8,           // High memory usage (stores raw data)
      .search_time_complexity_factor = 10,// Very slow search (brute force)
      .accuracy_factor = 1.0,             // Perfect accuracy
      .min_recommended_vectors = 1,
      .max_recommended_vectors = 100000,  // Only good for small datasets
      .supports_incremental_update = true,
      .supports_deletion = true,
      .memory_mappable = true
    }}
  };
  
  auto it = characteristics.find(type);
  if (it != characteristics.end()) {
    return it->second;
  }
  
  // Default characteristics for unknown types
  return IndexCharacteristics{
    .build_time_complexity_factor = 5,
    .memory_usage_factor = 5,
    .search_time_complexity_factor = 5,
    .accuracy_factor = 0.9,
    .min_recommended_vectors = 1000,
    .max_recommended_vectors = 1000000,
    .supports_incremental_update = false,
    .supports_deletion = false,
    .memory_mappable = false
  };
}

std::string GetIndexTypeName(IndexType type) {
  static const std::unordered_map<IndexType, std::string> names = {
    {IndexType::NSG, "NSG"},
    {IndexType::IVF, "IVF"},
    {IndexType::PQ, "PQ"},
    {IndexType::IVF_PQ, "IVF_PQ"},
    {IndexType::FLAT, "FLAT"},
    {IndexType::AUTO, "AUTO"}
  };
  
  auto it = names.find(type);
  return it != names.end() ? it->second : "UNKNOWN";
}

IndexType ParseIndexType(const std::string& type_str) {
  static const std::unordered_map<std::string, IndexType> type_map = {
    {"NSG", IndexType::NSG},
    {"nsg", IndexType::NSG},
    {"IVF", IndexType::IVF},
    {"ivf", IndexType::IVF},
    {"PQ", IndexType::PQ},
    {"pq", IndexType::PQ},
    {"IVF_PQ", IndexType::IVF_PQ},
    {"ivf_pq", IndexType::IVF_PQ},
    {"IVFPQ", IndexType::IVF_PQ},
    {"ivfpq", IndexType::IVF_PQ},
    {"FLAT", IndexType::FLAT},
    {"flat", IndexType::FLAT},
    {"BRUTE_FORCE", IndexType::FLAT},
    {"brute_force", IndexType::FLAT},
    {"AUTO", IndexType::AUTO},
    {"auto", IndexType::AUTO}
  };
  
  auto it = type_map.find(type_str);
  return it != type_map.end() ? it->second : IndexType::AUTO;
}

} // namespace index
} // namespace engine
} // namespace vectordb