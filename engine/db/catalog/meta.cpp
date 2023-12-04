#include "db/catalog/meta.hpp"

namespace vectordb {
namespace engine {
namespace meta {

FieldType GetFieldType(std::string& type) {
  std::transform(type.begin(), type.end(), type.begin(), [](unsigned char c) {
    return std::toupper(c);
  });

  auto it = fieldTypeMap.find(type);
  if (it != fieldTypeMap.end()) {
    return it->second;
  }

  return vectordb::engine::meta::FieldType::UNKNOWN;
}

MetricType GetMetricType(std::string& type) {
  std::transform(type.begin(), type.end(), type.begin(), [](unsigned char c) {
    return std::toupper(c);
  });

  auto it = metricTypeMap.find(type);
  if (it != metricTypeMap.end()) {
    return it->second;
  }

  return vectordb::engine::meta::MetricType::UNKNOWN;
}

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
