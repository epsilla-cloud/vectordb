#include <iostream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <string>

#include "server/web_server/utils/util.hpp"
#include "db/catalog/meta.hpp"

namespace vectordb {
namespace server {
namespace web {
  static const std::unordered_map<std::string, vectordb::engine::meta::FieldType> fieldTypeMap = {
      {"TINYINT", vectordb::engine::meta::FieldType::INT1},
      {"SMALLINT", vectordb::engine::meta::FieldType::INT2},
      {"INT", vectordb::engine::meta::FieldType::INT4},
      {"BIGINT", vectordb::engine::meta::FieldType::INT8},
      {"FLOAT", vectordb::engine::meta::FieldType::FLOAT},
      {"DOUBLE", vectordb::engine::meta::FieldType::DOUBLE},
      {"STRING", vectordb::engine::meta::FieldType::STRING},
      {"BOOL", vectordb::engine::meta::FieldType::BOOL},
      {"JSON", vectordb::engine::meta::FieldType::JSON},
      {"VECTOR_FLOAT", vectordb::engine::meta::FieldType::VECTOR_FLOAT},
      {"VECTOR_DOUBLE", vectordb::engine::meta::FieldType::VECTOR_DOUBLE},
      {"UNKNOWN", vectordb::engine::meta::FieldType::UNKNOWN}
  };

  static const std::unordered_map<std::string, vectordb::engine::meta::MetricType> metricTypeMap = {
      {"EUCLIDEAN", vectordb::engine::meta::MetricType::EUCLIDEAN},
      {"COSINE", vectordb::engine::meta::MetricType::COSINE},
      {"DOT_PRODUCT", vectordb::engine::meta::MetricType::DOT_PRODUCT}
  };

  vectordb::engine::meta::FieldType WebUtil::GetFieldType(std::string& type) {
    std::transform(type.begin(), type.end(), type.begin(), [](unsigned char c) {
        return std::toupper(c);
    });

    auto it = fieldTypeMap.find(type);
    if (it != fieldTypeMap.end()) {
        return it->second;
    }

    return vectordb::engine::meta::FieldType::UNKNOWN;
  }

  vectordb::engine::meta::MetricType WebUtil::GetMetricType(std::string& type) {
    std::transform(type.begin(), type.end(), type.begin(), [](unsigned char c) {
        return std::toupper(c);
    });

    auto it = metricTypeMap.find(type);
    if (it != metricTypeMap.end()) {
        return it->second;
    }

    // TODO: need to return UNKNOWN after EUCLIDEAN support only.
    return vectordb::engine::meta::MetricType::EUCLIDEAN;
  }

  std::string WebUtil::JoinStrs(const std::vector<std::string>& strings, const std::string& delimiter) {
      std::ostringstream oss;
      bool first = true;

      for (const auto& str : strings) {
          if (!first) {
              oss << delimiter;
          }
          oss << str;
          first = false;
      }

      return oss.str();
  }
} // namespace web
} // namespace server
} // namespace vectordb
