#pragma once

#include "db/catalog/meta.hpp"

namespace vectordb {
namespace server {
namespace web {

class WebUtil {
 public:
  static vectordb::engine::meta::FieldType GetFieldType(std::string& type);
  static vectordb::engine::meta::MetricType GetMetricType(std::string& type);
};

}
}
}
