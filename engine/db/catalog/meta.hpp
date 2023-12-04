#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "db/catalog/meta_types.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {
namespace meta {

class FieldsHolder;

class Meta {
 public:
  virtual ~Meta() = default;

  virtual Status LoadDatabase(const std::string& db_catalog_path, const std::string& db_name) = 0;

  virtual Status HasDatabase(const std::string& db_name, bool& response) = 0;

  virtual Status GetDatabase(const std::string& db_name, DatabaseSchema& response) = 0;

  virtual Status UnloadDatabase(const std::string& db_name) = 0;

  virtual Status DropDatabase(const std::string& db_name) = 0;

  virtual Status CreateTable(const std::string& db_name, TableSchema& table_schema, size_t& table_id) = 0;

  virtual Status HasTable(const std::string& db_name, const std::string& table_name, bool& response) = 0;

  virtual Status GetTable(const std::string& db_name, const std::string& table_name, TableSchema& response) = 0;

  virtual Status DropTable(const std::string& db_name, const std::string& table_name) = 0;

  virtual void SetLeader(bool is_leader) = 0;

};  // MetaData

FieldType GetFieldType(std::string& type);

MetricType GetMetricType(std::string& type);

using MetaPtr = std::shared_ptr<Meta>;

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
