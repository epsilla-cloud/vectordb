#pragma once

#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "db/catalog/meta_types.hpp"

namespace vectordb {
namespace engine {
namespace meta {

class FieldsHolder;

class Meta {
 public:
  virtual ~Meta() = default;

  virtual Status CreateDatabase(DatabaseSchema& database_schema) = 0;

  virtual Status DescribeDatabase(DatabaseSchema& database_schema) = 0;

  virtual Status DropDatabase(const std::string& database_id) = 0;

  virtual Status CreateTable(TableSchema& table_schema) = 0;

  virtual Status DescribeTable(TableSchema& table_schema) = 0;

  virtual Status DropTable(const std::string& table_id) = 0;

  virtual Status CreateField(FieldSchema& field_schema) = 0;

  virtual Status DescribeField(FieldSchema& field_schema) = 0;

  virtual Status DropField(const std::string& field_id) = 0;
};  // MetaData

using MetaPtr = std::shared_ptr<Meta>;

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
