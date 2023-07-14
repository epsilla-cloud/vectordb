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

  virtual Status LoadDatabase(std::string& db_catalog_path, const std::string& db_name) = 0;

  virtual Status HasDatabase(const std::string& db_name, bool& response) = 0;

  virtual Status GetDatabase(const std::string& db_name, DatabaseSchema& response) = 0;

  virtual Status UnloadDatabase(const std::string& db_name) = 0;

  virtual Status DropDatabase(const std::string& db_name) = 0;

  virtual Status CreateTable(std::string& db_name, TableSchema& table_schema) = 0;

  virtual Status HasTable(std::string& db_name, bool& response) = 0;

  virtual Status GetTable(std::string& db_name, TableSchema& response) = 0;

  virtual Status DropTable(std::string& db_name, const std::string& table_name) = 0;
};  // MetaData

using MetaPtr = std::shared_ptr<Meta>;

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
