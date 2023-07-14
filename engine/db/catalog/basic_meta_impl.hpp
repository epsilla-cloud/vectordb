// basic_meta_impl.hpp
#pragma once

#include <unordered_map>
#include <unordered_set>

#include "db/catalog/meta.hpp"

namespace vectordb {
namespace engine {
namespace meta {

class BasicMetaImpl : public Meta {
 public:
  explicit BasicMetaImpl();
  ~BasicMetaImpl();

  Status LoadDatabase(std::string& db_catalog_path, const std::string& db_name) override;

  Status HasDatabase(const std::string& db_name, bool& response) override;

  Status GetDatabase(const std::string& db_name, DatabaseSchema& response) override;

  Status UnloadDatabase(const std::string& db_name) override;

  Status DropDatabase(const std::string& db_name) override;

  Status CreateTable(std::string& db_name, TableSchema& table_schema) override;

  Status HasTable(const std::string& db_name, const std::string& table_name, bool& response) override;

  Status GetTable(const std::string& db_name, const std::string& table_name, TableSchema& response) override;

  Status DropTable(const std::string& db_name, const std::string& table_name) override;

 private:
  std::unordered_map<std::string, DatabaseSchema> databases_;
  std::unordered_set<std::string> loaded_databases_paths_;  // We cannot allow loading the same database twice
};  // BasicMetaImpl

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
