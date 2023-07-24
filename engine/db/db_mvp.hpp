#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/catalog/meta.hpp"
#include "db/table_mvp.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

class DBMVP {
 public:
  explicit DBMVP(meta::DatabaseSchema& database_schema);

  ~DBMVP() {}

  Status CreateTable(meta::TableSchema& table_schema);
  Status DeleteTable(const std::string& table_name);
  std::shared_ptr<TableMVP> GetTable(const std::string& table_name);
  Status Rebuild();

 public:
  std::string db_catalog_path_;                                   // The path to the db catalog.
  // TODO: change to concurrent version.
  std::unordered_map<std::string, size_t> table_name_to_id_map_;  // The table name to table id map.
  std::vector<std::shared_ptr<TableMVP>> tables_;                    // The tables in this database.
};

using DBMVPPtr = std::shared_ptr<DBMVP>;

}  // namespace engine
}  // namespace vectordb