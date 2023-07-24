#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/catalog/meta.hpp"
#include "db/table_mvp.hpp"
#include "db/db_mvp.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

class DBServer {
 public:
  DBServer();

  ~DBMVP() {}

  Status LoadDB(meta::DatabaseSchema& db_schema);
  Status DropDB(const std::string& db_name);
  std::shared_ptr<DBMVP> GetDB(const std::string& db_name);

 private:
  std::unordered_map<std::string, size_t> db_name_to_id_map_;  // The db name to db index map.
  std::vector<std::shared_ptr<DBMVP>> dbs_;                    // The dbs.
};


}  // namespace engine
}  // namespace vectordb
