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

  ~DBServer() {}

  Status LoadDB(const std::string& db_name, std::string& db_catalog_path);
  Status UnloadDB(const std::string& db_name);
  Status CreateTable(const std::string& db_name, meta::TableSchema& table_schema);
  Status DropTable(const std::string& db_name, const std::string& table_name);
  std::shared_ptr<DBMVP> GetDB(const std::string& db_name);
  Status Rebuild();
  Status Insert(const std::string& db_name, const std::string& table_name, vectordb::Json& records);
  Status Search(
    const std::string& db_name,
    const std::string& table_name, 
    std::string& field_name,
    std::vector<std::string>& query_fields, 
    const float* query_data, 
    const int64_t K, 
    vectordb::Json& result
  );

 private:
  std::shared_ptr<meta::Meta> meta_;                           // The db meta.
  // TODO: change to concurrent version.
  std::unordered_map<std::string, size_t> db_name_to_id_map_;  // The db name to db index map.
  std::vector<std::shared_ptr<DBMVP>> dbs_;                    // The dbs.
};


}  // namespace engine
}  // namespace vectordb
