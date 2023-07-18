#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/catalog/meta.hpp"
#include "db/table.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

class DB {
 public:
  explicit DB(meta::DatabaseSchema& database_schema);

  ~DB();

 private:
  std::unordered_map<std::string, size_t> table_name_to_id_map_;  // The table name to table id map.
  std::vector<Table> tables_;                                     // The tables in this database.
};

}  // namespace engine
}  // namespace vectordb
