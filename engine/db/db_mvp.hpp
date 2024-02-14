#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/catalog/meta.hpp"
#include "db/table_mvp.hpp"
#include "utils/status.hpp"
#include "services/embedding_service.hpp"

namespace vectordb {
namespace engine {

class DBMVP {
 public:
  explicit DBMVP(
    meta::DatabaseSchema& database_schema,
    int64_t init_table_scale,
    bool is_leader,
    std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service,
    std::unordered_map<std::string, std::string> &headers);

  ~DBMVP() {}

  Status CreateTable(meta::TableSchema& table_schema);
  Status DeleteTable(const std::string& table_name);
  std::vector<std::string> GetTables();
  std::shared_ptr<TableMVP> GetTable(const std::string& table_name);
  Status Rebuild();
  Status SwapExecutors();

  void SetWALEnabled(bool enabled) {
    for (auto table : tables_) {
      table->SetWALEnabled(enabled);
    }
  }

  void SetLeader(bool is_leader) {
    is_leader_ = is_leader;
    for (auto table : tables_) {
      table->SetLeader(is_leader);
    }
  }

 public:
  std::string db_catalog_path_;                                   // The path to the db catalog.
  // TODO: change to concurrent version.
  std::unordered_map<std::string, size_t> table_name_to_id_map_;  // The table name to table id map.
  std::vector<std::shared_ptr<TableMVP>> tables_;                    // The tables in this database.
  int64_t init_table_scale_;
  std::atomic<bool> is_leader_;
  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service_;
};

using DBMVPPtr = std::shared_ptr<DBMVP>;

}  // namespace engine
}  // namespace vectordb
