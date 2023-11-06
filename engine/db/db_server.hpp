#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "db/catalog/meta.hpp"
#include "db/db_mvp.hpp"
#include "db/table_mvp.hpp"
#include "query/expr/expr.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

constexpr const long RebuildInterval = 60000;  // TODO:: to be config.

class DBServer {
 public:
  DBServer();

  ~DBServer();

  Status LoadDB(const std::string& db_name, std::string& db_catalog_path, int64_t init_table_scale, bool wal_enabled);
  Status UnloadDB(const std::string& db_name);
  Status CreateTable(const std::string& db_name, meta::TableSchema& table_schema, size_t& table_id);
  Status DropTable(const std::string& db_name, const std::string& table_name);
  std::shared_ptr<DBMVP> GetDB(const std::string& db_name);
  Status ListTables(const std::string& db_name, std::vector<std::string>& table_names);
  Status Insert(const std::string& db_name, const std::string& table_name, vectordb::Json& records);
  Status InsertPrepare(const std::string& db_name, const std::string& table_name, vectordb::Json& pks, vectordb::Json& result);
  Status Delete(
    const std::string& db_name,
    const std::string& table_name,
    vectordb::Json& pkList,
    const std::string& filter
  );
  Status Search(
    const std::string& db_name,
    const std::string& table_name,
    std::string& field_name,
    std::vector<std::string>& query_fields,
    int64_t query_dimension,
    const float* query_data,
    const int64_t limit,
    vectordb::Json& result,
    const std::string& filter,
    bool with_distance
  );

  Status Project(
      const std::string& db_name,
      const std::string& table_name,
      std::vector<std::string>& query_fields,
      vectordb::Json &primary_keys,
      const std::string& filter,
      const int64_t skip,
      const int64_t limit,
      vectordb::Json& result);

  void StartRebuild() {
    if (rebuild_started_) {
      return;
    }
    rebuild_started_ = true;
    // Start the thread to periodically call Rebuild
    rebuild_thread_ = std::thread(&DBServer::RebuildPeriodically, this);
  }

  Status RebuildOndemand() {
    if (rebuild_started_) {
      return Status(DB_UNEXPECTED_ERROR, "Auto rebuild is enabled. Cannot conduct on-demand rebuild.");
    }
    rebuild_started_ = true;
    Rebuild();
    rebuild_started_ = false;
    return Status::OK();
  }

  void SetLeader(bool is_leader) {
    is_leader_ = is_leader;
    meta_->SetLeader(is_leader_);
    for (auto db : dbs_) {
      db->SetLeader(is_leader);
    }
  }

 private:
  std::shared_ptr<meta::Meta> meta_;  // The db meta.
  // TODO: change to concurrent version.
  std::unordered_map<std::string, size_t> db_name_to_id_map_;  // The db name to db index map.
  std::vector<std::shared_ptr<DBMVP>> dbs_;                    // The dbs.
  std::thread rebuild_thread_;
  bool stop_rebuild_thread_ = false;
  bool rebuild_started_ = false;

  // periodically in a separate thread
  void RebuildPeriodically() {
    const std::chrono::milliseconds rebuild_interval(RebuildInterval);

    while (!stop_rebuild_thread_) {
      Rebuild();  // Call the Rebuild function

      // Introduce the time delay before the next Rebuild
      std::this_thread::sleep_for(rebuild_interval);
    }
  };

  Status Rebuild();

  std::atomic<bool> is_leader_;
};

}  // namespace engine
}  // namespace vectordb
