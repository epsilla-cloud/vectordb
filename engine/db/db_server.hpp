#pragma once

#include <atomic>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>

#include "db/catalog/meta.hpp"
#include "db/table_mvp.hpp"
#include "db/db_mvp.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

constexpr const long RebuildInterval = 60000; // TODO:: to be config.

class DBServer {
 public:
  DBServer();

  ~DBServer();

  Status LoadDB(const std::string& db_name, std::string& db_catalog_path, int64_t init_table_scale, bool wal_enabled);
  Status UnloadDB(const std::string& db_name);
  Status CreateTable(const std::string& db_name, meta::TableSchema& table_schema);
  Status DropTable(const std::string& db_name, const std::string& table_name);
  std::shared_ptr<DBMVP> GetDB(const std::string& db_name);
  Status Insert(const std::string& db_name, const std::string& table_name, vectordb::Json& records);
  Status Search(
    const std::string& db_name,
    const std::string& table_name,
    std::string& field_name,
    std::vector<std::string>& query_fields,
    int64_t query_dimension,
    const float* query_data,
    const int64_t K,
    vectordb::Json& result,
    bool with_distance
  );
  Status CalcDistance(
    const std::string& db_name,
    const std::string& table_name,
    std::string& field_name,
    int64_t query_dimension,
    const float* query_data,
    int64_t id_list_size,
    const int64_t* id_list,
    vectordb::Json& result
  );
  Status Project(
    const std::string& db_name,
    const std::string& table_name,
    std::vector<std::string>& query_fields,
    vectordb::Json& result
  );

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

 private:
  std::shared_ptr<meta::Meta> meta_;                           // The db meta.
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
      Rebuild(); // Call the Rebuild function

      // Introduce the time delay before the next Rebuild
      std::this_thread::sleep_for(rebuild_interval);
    }
  };

  Status Rebuild();
};


}  // namespace engine
}  // namespace vectordb
