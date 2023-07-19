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

  Status Start();
  Status Stop();

  Status CreateTable(meta::TableSchema& table_schema_);
  Status DescribeTable();
  Status DeleteTable();

  Status AllTables();

  Status PreloadTable();
  Status GetTableRowCount();
  Status InsertVectors();



 private:
  std::unordered_map<std::string, size_t> table_name_to_id_map_;  // The table name to table id map.
  std::vector<Table> tables_;                                     // The tables in this database.
};


using DBPtr = std::shared_ptr<DB>;


//
//struct DBMetaOptions {
//  std::string path_;
//  std::vector<std::string> slave_paths_;
//  std::string backend_uri_;
//};  // DBMetaOptions
//
//
//struct DBOptions {
//  typedef enum { SINGLE = 0, CLUSTER_READONLY, CLUSTER_WRITABLE } MODE;
//
//  int mode_ = MODE::SINGLE;
//
//  DBMetaOptions meta_;
//
//  size_t insert_buffer_size_ = 4 * 1024UL * 1024UL * 1024UL;
//  bool insert_cache_immediately_ = false;
//};  // Options


}  // namespace engine
}  // namespace vectordb
