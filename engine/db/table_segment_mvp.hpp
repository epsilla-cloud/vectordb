#pragma once

#include <atomic>
#include <string>
#include <unordered_map>

#include "db/catalog/meta.hpp"
#include "db/unique_key.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/json.hpp"
#include "utils/status.hpp"
namespace vectordb {
namespace engine {

struct AttributeTable {
 public:
  char* data;
  AttributeTable(int64_t len) {
    data = new char[len];
  }
  ~AttributeTable() {
    delete[] data;
  }
};

class TableSegmentMVP {
 public:
  // Default constructor just for table level init.
  explicit TableSegmentMVP(meta::TableSchema& table_schema, int64_t init_table_scale);
  // Load segment from disk.
  explicit TableSegmentMVP(meta::TableSchema& table_schema, const std::string& db_catalog_path, int64_t init_table_scale);

  Status Init(meta::TableSchema& table_schema, int64_t size_limit);

  Status DoubleSize();

  Status Insert(meta::TableSchema& table_schema, Json& records);
  Status Insert(meta::TableSchema& table_schema, Json& records, int64_t wal_id);

  // Save the table segment to disk.
  Status SaveTableSegment(meta::TableSchema& table_schema, const std::string& db_catalog_path);

  void Debug(meta::TableSchema& table_schema);

  ~TableSegmentMVP();

  std::atomic<bool> skip_sync_disk_;                                   // For default DB, skip sync to disk.
  size_t size_limit_;                                                  // The maximum size of the segment. Default 2^20.
  size_t first_record_id_;                                             // The internal record id of the first record in the segment.
  std::atomic<size_t> record_number_;                                  // Currently how many records in the segment.
  std::unordered_map<std::string, size_t> field_name_mem_offset_map_;  // The offset of each attribute in attribute table.
  std::unordered_map<size_t, size_t> field_id_mem_offset_map_;         // The offset of each attribute in attribute table.
                                                                       // Constructed from schema.
  std::atomic<int64_t> wal_global_id_;                                 // The consumed global wal id.
  int64_t primitive_num_;
  int64_t primitive_offset_;
  int64_t string_num_;
  int64_t vector_num_;
  char* attribute_table_;      // The attribute table in memory (exclude vector attributes and string attributes).
  std::string* string_table_;  // The string attribute table in memory.
  // std::vector<std::vector<std::string>> string_tables_;  // Hold the string attributes.
  std::vector<int64_t> vector_dims_;
  float** vector_tables_;      // The vector attribute tables. Each vector attribute has its own vector table.
                               // (From left to right defined in schema)
  ConcurrentBitset* deleted_;  // The deleted bitset. If the i-th bit is 1, then the i-th record is deleted.
                               // The deleted records still occupy the position in all other structures.
                               // They should be skipped during search.

 private:
  // used to store primary key set for duplication check
  UniqueKey primary_key_;
  // The id of primitive primary key
  // primitive_pk_field_id_.get() != nullptr if there's a primitive pk
  std::unique_ptr<int64_t> primitive_pk_field_id_;
  // The index of primary key among string keys
  // string_pk_offset_.get() != nullptr if there's a string pk
  std::unique_ptr<int64_t> string_pk_offset_;

  // std::shared_ptr<AttributeTable> attribute_table_;  // The attribute table in memory (exclude vector attributes and string attributes).
  // std::shared_ptr<std::string*> string_table_;       // The string attribute table in memory.
  // // std::vector<std::vector<std::string>> string_tables_;  // Hold the string attributes.
  // std::vector<int64_t> vector_dims_;
  // std::vector<std::shared_ptr<float*>> vector_tables_;  // The vector attribute tables. Each vector attribute has its own vector table.
  //                                                       // (From left to right defined in schema)
  // std::shared_ptr<ConcurrentBitset> deleted_;           // The deleted bitset. If the i-th bit is 1, then the i-th record is deleted.
  //                                                       // The deleted records still occupy the position in all other structures.
  //                                                       // They should be skipped during search.
};

}  // namespace engine
}  // namespace vectordb
