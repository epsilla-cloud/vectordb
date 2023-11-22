#pragma once

#include <atomic>
#include <iostream>
#include <string>
#include <typeinfo>
#include <unordered_map>

#include "db/catalog/meta.hpp"
#include "db/sparse_vector.hpp"
#include "db/unique_key.hpp"
#include "query/expr/expr_evaluator.hpp"
#include "query/expr/expr_types.hpp"
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
  Status InsertPrepare(meta::TableSchema& table_schema, Json& pks, Json& result);

  Status Delete(Json& records, std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes, int64_t wal_id);

  // Convert a primary key to an internal id
  bool PK2ID(Json& record, size_t& id);

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
  int64_t var_len_attr_num_;
  int64_t vector_num_;
  char* attribute_table_;                                        // The attribute table in memory (exclude vector attributes and string attributes).
  std::vector<std::vector<unsigned char>>* var_len_attr_table_;  // The variable length attribute table in memory.
  // std::vector<std::vector<std::string>> string_tables_;  // Hold the string attributes.
  std::vector<int64_t> vector_dims_;
  float** vector_tables_;      // The vector attribute tables. Each vector attribute has its own vector table.
                               // (From left to right defined in schema)
  ConcurrentBitset* deleted_;  // The deleted bitset. If the i-th bit is 1, then the i-th record is deleted.
                               // The deleted records still occupy the position in all other structures.
                               // They should be skipped during search.

  bool isIntPK() const;
  bool isStringPK() const;
  meta::FieldType pkType() const;
  meta::FieldSchema pkField() const;
  int64_t pkFieldIdx() const;
  bool isEntryDeleted(int64_t id) const;

 private:
  // used to store primary key set for duplication check
  UniqueKey primary_key_;
  // The index of primary key in schema fields
  // pk_field_id_.get() != nullptr if there's a pk
  std::unique_ptr<int64_t> pk_field_idx_;
  // The index of primary key among string keys
  // string_pk_offset_.get() != nullptr if there's a string pk
  std::unique_ptr<int64_t> string_pk_offset_;

  // save a copy of the schema - the lifecycle of the schema is not managed by the segment, so
  // we can keep the raw pointer here. In addition, the table_segment_mvp's lifecycle is always longer
  // than the owner, so so don't need to worry about invalid pointer here.
  meta::TableSchema schema;

  Status DeleteByStringPK(const std::string& pk, vectordb::query::expr::ExprEvaluator& evaluator, int filter_root_index);

  template <typename T>
  Status DeleteByIntPK(T pk, vectordb::query::expr::ExprEvaluator& evaluator, int filter_root_index) {
    size_t result = 0;
    auto found = primary_key_.getKey(pk, result);
    if (found && evaluator.LogicalEvaluate(filter_root_index, result)) {
      deleted_->set(result);
      primary_key_.removeKey(pk);
      return Status::OK();
    }
    return Status(RECORD_NOT_FOUND, "Record with primary key not exist or skipped by filter: " + pk);
  }

  Status DeleteByID(const size_t id, vectordb::query::expr::ExprEvaluator& evaluator, int filter_root_index);

  // std::shared_ptr<AttributeTable> attribute_table_;  // The attribute table in memory (exclude vector attributes and string attributes).
  // std::shared_ptr<std::string*> var_len_attr_table_;       // The string attribute table in memory.
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
