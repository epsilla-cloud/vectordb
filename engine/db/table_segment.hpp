#pragma once

#include <atomic>
#include <iostream>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <shared_mutex>

#include "db/catalog/meta.hpp"
#include "db/unique_key.hpp"
#include "db/index/spatial/geoindex.hpp"
#include "db/vector.hpp"
#include "db/concurrent_operations.hpp"
#include "query/expr/expr_evaluator.hpp"
#include "query/expr/expr_types.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/json.hpp"
#include "utils/status.hpp"
#include "services/embedding_service.hpp"
#include "logger/logger.hpp"
#include "config/config.hpp"

namespace vectordb {
namespace engine {

struct AttributeTable {
 public:
  std::unique_ptr<char[]> data;
  int64_t length;
  
  AttributeTable(int64_t len) : length(len), data(std::make_unique<char[]>(len)) {
  }
  
  // Copy constructor - deep copy
  AttributeTable(const AttributeTable& other) : length(other.length), 
    data(std::make_unique<char[]>(other.length)) {
    std::memcpy(data.get(), other.data.get(), length);
  }
  
  // Move constructor - default is fine with unique_ptr
  AttributeTable(AttributeTable&& other) noexcept = default;
  
  // Copy assignment operator
  AttributeTable& operator=(const AttributeTable& other) {
    if (this != &other) {
      length = other.length;
      data = std::make_unique<char[]>(length);
      std::memcpy(data.get(), other.data.get(), length);
    }
    return *this;
  }
  
  // Move assignment operator - default is fine with unique_ptr
  AttributeTable& operator=(AttributeTable&& other) noexcept = default;
  
  // Destructor - default is fine with unique_ptr
  ~AttributeTable() = default;
  
  // Convenience method to get raw pointer for existing code compatibility
  char* get() { return data.get(); }
  const char* get() const { return data.get(); }
};

/**
 * @brief Modern table segment implementation with improved memory management
 * and thread safety - migrated from TableSegmentMVP
 * 
 * This class provides core data storage functionality for vector database tables,
 * including:
 * - Column-oriented storage for vectors
 * - Row-oriented storage for attributes  
 * - Primary key indexing and uniqueness constraints
 * - Soft deletion with bitset tracking
 * - Concurrent read/write operations
 * - Memory-efficient data structures
 * - Integration with geospatial indexing
 */
class TableSegment {
 public:
  // Constructors
  explicit TableSegment(meta::TableSchema& table_schema, int64_t init_table_scale, std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service);
  explicit TableSegment(meta::TableSchema& table_schema, const std::string& db_catalog_path, int64_t init_table_scale, std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service);
  
  // Factory method for loading from disk (safer than constructor throwing)
  static std::pair<Status, std::shared_ptr<TableSegment>> CreateFromDisk(
      meta::TableSchema& table_schema, 
      const std::string& db_catalog_path, 
      int64_t init_table_scale, 
      std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service);

  Status Init(meta::TableSchema& table_schema, int64_t size_limit);
  Status DoubleSize();

  Status Insert(meta::TableSchema& table_schema, Json& records, int64_t wal_id, std::unordered_map<std::string, std::string> &headers, bool upsert = false);
  Status InsertPrepare(meta::TableSchema& table_schema, Json& pks, Json& result);

  Status Delete(Json& records, std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes, int64_t wal_id);
  
  // Hard delete methods - physically remove data instead of marking as deleted
  Status HardDelete(Json& records, std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes, int64_t wal_id);
  Status SafeHardDeleteByID(size_t id); // Concurrency-safe version
  Status HardDeleteByStringPK(const std::string& pk, vectordb::query::expr::ExprEvaluator& evaluator, int filter_root_index);
  
  // Batch hard delete for better concurrency control
  Status BatchHardDelete(const std::vector<size_t>& ids_to_delete);
  
  // Helper methods for concurrent hard delete
  Status CompactDataStructures(const std::vector<size_t>& sorted_ids);
  void RebuildPrimaryKeyIndex();
  
  // HardDeleteByIntPK is implemented inline below

  // Convert a primary key to an internal id
  bool PK2ID(Json& record, size_t& id);

  // Compact the segment by removing deleted records and reorganizing data
  Status CompactSegment();

  // Get the ratio of deleted records to total records
  double GetDeletedRatio() const;

  // Check if compaction is needed based on deleted ratio threshold
  bool NeedsCompaction(double threshold = 0.3) const;

  // Save the table segment to disk.
  Status SaveTableSegment(meta::TableSchema& table_schema, const std::string& db_catalog_path, bool force = false);

  size_t GetRecordCount();
  void Debug(meta::TableSchema& table_schema);
  Status Release();
  ~TableSegment();

  // All the public member variables from TableSegmentMVP
  std::atomic<bool> skip_sync_disk_;                                              // For default DB, skip sync to disk.
  size_t size_limit_;                                                             // The maximum size of the segment. Default 2^20.
  size_t first_record_id_;                                                        // The internal record id of the first record in the segment.
  std::atomic<size_t> record_number_;                                             // Currently how many records in the segment.
  std::unordered_map<std::string, size_t> field_name_mem_offset_map_;             // The offset of each attribute in attribute table.
  std::unordered_map<size_t, size_t> field_id_mem_offset_map_;                    // The offset of each attribute in attribute table.
  std::unordered_map<std::string, size_t> vec_field_name_executor_pool_idx_map_;  // The index of a vector field in the executor pool.

  std::atomic<int64_t> wal_global_id_;  // The consumed global wal id.
  int64_t primitive_num_;
  int64_t primitive_offset_;
  int64_t var_len_attr_num_;
  std::vector<meta::FieldType> var_len_attr_field_type_;
  int64_t sparse_vector_num_;
  int64_t dense_vector_num_;
  std::unique_ptr<char[]> attribute_table_;                         // The attribute table in memory (exclude vector attributes and string attributes).
  std::vector<VariableLenAttrColumnContainer> var_len_attr_table_;  // The variable length attribute table in memory.
  std::vector<int64_t> vector_dims_;
  std::vector<std::unique_ptr<float[]>> vector_tables_;  // The vector attribute tables. Each vector attribute has its own vector table.
  std::unique_ptr<ConcurrentBitset> deleted_;            // The deleted bitset. If the i-th bit is 1, then the i-th record is deleted.

  bool isIntPK() const;
  bool isStringPK() const;
  meta::FieldType pkType() const;
  meta::FieldSchema pkField() const;
  int64_t pkFieldIdx() const;
  bool isEntryDeleted(int64_t id) const;

  // Geospatial indices
  std::unordered_map<std::string, std::shared_ptr<vectordb::engine::index::GeospatialIndex>> geospatial_indices_;

 private:
  friend class IncrementalCompactor;  // Allow IncrementalCompactor access for compaction
  vectordb::engine::Logger logger_;

  // Enhanced concurrency control
  mutable std::shared_mutex data_rw_mutex_;  // Reader-writer lock replacing single mutex
  std::unique_ptr<AtomicCapacityManager> capacity_manager_;  // Thread-safe capacity management
  std::unique_ptr<SnapshotManager> snapshot_manager_;  // Snapshot isolation for queries
  std::unique_ptr<AtomicUpsertManager> upsert_manager_;  // Atomic upsert operations
  std::unique_ptr<WALTransactionManager> wal_manager_;  // WAL transaction support

  // used to store primary key set for duplication check
  UniqueKey primary_key_;
  // The index of primary key in schema fields
  std::unique_ptr<int64_t> pk_field_idx_;
  // The index of primary key among string keys
  std::unique_ptr<int64_t> string_pk_offset_;

  // save a copy of the schema
  meta::TableSchema schema;
  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service_;

  Status DeleteByStringPK(const std::string& pk, vectordb::query::expr::ExprEvaluator& evaluator, int filter_root_index);

  template <typename T>
  Status DeleteByIntPK(T pk, vectordb::query::expr::ExprEvaluator& evaluator, int filter_root_index) {
    size_t result = 0;
    auto found = primary_key_.getKey(pk, result);
    if (found) {
      if (evaluator.LogicalEvaluate(filter_root_index, result)) {
        deleted_->set(result);
        primary_key_.removeKey(pk);
        logger_.Debug("[TableSegment] Marked record id=" + std::to_string(result) + 
                     " as deleted in bitset and removed INT pk=" + std::to_string(pk) + " from index");
        return Status::OK();
      } else {
        logger_.Debug("[TableSegment] Record with INT pk=" + std::to_string(pk) + 
                     " found at id=" + std::to_string(result) + " but skipped by filter");
      }
    } else {
      logger_.Debug("[TableSegment] Record with INT pk=" + std::to_string(pk) + " not found in primary key index");
    }
    return Status(RECORD_NOT_FOUND, "Record with primary key not exist or skipped by filter: " + std::to_string(pk));
  }

  template <typename T>
  Status HardDeleteByIntPK(T pk, vectordb::query::expr::ExprEvaluator& evaluator, int filter_root_index) {
    size_t result = 0;
    auto found = primary_key_.getKey(pk, result);
    if (found) {
      if (evaluator.LogicalEvaluate(filter_root_index, result)) {
        return SafeHardDeleteByID(result); // Use safe concurrent version
      } else {
        logger_.Debug("[TableSegment] Record with INT pk=" + std::to_string(pk) + 
                     " found at id=" + std::to_string(result) + " but skipped by filter");
      }
    } else {
      logger_.Debug("[TableSegment] Record with INT pk=" + std::to_string(pk) + " not found in primary key index");
    }
    return Status(RECORD_NOT_FOUND, "Record with primary key not exist or skipped by filter: " + std::to_string(pk));
  }

  Status DeleteByID(const size_t id, vectordb::query::expr::ExprEvaluator& evaluator, int filter_root_index);
};

// Backward compatibility typedef
using TableSegmentMVP = TableSegment;

}  // namespace engine
}  // namespace vectordb