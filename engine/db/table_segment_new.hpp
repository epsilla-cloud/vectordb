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

namespace vectordb {
namespace engine {

/**
 * @brief Attribute table structure for storing non-vector data
 */
struct AttributeTable {
 public:
  std::unique_ptr<char[]> data;
  int64_t length;
  
  explicit AttributeTable(int64_t len) : length(len), data(std::make_unique<char[]>(len)) {}
  
  // Copy constructor - deep copy
  AttributeTable(const AttributeTable& other) : length(other.length), 
    data(std::make_unique<char[]>(other.length)) {
    std::memcpy(data.get(), other.data.get(), length);
  }
  
  // Move constructor
  AttributeTable(AttributeTable&& other) noexcept 
    : data(std::move(other.data)), length(other.length) {
    other.length = 0;
  }
  
  // Assignment operators
  AttributeTable& operator=(const AttributeTable& other);
  AttributeTable& operator=(AttributeTable&& other) noexcept;
  
  ~AttributeTable() = default;
};

/**
 * @brief TableSegment class - low-level storage segment for table data
 * 
 * This class provides low-level storage management with in-memory data storage 
 * and persistence capabilities. It handles:
 * - Primary key enforcement
 * - Concurrent deletion tracking via bitsets  
 * - Column-oriented storage for vectors
 * - Row-oriented storage for attributes
 * 
 * Key features:
 * - Thread-safe operations with concurrent access support
 * - Efficient memory management with RAII patterns
 * - SIMD-optimized vector operations
 * - Configurable capacity and growth strategies
 * 
 * Renamed from TableSegmentMVP to follow standard naming conventions.
 */
class TableSegment {
 public:
  explicit TableSegment(meta::TableSchema& table_schema,
                       const std::string& db_catalog_path,
                       int64_t segment_id,
                       int64_t capacity,
                       bool is_leader,
                       std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service,
                       std::unordered_map<std::string, std::string>& headers);

  ~TableSegment();

  // Core operations
  Status Insert(vectordb::Json& records, 
               std::unordered_map<std::string, std::string>& headers,
               bool upsert = false);

  Status Delete(vectordb::Json& primary_keys,
               std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes);

  Status Get(const std::vector<std::string>& field_names,
            const std::vector<int64_t>& ids,
            vectordb::Json& result);

  Status Search(const std::string& field_name,
               const std::vector<std::string>& query_fields, 
               const VectorPtr query_vector,
               int64_t limit,
               vectordb::Json& result,
               std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes,
               bool with_distance);

  // Persistence operations  
  Status Load(const std::string& segment_path);
  Status Dump(const std::string& segment_path);
  Status Backup(const std::string& backup_path);
  Status Restore(const std::string& backup_path);

  // Index operations
  Status BuildIndex(const std::string& field_name);
  Status RebuildIndex(const std::string& field_name);

  // Compaction and maintenance
  Status Compact();
  bool NeedsCompaction(double threshold = 0.3) const;
  size_t GetDeletedRecordCount() const;

  // Configuration
  void SetWALEnabled(bool enabled) { wal_enabled_ = enabled; }
  void SetIsLeader(bool is_leader) { is_leader_ = is_leader; }

  // Statistics and monitoring
  size_t GetRecordCount() const;
  size_t GetCapacity() const { return capacity_; }
  double GetUtilization() const;
  Status GetStatistics(vectordb::Json& stats);

  // Metadata access
  int64_t GetSegmentId() const { return segment_id_; }
  const meta::TableSchema& GetSchema() const { return table_schema_; }
  bool IsFull() const;

 private:
  meta::TableSchema table_schema_;
  std::string db_catalog_path_;
  int64_t segment_id_;
  int64_t capacity_;
  bool is_leader_;
  bool wal_enabled_;
  
  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service_;
  std::unordered_map<std::string, std::string> headers_;

  // Data storage
  std::unordered_map<std::string, VectorPtr> vector_fields_;
  std::unordered_map<std::string, std::shared_ptr<AttributeTable>> attribute_fields_;
  
  // Concurrent access control
  vectordb::ConcurrentBitset deleted_docs_;
  vectordb::ConcurrentHashMap<std::string, int64_t> primary_key_index_;
  
  // Thread safety
  mutable std::shared_mutex segment_mutex_;
  
  // Statistics
  std::atomic<size_t> record_count_{0};
  std::atomic<size_t> deleted_count_{0};
  
  vectordb::engine::Logger logger_;

  // Helper methods
  Status ValidateRecord(const vectordb::Json& record);
  Status AllocateSpace(size_t required_space);
  int64_t AssignRecordId();
  Status UpdatePrimaryKeyIndex(const std::string& primary_key, int64_t record_id);
  Status HandleVectorField(const std::string& field_name, const vectordb::Json& value, int64_t record_id);
  Status HandleAttributeField(const std::string& field_name, const vectordb::Json& value, int64_t record_id);
};

}  // namespace engine
}  // namespace vectordb