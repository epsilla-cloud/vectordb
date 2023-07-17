#pragma once

#include <atomic>
#include <string>
#include <unordered_map>

#include "db/catalog/meta.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

class TableSegment {
 public:
  // Default constructor just for table level init.
  explicit TableSegment();
  // Load segment from disk.
  explicit TableSegment(meta::TableSchema& table_schema, std::string& db_catalog_path);
  // Create an in-memory segment.
  explicit TableSegment(meta::TableSchema& table_schema, size_t size_limit);

  ~TableSegment();

 private:
  bool from_disk_;                                              // Whether the table segment is loaded from disk (or synced with disk during rebuild)
  size_t size_limit_;                                           // The maximum size of the segment. Default 2^20.
  std::atomic<size_t> record_number_;                           // Currently how many records in the segment.
  std::unordered_map<size_t, size_t> field_id_mem_offset_map_;  // The offset of each attribute in attribute table.
                                                                // Constructed from schema.
  char* attribute_table_;                                       // The attribute table in memory.
  char** string_tables_;                                        // The string attribute tables. Each string attribute has its own string table.
                                                                // (From left to right defined in schema)
  std::vector<std::string> growing_strings_;                    // Hold the string attributes of a growing segment.
  char** vector_tables_;                                        // The vector attribute tables. Each vector attribute has its own vector table.
                                                                // (From left to right defined in schema)
  ConcurrentBitset deleted_;                                    // The deleted bitset. If the i-th bit is 1, then the i-th record is deleted.
                                                                // The deleted records still occupy the position in all other structures.
                                                                // They should be skipped during search.
  // The primary key index. The key is the primary key value, and the value is
  // the record id. The primary key is the first field in the schema. Use the map depending on which data type
  // the primary key is.
  ConcurrentHashMap<int8_t, size_t> primary_key_int8_index_;
  ConcurrentHashMap<int16_t, size_t> primary_key_int16_index_;
  ConcurrentHashMap<int32_t, size_t> primary_key_int32_index_;
  ConcurrentHashMap<int64_t, size_t> primary_key_int64_index_;
  ConcurrentHashMap<std::string, size_t> primary_key_string_index_;
};

}  // namespace engine
}  // namespace vectordb
