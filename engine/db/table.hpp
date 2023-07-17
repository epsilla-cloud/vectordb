#pragma once

#include <atomic>
#include <string>
#include <unordered_map>

#include "db/catalog/meta.hpp"
#include "db/table_segment.hpp"
#include "utils/atomic_counter.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

class Table {
 public:
  explicit Table(meta::TableSchema& table_schema);

  ~Table();

  // Load table from disk.
  Status Load(std::string& db_catalog_path);

 private:
  AtomicCounter next_record_id_;                // The next record's internal id.
  AtomicCounter commited_record_id;             // The max record id that is ready to be used by queries.
  std::shared_ptr<TableSegment> disk_segment_;  // The table segment loaded/synced from disk.
  TableSegment in_memory_segments_[1024];       // The in-memory table segments. For now we support up to 1 billion vectors per table
                                                // In the future we can use horizontal partitioning to support more vectors.
                                                // But it will be beyond this service's scope.
};

}  // namespace engine
}  // namespace vectordb
