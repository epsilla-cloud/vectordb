#pragma once

#include <atomic>
#include <string>
#include <unordered_map>

#include "db/catalog/meta.hpp"
#include "db/table_segment_mvp.hpp"
#include "db/ann_graph_segment.hpp"
#include "utils/atomic_counter.hpp"
#include "utils/concurrent_bitset.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

class TableMVP {
 public:
  explicit TableMVP(meta::TableSchema& table_schema);

  ~TableMVP();

  // Load table from disk.
  Status Load(std::string& db_catalog_path);

 private:
  AtomicCounter next_record_id_;                        // The next record's internal id.
  AtomicCounter commited_record_id;                     // The max record id that is ready to be used by queries.
  std::shared_ptr<TableSegmentMVP> table_segment_;          // The table segment loaded/synced from disk.
  std::shared_ptr<ANNGraphSegment> ann_graph_segment_;  // The ann graph segment loaded/synced from disk.
};

}  // namespace engine
}  // namespace vectordb
