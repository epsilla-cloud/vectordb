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

class ANNGraphSegment {
 public:
  // Default constructor just for table level init.
  explicit ANNGraphSegment();
  // Load segment from disk.
  explicit ANNGraphSegment(std::string& db_catalog_path);
  // Create an in-memory segment.
  explicit ANNGraphSegment(size_t size_limit);

  ~ANNGraphSegment();

 private:
  bool from_disk_;                                                        // Whether the table segment is loaded from disk (or synced with disk during rebuild)
  size_t first_record_id_;                                                // The internal record id (node id) of the first record in the segment.
  std::atomic<size_t> record_number_;                                     // Currently how many records (nodes) in the segment.
  char* offset_table_;                                                    // The offset table for neighbor list for each node. Only for from_disk_ is true segment.
  char* neighbor_list_;                                                   // The neighbor list for each node consecutively stored. Only for from_disk_ is true segment.
  ConcurrentBitset updated_;                                              // The updated bitset. If the i-th bit is 1, then the i-th record's neighbor list is updated.
                                                                          // In this case, query should consult the hashmap instead of the neighbor list for getting neighbors.
  ConcurrentHashMap<size_t, std::vector<size_t>> updated_neighbor_list_;  // The updated neighbor lists. Only the nodes with updated bit set to 1
                                                                          // will have updated neighbor list.
};

}  // namespace engine
}  // namespace vectordb
