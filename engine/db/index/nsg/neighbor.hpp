#pragma once

#include <mutex>

namespace vectordb {
namespace engine {
namespace index {

using node_t = int64_t;

// TODO: search use simple neighbor
struct Neighbor {
  node_t id;  // offset of node in origin data
  float distance;
  bool has_explored;

  Neighbor() = default;

  explicit Neighbor(node_t id, float distance, bool f) : id{id}, distance{distance}, has_explored(f) {
  }

  explicit Neighbor(node_t id, float distance) : id{id}, distance{distance}, has_explored(false) {
  }

  inline bool
  operator<(const Neighbor& other) const {
    return distance < other.distance;
  }
};

typedef std::lock_guard<std::mutex> LockGuard;

}  // namespace index
}  // namespace engine
}  // namespace vectordb
