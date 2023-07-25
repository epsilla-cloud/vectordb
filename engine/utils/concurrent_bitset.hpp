#pragma once

#include <atomic>
#include <memory>
#include <vector>

namespace vectordb {

class ConcurrentBitset {
 public:
  using id_type_t = int64_t;

  explicit ConcurrentBitset(id_type_t size);

  bool test(id_type_t id);
  void set(id_type_t id);
  void clear(id_type_t id);

  size_t capacity();
  size_t size();
  uint8_t* data();

 private:
  size_t capacity_;
  std::vector<std::atomic<uint8_t>> bitset_;
};

using ConcurrentBitsetPtr = std::shared_ptr<ConcurrentBitset>;

}  // namespace vectordb
