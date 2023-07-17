#pragma once

#include <optional>
#include <unordered_map>
#include <vector>

#include "utils/read_write_lock.hpp"

namespace vectordb {
template <typename KeyType, typename ValueType>
class ConcurrentHashMap {
 private:
  std::vector<std::unordered_map<KeyType, ValueType>> buckets_;
  std::vector<ReadWriteLock> bucket_locks_;

 public:
  ConcurrentHashMap(size_t num_buckets)
      : buckets_(num_buckets), bucket_locks_(num_buckets) {}

  void Insert(const KeyType& key, const ValueType& value) {
    size_t bucket_index = std::hash<KeyType>{}(key) % buckets_.size();
    bucket_locks_[bucket_index].LockWrite();
    buckets_[bucket_index][key] = value;
    bucket_locks_[bucket_index].UnlockWrite();
  }

  std::pair<bool, ValueType> Get(const KeyType& key) {
    size_t bucket_index = std::hash<KeyType>{}(key) % buckets_.size();
    bucket_locks_[bucket_index].LockRead();
    auto it = buckets_[bucket_index].find(key);
    if (it != buckets_[bucket_index].end()) {
      // TODO: copy or move?
      ValueType value = it->second;
      bucket_locks_[bucket_index].UnlockRead();
      return {true, value};
    } else {
      bucket_locks_[bucket_index].UnlockRead();
      return {false, ValueType{}};  // return default-constructed ValueType
    }
  }
};
}  // namespace vectordb