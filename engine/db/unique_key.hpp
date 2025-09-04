#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "db/catalog/meta_types.hpp"

namespace vectordb {
namespace engine {

class UniqueKey {
 public:
  UniqueKey() {}

  bool hasKey(int8_t k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return int8_map_.find(k) != int8_map_.end();
  }

  bool addKeyIfNotExist(int8_t k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int8_map_.emplace(std::pair<int8_t, size_t>(k, offset)).second;
  }

  void updateKey(int8_t k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    int8_map_[k] = offset;
  }

  bool getKey(int8_t k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto it = int8_map_.find(k);
    auto found = it != int8_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
  }

  bool getKeyWithLock(int8_t k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return getKey(k, offset_result);
  }

  bool removeKey(int8_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int8_map_.erase(k) == 1;
  }

  bool hasKey(int16_t k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return int16_map_.find(k) != int16_map_.end();
  }

  bool addKeyIfNotExist(int16_t k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int16_map_.emplace(std::pair<int16_t, size_t>(k, offset)).second;
  }

  void updateKey(int16_t k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    int16_map_[k] = offset;
  }

  bool getKey(int16_t k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto it = int16_map_.find(k);
    auto found = it != int16_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
  }

  bool getKeyWithLock(int16_t k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return getKey(k, offset_result);
  }

  bool removeKey(int16_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int16_map_.erase(k) == 1;
  }

  bool hasKey(int32_t k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return int32_map_.find(k) != int32_map_.end();
  }

  bool addKeyIfNotExist(int32_t k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int32_map_.insert(std::pair<int32_t, size_t>(k, offset)).second;
  }

  void updateKey(int32_t k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    int32_map_[k] = offset;
  }

  bool getKey(int32_t k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto it = int32_map_.find(k);
    auto found = it != int32_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
  }

  bool getKeyWithLock(int32_t k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return getKey(k, offset_result);
  }

  bool removeKey(int32_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int32_map_.erase(k) == 1;
  }

  bool hasKey(int64_t k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return int64_map_.find(k) != int64_map_.end();
  }

  bool addKeyIfNotExist(int64_t k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int64_map_.insert(std::pair<int64_t, size_t>(k, offset)).second;
  }

  void updateKey(int64_t k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    int64_map_[k] = offset;
  }

  bool getKey(int64_t k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto it = int64_map_.find(k);
    auto found = it != int64_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
  }

  bool getKeyWithLock(int64_t k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return getKey(k, offset_result);
  }

  bool removeKey(int64_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int64_map_.erase(k) == 1;
  }

  bool hasKey(const std::string &k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return string_map_.find(k) != string_map_.end();
  }

  bool addKeyIfNotExist(const std::string &k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return string_map_.emplace(std::pair<std::string, size_t>(k, offset)).second;
  }

  void updateKey(const std::string &k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    string_map_[k] = offset;
  }

  bool getKey(const std::string &k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    auto it = string_map_.find(k);
    auto found = it != string_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
  }

  bool getKeyWithLock(const std::string &k, size_t &offset_result) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return getKey(k, offset_result);
  }

  bool removeKey(const std::string &k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return string_map_.erase(k) == 1;
  }

  // Thread-safe putKey methods
  template <typename T>
  void putKey(T k, size_t offset) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    if constexpr (std::is_same_v<T, int8_t>) {
      int8_map_[k] = offset;
    } else if constexpr (std::is_same_v<T, int16_t>) {
      int16_map_[k] = offset;
    } else if constexpr (std::is_same_v<T, int32_t>) {
      int32_map_[k] = offset;
    } else if constexpr (std::is_same_v<T, int64_t>) {
      int64_map_[k] = offset;
    } else if constexpr (std::is_same_v<T, std::string>) {
      string_map_[k] = offset;
    }
  }

  void clear() {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    int8_map_.clear();
    int16_map_.clear();
    int32_map_.clear();
    int64_map_.clear();
    string_map_.clear();
  }
  
  void swap(UniqueKey& other) {
    std::unique_lock<std::shared_timed_mutex> l1(mutex_);
    std::unique_lock<std::shared_timed_mutex> l2(other.mutex_);
    int8_map_.swap(other.int8_map_);
    int16_map_.swap(other.int16_map_);
    int32_map_.swap(other.int32_map_);
    int64_map_.swap(other.int64_map_);
    string_map_.swap(other.string_map_);
  }

 private:
  std::shared_timed_mutex mutex_;
  std::unordered_map<int8_t, size_t> int8_map_;
  std::unordered_map<int16_t, size_t> int16_map_;
  std::unordered_map<int32_t, size_t> int32_map_;
  std::unordered_map<int64_t, size_t> int64_map_;
  std::unordered_map<std::string, size_t> string_map_;
};

}  // namespace engine
}  // namespace vectordb