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

  bool getKey(int8_t k, size_t &offset_result) {
    auto it = int8_map_.find(k);
    auto found = it != int8_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
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

  bool getKey(int16_t k, size_t &offset_result) {
    auto it = int16_map_.find(k);
    auto found = it != int16_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
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

  bool getKey(int32_t k, size_t &offset_result) {
    auto it = int32_map_.find(k);
    auto found = it != int32_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
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

  bool getKey(int64_t k, size_t &offset_result) {
    auto it = int64_map_.find(k);
    auto found = it != int64_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
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

  bool getKey(const std::string &k, size_t &offset_result) {
    auto it = string_map_.find(k);
    auto found = it != string_map_.end();
    if (found) {
      offset_result = it->second;
    }
    return found;
  }

  bool removeKey(const std::string &k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return string_map_.erase(k) == 1;
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