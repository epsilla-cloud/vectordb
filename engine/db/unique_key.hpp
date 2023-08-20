#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_set>

#include "db/catalog/meta_types.hpp"

namespace vectordb {
namespace engine {

class UniqueKey {
 public:
  UniqueKey() {}

  bool hasKey(int8_t k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return int8_set_.find(k) != int8_set_.end();
  }

  bool addKeyIfNotExist(int8_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int8_set_.insert(k).second;
  }

  bool removeKey(int8_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int8_set_.erase(k) == 1;
  }

  bool hasKey(int16_t k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return int16_set_.find(k) != int16_set_.end();
  }

  bool addKeyIfNotExist(int16_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int16_set_.insert(k).second;
  }

  bool removeKey(int16_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int16_set_.erase(k) == 1;
  }

  bool hasKey(int32_t k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return int32_set_.find(k) != int32_set_.end();
  }

  bool addKeyIfNotExist(int32_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int32_set_.insert(k).second;
  }

  bool removeKey(int32_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int32_set_.erase(k) == 1;
  }

  bool hasKey(int64_t k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return int64_set_.find(k) != int64_set_.end();
  }

  bool addKeyIfNotExist(int64_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int64_set_.insert(k).second;
  }

  bool removeKey(int64_t k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return int64_set_.erase(k) == 1;
  }

  bool hasKey(const std::string &k) {
    std::shared_lock<std::shared_timed_mutex> l(mutex_);
    return string_set_.find(k) != string_set_.end();
  }

  bool addKeyIfNotExist(const std::string &k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return string_set_.insert(k).second;
  }

  bool removeKey(const std::string &k) {
    std::unique_lock<std::shared_timed_mutex> l(mutex_);
    return string_set_.erase(k) == 1;
  }

 private:
  std::shared_timed_mutex mutex_;
  std::unordered_set<int8_t> int8_set_;
  std::unordered_set<int16_t> int16_set_;
  std::unordered_set<int32_t> int32_set_;
  std::unordered_set<int64_t> int64_set_;
  std::unordered_set<std::string> string_set_;
};

}  // namespace engine
}  // namespace vectordb