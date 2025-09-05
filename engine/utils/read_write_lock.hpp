#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace vectordb {
class ReadWriteLock {
 private:
  mutable std::mutex mtx_;
  std::condition_variable read_cv_;
  std::condition_variable write_cv_;
  
  // Use atomic for reader count to avoid race conditions
  std::atomic<int> readers_{0};
  std::atomic<int> waiting_writers_{0};
  bool writer_active_ = false;

 public:
  void LockRead();
  void UnlockRead();
  void LockWrite();
  void UnlockWrite();
};
}  // namespace vectordb