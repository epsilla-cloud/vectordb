#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace vectordb {

/**
 * @brief ReadWriteLock with Writer-Priority Policy
 *
 * CRITICAL FIX: This implementation prevents writer starvation by giving priority to writers.
 *
 * Behavior:
 * - When a writer is waiting, new readers will be blocked (even if there are active readers)
 * - This ensures writers don't starve when there's continuous read traffic
 * - However, active readers can continue to completion before writer acquires the lock
 *
 * Lock ordering (prevents writer starvation):
 * 1. Writer arrives -> increments waiting_writers_
 * 2. New readers see waiting_writers_ > 0 -> block
 * 3. Active readers finish -> last reader notifies writer
 * 4. Writer acquires lock
 *
 * Trade-offs:
 * - Prevents writer starvation (good for write-heavy workloads)
 * - May cause reader starvation if writes are frequent
 * - For read-heavy workloads, consider using std::shared_mutex instead
 */
class ReadWriteLock {
 private:
  mutable std::mutex mtx_;
  std::condition_variable read_cv_;   // Notifies waiting readers
  std::condition_variable write_cv_;  // Notifies waiting writers

  // Use atomic for reader count to avoid race conditions
  std::atomic<int> readers_{0};
  std::atomic<int> waiting_writers_{0};
  bool writer_active_ = false;

 public:
  void LockRead();
  void UnlockRead();
  void LockWrite();
  void UnlockWrite();

  // Query methods for monitoring
  int GetReaderCount() const { return readers_.load(std::memory_order_acquire); }
  int GetWaitingWriterCount() const { return waiting_writers_.load(std::memory_order_acquire); }
  bool IsWriterActive() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return writer_active_;
  }
};
}  // namespace vectordb