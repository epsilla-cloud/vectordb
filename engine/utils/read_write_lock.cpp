#include "utils/read_write_lock.hpp"

namespace vectordb {

void ReadWriteLock::LockRead() {
  std::unique_lock<std::mutex> lock(mtx_);

  // CRITICAL FIX: Wait if there's an active writer OR waiting writers
  // This implements writer-priority policy to prevent writer starvation
  // New readers will be blocked when writers are waiting, even if no writer is active yet
  read_cv_.wait(lock, [this] {
    return !writer_active_ && waiting_writers_.load(std::memory_order_acquire) == 0;
  });

  readers_.fetch_add(1, std::memory_order_acquire);
}

void ReadWriteLock::UnlockRead() {
  // Atomically decrement readers count
  int prev_readers = readers_.fetch_sub(1, std::memory_order_release);

  // CRITICAL: Only notify writers if this was the last reader
  // This optimization avoids unnecessary mutex locks when there are still readers
  if (prev_readers == 1) {
    std::lock_guard<std::mutex> lock(mtx_);
    // Notify one waiting writer (if any) that all readers have finished
    write_cv_.notify_one();
  }
}

void ReadWriteLock::LockWrite() {
  std::unique_lock<std::mutex> lock(mtx_);

  // CRITICAL FIX: Increment waiting writers FIRST to prevent new readers
  // This is the key to writer-priority: new readers will see waiting_writers_ > 0 and block
  waiting_writers_.fetch_add(1, std::memory_order_acquire);

  // Wait for all readers to finish and no active writer
  // Active readers can still finish, but no NEW readers will start
  write_cv_.wait(lock, [this] {
    return readers_.load(std::memory_order_acquire) == 0 && !writer_active_;
  });

  // Now we have exclusive access
  writer_active_ = true;
  waiting_writers_.fetch_sub(1, std::memory_order_release);
}

void ReadWriteLock::UnlockWrite() {
  std::lock_guard<std::mutex> lock(mtx_);

  writer_active_ = false;

  // CRITICAL FIX: Writer-priority policy
  // If there are waiting writers, notify one writer (writer priority)
  // Otherwise, notify all waiting readers
  if (waiting_writers_.load(std::memory_order_acquire) > 0) {
    // Writer priority: give lock to next waiting writer
    write_cv_.notify_one();
  } else {
    // No writers waiting: allow all waiting readers to proceed
    read_cv_.notify_all();
  }
}

} // namespace vectordb