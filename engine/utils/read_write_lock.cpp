#include "utils/read_write_lock.hpp"

namespace vectordb {

void ReadWriteLock::LockRead() {
  std::unique_lock<std::mutex> lock(mtx_);
  
  // Wait if there's an active writer or writers waiting
  // This prevents reader starvation and writer starvation
  read_cv_.wait(lock, [this] { 
    return !writer_active_ && waiting_writers_ == 0; 
  });
  
  readers_.fetch_add(1, std::memory_order_acquire);
}

void ReadWriteLock::UnlockRead() {
  // Atomically decrement readers count
  int prev_readers = readers_.fetch_sub(1, std::memory_order_release);
  
  // Only notify writers if this was the last reader
  if (prev_readers == 1) {
    std::lock_guard<std::mutex> lock(mtx_);
    write_cv_.notify_one();
  }
}

void ReadWriteLock::LockWrite() {
  std::unique_lock<std::mutex> lock(mtx_);
  
  // Increment waiting writers to prevent new readers
  waiting_writers_.fetch_add(1, std::memory_order_acquire);
  
  // Wait for all readers to finish and no active writer
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
  
  // Notify all waiting readers if no writers waiting
  if (waiting_writers_.load(std::memory_order_acquire) == 0) {
    read_cv_.notify_all();
  } else {
    // Otherwise notify one waiting writer
    write_cv_.notify_one();
  }
}

} // namespace vectordb