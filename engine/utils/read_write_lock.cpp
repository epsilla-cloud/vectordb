#include "utils/read_write_lock.hpp"

void ReadWriteLock::LockRead() {
  std::unique_lock<std::mutex> lock(mtx_);
  cv_.wait(lock, [this] { return !writer_; });
  ++readers_;
}

void ReadWriteLock::UnlockRead() {
  std::unique_lock<std::mutex> lock(mtx_);
  if (--readers_ == 0 && writer_) {
    cv_.notify_one();
  }
}

void ReadWriteLock::LockWrite() {
  std::unique_lock<std::mutex> lock(mtx_);
  writer_ = true;
  cv_.wait(lock, [this] { return readers_ == 0; });
}

void ReadWriteLock::UnlockWrite() {
  std::unique_lock<std::mutex> lock(mtx_);
  writer_ = false;
  cv_.notify_all();
}
