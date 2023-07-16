#pragma once

#include <condition_variable>
#include <mutex>

class ReadWriteLock {
 private:
  std::mutex mtx_;
  std::condition_variable cv_;
  int readers_ = 0;
  bool writer_ = false;

 public:
  void LockRead();
  void UnlockRead();
  void LockWrite();
  void UnlockWrite();
};
