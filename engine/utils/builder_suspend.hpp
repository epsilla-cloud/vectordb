#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace vectordb {

class BuilderSuspend {
 public:
  static void suspend();
  static void resume();
  static void check_wait();

 private:
  static std::atomic<bool> suspend_flag_;
  static std::mutex mutex_;
  static std::condition_variable cv_;
};

}  // namespace vectordb