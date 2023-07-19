#include "utils/builder_suspend.hpp"

namespace vectordb {

std::atomic<bool> BuilderSuspend::suspend_flag_(false);
std::mutex BuilderSuspend::mutex_;
std::condition_variable BuilderSuspend::cv_;

void BuilderSuspend::suspend() {
  suspend_flag_ = true;
}

void BuilderSuspend::resume() {
  suspend_flag_ = false;
}

void BuilderSuspend::check_wait() {
  while (suspend_flag_) {
    std::unique_lock<std::mutex> lck(mutex_);
    cv_.wait_for(lck, std::chrono::seconds(5));
  }
}

}  // namespace vectordb