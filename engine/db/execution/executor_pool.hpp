#include <mutex>
#include <queue>
#include <shared_mutex>

#include "db/execution/vec_search_executor.hpp"
namespace vectordb {
namespace engine {
namespace execution {

class ExecutorPool {
 public:
  std::shared_ptr<VecSearchExecutor> acquire() {
    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [&] { return resources_.size() > 0; });
    auto resource = resources_.front();
    resources_.pop();
    return resource;
  }

  void release(const std::shared_ptr<VecSearchExecutor> &resource) {
    std::unique_lock<std::mutex> lock(mutex_);
    resources_.push(resource);
    lock.unlock();
    condition_.notify_one();  // Notify waiting threads
  }

 private:
  std::queue<std::shared_ptr<VecSearchExecutor>> resources_;
  std::mutex mutex_;
  std::condition_variable condition_;
};

class RAIIVecSearchExecutor {
 public:
  RAIIVecSearchExecutor(
      std::shared_ptr<ExecutorPool> pool,
      std::shared_ptr<VecSearchExecutor> exec) : pool_(pool), exec_(exec) {
  }

  ~RAIIVecSearchExecutor() {
    pool_->release(exec_);
  }

  std::shared_ptr<VecSearchExecutor> exec_;
  std::shared_ptr<ExecutorPool> pool_;
};

}  // namespace execution
}  // namespace engine
}  // namespace vectordb