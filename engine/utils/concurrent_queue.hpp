#include <condition_variable>
#include <mutex>
#include <queue>

namespace vectordb {
template <typename T>
class ConcurrentQueue {
 private:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;

 public:
  ConcurrentQueue() = default;
  ConcurrentQueue(const ConcurrentQueue<T>&) = delete;
  ConcurrentQueue& operator=(const ConcurrentQueue<T>&) = delete;

  void Push(T value) {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push(value);
    cond_.notify_one();
  }

  bool Empty() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
  }

  bool TryPop(T& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (queue_.empty()) {
      return false;
    }
    value = queue_.front();
    queue_.pop();
    return true;
  }

  void WaitAndPop(T& value) {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this] { return !queue_.empty(); });
    value = queue_.front();
    queue_.pop();
  }
};
}  // namespace vectordb
