#pragma once
#include <atomic>

namespace vectordb {

class AtomicCounter {
 public:
  AtomicCounter();

  int64_t Get();

  int64_t IncrementAndGet();

  int64_t GetAndIncrement();

  void SetValue(int64_t value);

 private:
  std::atomic<int64_t> counter_;
};

}  // namespace vectordb
