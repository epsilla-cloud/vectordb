#pragma once
#include <atomic>

namespace vectordb {

class AtomicCounter {
 public:
  AtomicCounter();

  int Get();

  int IncrementAndGet();

  int GetAndIncrement();

  void SetValue(int value);

 private:
  std::atomic<int> counter_;
};

}  // namespace vectordb
