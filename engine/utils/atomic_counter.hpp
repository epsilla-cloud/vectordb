#pragma once
#include <atomic>

namespace vectordb {

class AtomicCounter {
 public:
  AtomicCounter();

  int Get();

  int IncrementAndGet();

  void SetValue(int value);

 private:
  std::atomic<int> counter_;
};

}  // namespace vectordb
