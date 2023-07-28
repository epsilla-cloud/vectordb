#include "utils/atomic_counter.hpp"

namespace vectordb {

AtomicCounter::AtomicCounter() : counter_(0) {}

int64_t AtomicCounter::IncrementAndGet() {
  return counter_.fetch_add(1) + 1;
  // return ++counter_;
}

int64_t AtomicCounter::GetAndIncrement() {
  return counter_.fetch_add(1);
}

void AtomicCounter::SetValue(int64_t value) {
  counter_.store(value);
}

int64_t AtomicCounter::Get() {
  return counter_.load();
}

}  // namespace vectordb
