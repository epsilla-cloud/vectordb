#include "utils/atomic_counter.hpp"

namespace vectordb {

AtomicCounter::AtomicCounter() : counter_(0) {}

int AtomicCounter::IncrementAndGet() {
  return counter_.fetch_add(1) + 1;
  // return ++counter_;
}

int AtomicCounter::GetAndIncrement() {
  return counter_.fetch_add(1);
}

void AtomicCounter::SetValue(int value) {
  counter_.store(value);
}

int AtomicCounter::Get() {
  return counter_.load();
}

}  // namespace vectordb
