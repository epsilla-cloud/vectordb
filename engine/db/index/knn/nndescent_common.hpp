#pragma once

#include <algorithm>
#include <boost/assert.hpp>
#include <boost/foreach.hpp>
#include <boost/random.hpp>
#include <cmath>
#include <fstream>
#include <iostream>
#include <limits>
#include <vector>
#define USE_SPINLOCK 1
#ifdef USE_SPINLOCK
#include <mutex>

#include "boost/smart_ptr/detail/spinlock.hpp"
#endif
#ifdef _OPENMP
#include <omp.h>
#endif

namespace vectordb {
namespace engine {
namespace index {

using std::numeric_limits;
using std::vector;

#define SYMMETRIC 1

#ifdef _OPENMP
#if SYMMETRIC
#define NEED_LOCK 1
#endif
#endif

#if NEED_LOCK
#ifdef USE_SPINLOCK
class Mutex {
  boost::detail::spinlock lock;

 public:
  void init() {
  }
  void set() {
    lock.lock();
  }

  void unset() {
    lock.unlock();
  }
  friend class ScopedLock;
};

class ScopedLock : public std::lock_guard<boost::detail::spinlock> {
 public:
  ScopedLock(Mutex &mutex) : std::lock_guard<boost::detail::spinlock>(mutex.lock) {
  }
};
#else
class Mutex {
  omp_lock_t *lock;

 public:
  Mutex() : lock(0) {
  }

  void init() {
    lock = new omp_lock_t;
    omp_init_lock(lock);
  }

  ~Mutex() {
    if (lock) {
      omp_destroy_lock(lock);
      delete lock;
    }
  }

  void set() {
    omp_set_lock(lock);
  }

  void unset() {
    omp_unset_lock(lock);
  }

  friend class ScopedLock;
};
class ScopedLock {
  omp_lock_t *lock;

 public:
  ScopedLock(Mutex &mutex) {
    lock = mutex.lock;
    omp_set_lock(lock);
  }
  ~ScopedLock() {
    omp_unset_lock(lock);
  }
};
#endif
#else
class Mutex {
 public:
  void init(){};
  void set(){};
  void unset(){};
};
class ScopedLock {
 public:
  ScopedLock(Mutex &) {
  }
};
#endif

struct KNNEntry {
  static const int BAD = -1;  // numeric_limits<int>::max();
  int key;
  float dist;
  bool flag;
  bool match(const KNNEntry &e) const { return key == e.key; }
  KNNEntry(int key_, float dist_, bool flag_ = true) : key(key_), dist(dist_), flag(flag_) {}
  KNNEntry() : dist(numeric_limits<float>::max()) {}
  void reset() {
    key = BAD;
    dist = numeric_limits<float>::max();
  }
  friend bool operator<(const KNNEntry &e1, const KNNEntry &e2) {
    return e1.dist < e2.dist;
  }
};

class KNN : public vector<KNNEntry> {
  int K;
  Mutex mutex;

 public:
  typedef KNNEntry Element;
  typedef vector<KNNEntry> Base;

  void init(int k) {
    mutex.init();
    K = k;
    this->resize(k);
    BOOST_FOREACH (KNNEntry &e, *this) {
      e.reset();
    }
  }

  int update(Element t) {
    // ScopedLock ll(mutex);
    mutex.set();
    int i = this->size() - 1;
    int j;
    if (!(t < this->back())) {
      mutex.unset();
      return -1;
    }
    for (;;) {
      if (i == 0) break;
      j = i - 1;
      if (this->at(j).match(t)) {
        mutex.unset();
        return -1;
      }
      if (this->at(j) < t) break;
      i = j;
    }

    j = this->size() - 1;
    for (;;) {
      if (j == i) break;
      this->at(j) = this->at(j - 1);
      --j;
    }
    this->at(i) = t;
    mutex.unset();
    return i;
  }

  void update_unsafe(Element t) {
    int i = this->size() - 1;
    int j;
    if (!(t < this->back())) return;
    for (;;) {
      if (i == 0) break;
      j = i - 1;
      if (this->at(j).match(t)) {
        return;
      }
      if (this->at(j) < t) break;
      i = j;
    }

    j = this->size() - 1;
    for (;;) {
      if (j == i) break;
      this->at(j) = this->at(j - 1);
      --j;
    }
    this->at(i) = t;
  }

  void lock() {
    mutex.set();
  }

  void unlock() {
    mutex.unset();
  }
};

static inline float recall(const int *knn, const KNN &ans, int K) {
  int match = 0;
  for (int i = 0; i < K; ++i) {
    for (int j = 0; j < K; ++j) {
      if (knn[i] == ans[j].key) {
        ++match;
        break;
      }
    }
  }
  return float(match) / K;
}

class Random {
  boost::mt19937 rng;

 public:
  Random() {
  }
  void seed(int s) {
    rng.seed(s);
  }
  ptrdiff_t operator()(ptrdiff_t i) {
    return rng() % i;
  }
};
}  // namespace index
}  // namespace engine
}  // namespace vectordb
