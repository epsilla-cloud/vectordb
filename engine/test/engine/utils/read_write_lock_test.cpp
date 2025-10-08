/**
 * @file read_write_lock_test.cpp
 * @brief Comprehensive tests for ReadWriteLock with writer-priority policy
 *
 * Tests cover:
 * - Basic read/write locking
 * - Multiple concurrent readers
 * - Reader-writer mutual exclusion
 * - Writer-priority policy (prevents writer starvation)
 * - Stress testing under high contention
 * - Deadlock prevention
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include "utils/read_write_lock.hpp"

namespace vectordb {

class ReadWriteLockTest : public ::testing::Test {
 protected:
  ReadWriteLock lock_;
  std::atomic<int> shared_counter_{0};
  std::atomic<bool> test_failed_{false};
};

// Test 1: Basic read lock acquire and release
TEST_F(ReadWriteLockTest, BasicReadLock) {
  EXPECT_EQ(lock_.GetReaderCount(), 0);

  lock_.LockRead();
  EXPECT_EQ(lock_.GetReaderCount(), 1);

  lock_.UnlockRead();
  EXPECT_EQ(lock_.GetReaderCount(), 0);
}

// Test 2: Basic write lock acquire and release
TEST_F(ReadWriteLockTest, BasicWriteLock) {
  EXPECT_FALSE(lock_.IsWriterActive());
  EXPECT_EQ(lock_.GetWaitingWriterCount(), 0);

  lock_.LockWrite();
  EXPECT_TRUE(lock_.IsWriterActive());

  lock_.UnlockWrite();
  EXPECT_FALSE(lock_.IsWriterActive());
}

// Test 3: Multiple concurrent readers allowed
TEST_F(ReadWriteLockTest, MultipleConcurrentReaders) {
  const int num_readers = 10;
  std::vector<std::thread> threads;
  std::atomic<int> concurrent_readers{0};
  std::atomic<int> max_concurrent{0};

  for (int i = 0; i < num_readers; ++i) {
    threads.emplace_back([&]() {
      lock_.LockRead();

      int current = concurrent_readers.fetch_add(1) + 1;

      // Update max
      int max = max_concurrent.load();
      while (current > max && !max_concurrent.compare_exchange_weak(max, current)) {}

      // Hold lock briefly
      std::this_thread::sleep_for(std::chrono::milliseconds(10));

      concurrent_readers.fetch_sub(1);
      lock_.UnlockRead();
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  // All readers should have been able to run concurrently
  EXPECT_GE(max_concurrent.load(), 2) << "Multiple readers should be concurrent";
  EXPECT_EQ(lock_.GetReaderCount(), 0) << "All readers should have released";
}

// Test 4: Reader-Writer mutual exclusion
TEST_F(ReadWriteLockTest, ReaderWriterMutualExclusion) {
  std::atomic<bool> writer_active{false};
  std::atomic<bool> reader_active{false};
  std::atomic<bool> violation{false};

  std::thread writer([&]() {
    lock_.LockWrite();
    writer_active = true;

    // Check no readers are active
    if (reader_active.load()) {
      violation = true;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    writer_active = false;
    lock_.UnlockWrite();
  });

  // Give writer a head start
  std::this_thread::sleep_for(std::chrono::milliseconds(5));

  std::thread reader([&]() {
    lock_.LockRead();
    reader_active = true;

    // Check writer is not active
    if (writer_active.load()) {
      violation = true;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    reader_active = false;
    lock_.UnlockRead();
  });

  writer.join();
  reader.join();

  EXPECT_FALSE(violation.load()) << "Reader and writer must not be active simultaneously";
}

// Test 5: Writer-Writer mutual exclusion
TEST_F(ReadWriteLockTest, WriterWriterMutualExclusion) {
  std::atomic<int> active_writers{0};
  std::atomic<int> max_writers{0};

  std::vector<std::thread> threads;
  for (int i = 0; i < 5; ++i) {
    threads.emplace_back([&]() {
      lock_.LockWrite();

      int current = active_writers.fetch_add(1) + 1;

      // Update max
      int max = max_writers.load();
      while (current > max && !max_writers.compare_exchange_weak(max, current)) {}

      std::this_thread::sleep_for(std::chrono::milliseconds(5));

      active_writers.fetch_sub(1);
      lock_.UnlockWrite();
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(max_writers.load(), 1) << "Only one writer should be active at a time";
}

// Test 6: Writer priority - writers don't starve
TEST_F(ReadWriteLockTest, WriterPriorityPolicy) {
  std::atomic<bool> stop_readers{false};
  std::atomic<bool> writer_completed{false};
  std::atomic<int> reader_acquisitions_after_writer_waiting{0};

  // Start continuous readers
  std::vector<std::thread> readers;
  for (int i = 0; i < 3; ++i) {
    readers.emplace_back([&]() {
      while (!stop_readers.load()) {
        lock_.LockRead();

        // If writer is waiting, new readers shouldn't acquire
        if (lock_.GetWaitingWriterCount() > 0 && !writer_completed.load()) {
          reader_acquisitions_after_writer_waiting.fetch_add(1);
        }

        std::this_thread::sleep_for(std::chrono::microseconds(100));
        lock_.UnlockRead();
        std::this_thread::yield();
      }
    });
  }

  // Let readers get started
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  // Writer arrives and should eventually succeed (not starved)
  auto start_time = std::chrono::steady_clock::now();

  std::thread writer([&]() {
    lock_.LockWrite();
    writer_completed = true;
    lock_.UnlockWrite();
  });

  writer.join();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - start_time);

  stop_readers = true;
  for (auto& r : readers) {
    r.join();
  }

  // Writer should complete within reasonable time (not starved)
  EXPECT_LT(elapsed.count(), 2000)  // 2 seconds in milliseconds
    << "Writer should not starve (took " << elapsed.count() << "ms)";

  // With writer priority, new readers should be blocked when writer is waiting
  // Some acquisitions may happen from already-waiting readers, but should be limited
  EXPECT_LT(reader_acquisitions_after_writer_waiting.load(), 10)
    << "New readers should be blocked when writer is waiting (writer-priority)";
}

// Test 7: Correct increment/decrement of reader count
TEST_F(ReadWriteLockTest, ReaderCountAccuracy) {
  const int num_ops = 100;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_ops; ++i) {
    threads.emplace_back([&]() {
      lock_.LockRead();
      EXPECT_GT(lock_.GetReaderCount(), 0);
      std::this_thread::sleep_for(std::chrono::microseconds(10));
      lock_.UnlockRead();
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(lock_.GetReaderCount(), 0) << "All readers should be released";
}

// Test 8: Stress test - high contention mixed read/write
TEST_F(ReadWriteLockTest, StressTestMixedOperations) {
  const int num_threads = 20;
  const int ops_per_thread = 100;
  std::atomic<int> read_count{0};
  std::atomic<int> write_count{0};

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&, thread_id = i]() {
      for (int op = 0; op < ops_per_thread; ++op) {
        if (thread_id % 3 == 0) {
          // Writer thread
          lock_.LockWrite();

          // Critical section - should be exclusive
          int expected = shared_counter_.load();
          shared_counter_.store(expected + 1);

          if (lock_.GetReaderCount() != 0) {
            test_failed_ = true;
          }

          write_count.fetch_add(1);
          lock_.UnlockWrite();
        } else {
          // Reader thread
          lock_.LockRead();

          // Read the counter (non-modifying)
          volatile int value = shared_counter_.load();
          (void)value;

          if (lock_.IsWriterActive()) {
            test_failed_ = true;
          }

          read_count.fetch_add(1);
          lock_.UnlockRead();
        }

        std::this_thread::yield();
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_FALSE(test_failed_.load()) << "No mutual exclusion violations should occur";
  EXPECT_EQ(lock_.GetReaderCount(), 0) << "All readers released";
  EXPECT_FALSE(lock_.IsWriterActive()) << "All writers released";
  EXPECT_GT(read_count.load(), 0);
  EXPECT_GT(write_count.load(), 0);
}

// Test 9: Sequential read operations
TEST_F(ReadWriteLockTest, SequentialReads) {
  for (int i = 0; i < 10; ++i) {
    lock_.LockRead();
    EXPECT_EQ(lock_.GetReaderCount(), 1);
    lock_.UnlockRead();
    EXPECT_EQ(lock_.GetReaderCount(), 0);
  }
}

// Test 10: Sequential write operations
TEST_F(ReadWriteLockTest, SequentialWrites) {
  for (int i = 0; i < 10; ++i) {
    lock_.LockWrite();
    EXPECT_TRUE(lock_.IsWriterActive());
    shared_counter_++;
    lock_.UnlockWrite();
    EXPECT_FALSE(lock_.IsWriterActive());
  }

  EXPECT_EQ(shared_counter_.load(), 10);
}

// Test 11: Nested read locks from same thread (should work - reentrant for reads)
TEST_F(ReadWriteLockTest, MultipleReadLocksFromSameThread) {
  lock_.LockRead();
  EXPECT_EQ(lock_.GetReaderCount(), 1);

  lock_.LockRead();
  EXPECT_EQ(lock_.GetReaderCount(), 2);

  lock_.UnlockRead();
  EXPECT_EQ(lock_.GetReaderCount(), 1);

  lock_.UnlockRead();
  EXPECT_EQ(lock_.GetReaderCount(), 0);
}

// Test 12: Verify waiting writer count updates correctly
TEST_F(ReadWriteLockTest, WaitingWriterCount) {
  // Acquire read lock to block writers
  lock_.LockRead();

  std::atomic<bool> writers_ready{false};
  std::vector<std::thread> writers;

  // Start multiple writers that will wait
  for (int i = 0; i < 3; ++i) {
    writers.emplace_back([&]() {
      writers_ready = true;
      lock_.LockWrite();
      lock_.UnlockWrite();
    });
  }

  // Wait for writers to start waiting
  while (!writers_ready.load()) {
    std::this_thread::yield();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  EXPECT_EQ(lock_.GetWaitingWriterCount(), 3);

  // Release reader to let writers proceed
  lock_.UnlockRead();

  for (auto& w : writers) {
    w.join();
  }

  EXPECT_EQ(lock_.GetWaitingWriterCount(), 0);
}

// Test 13: Fairness - writers eventually get access
TEST_F(ReadWriteLockTest, WriterEventuallySucceeds) {
  std::atomic<bool> writer_acquired{false};
  std::atomic<bool> stop_readers{false};

  // Start multiple readers
  std::vector<std::thread> readers;
  for (int i = 0; i < 5; ++i) {
    readers.emplace_back([&]() {
      while (!stop_readers.load()) {
        lock_.LockRead();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        lock_.UnlockRead();
      }
    });
  }

  // Let readers run
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  // Writer arrives
  std::thread writer([&]() {
    lock_.LockWrite();
    writer_acquired = true;
    lock_.UnlockWrite();
  });

  // Give writer time to acquire (should succeed with writer-priority)
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  EXPECT_TRUE(writer_acquired.load()) << "Writer should eventually acquire lock";

  stop_readers = true;
  writer.join();
  for (auto& r : readers) {
    r.join();
  }
}

// Test 14: Data integrity - counter increments are not lost
TEST_F(ReadWriteLockTest, DataIntegrity) {
  const int num_writers = 10;
  const int increments_per_writer = 100;

  std::vector<std::thread> writers;
  for (int i = 0; i < num_writers; ++i) {
    writers.emplace_back([&]() {
      for (int j = 0; j < increments_per_writer; ++j) {
        lock_.LockWrite();
        shared_counter_++;
        lock_.UnlockWrite();
      }
    });
  }

  for (auto& w : writers) {
    w.join();
  }

  EXPECT_EQ(shared_counter_.load(), num_writers * increments_per_writer)
    << "All increments should be preserved";
}

// Test 15: Readers can read consistent data while no writers
TEST_F(ReadWriteLockTest, ReadersReadConsistentData) {
  shared_counter_ = 42;
  std::atomic<bool> violation{false};

  std::vector<std::thread> readers;
  for (int i = 0; i < 10; ++i) {
    readers.emplace_back([&]() {
      lock_.LockRead();

      int value = shared_counter_.load();
      if (value != 42) {
        violation = true;
      }

      lock_.UnlockRead();
    });
  }

  for (auto& r : readers) {
    r.join();
  }

  EXPECT_FALSE(violation.load()) << "All readers should see consistent value";
}

}  // namespace vectordb
