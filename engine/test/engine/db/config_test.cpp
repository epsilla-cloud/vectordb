#include "config/config.hpp"

#include <gtest/gtest.h>
#include <cstdlib>
#include <thread>

using namespace vectordb;

class ConfigTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Clear any existing environment variable
    unsetenv("EPSILLA_INTRA_QUERY_THREADS");
  }

  void TearDown() override {
    // Clean up environment variable
    unsetenv("EPSILLA_INTRA_QUERY_THREADS");
  }
};

TEST_F(ConfigTest, DefaultConstructor) {
  Config config;
  
  // Check that default values are set reasonably
  EXPECT_GT(config.IntraQueryThreads.load(), 0);
  EXPECT_LE(config.IntraQueryThreads.load(), 128);
  
  EXPECT_GT(config.RebuildThreads.load(), 0);
  EXPECT_LE(config.RebuildThreads.load(), 4);
  
  // Check default atomic values
  EXPECT_EQ(config.MasterQueueSize.load(), 500);
  EXPECT_EQ(config.LocalQueueSize.load(), 500);
  EXPECT_EQ(config.GlobalSyncInterval.load(), 15);
  EXPECT_EQ(config.MinimalGraphSize.load(), 100);
  EXPECT_EQ(config.NumExecutorPerField.load(), 16);
  EXPECT_FALSE(config.PreFilter.load());
}

TEST_F(ConfigTest, ThreadCountAutoDetection) {
  Config config;
  
  unsigned int hw_threads = std::thread::hardware_concurrency();
  if (hw_threads == 0) {
    hw_threads = 4;  // Fallback value
  }
  
  // IntraQueryThreads should be set to hardware threads by default
  EXPECT_EQ(config.IntraQueryThreads.load(), static_cast<int>(hw_threads));
  
  // RebuildThreads should be min(hw_threads, 4)
  int expected_rebuild_threads = static_cast<int>(std::min(hw_threads, 4u));
  EXPECT_EQ(config.RebuildThreads.load(), expected_rebuild_threads);
}

TEST_F(ConfigTest, EnvironmentVariableOverride) {
  // Set environment variable
  setenv("EPSILLA_INTRA_QUERY_THREADS", "8", 1);
  
  Config config;
  
  EXPECT_EQ(config.IntraQueryThreads.load(), 8);
}

TEST_F(ConfigTest, EnvironmentVariableBoundaryValues) {
  // Test minimum valid value
  setenv("EPSILLA_INTRA_QUERY_THREADS", "1", 1);
  Config config1;
  EXPECT_EQ(config1.IntraQueryThreads.load(), 1);
  
  // Test maximum valid value
  setenv("EPSILLA_INTRA_QUERY_THREADS", "128", 1);
  Config config2;
  EXPECT_EQ(config2.IntraQueryThreads.load(), 128);
  
  // Test invalid values (should fall back to hardware concurrency)
  setenv("EPSILLA_INTRA_QUERY_THREADS", "0", 1);
  Config config3;
  unsigned int hw_threads = std::thread::hardware_concurrency();
  if (hw_threads == 0) hw_threads = 4;
  EXPECT_EQ(config3.IntraQueryThreads.load(), static_cast<int>(hw_threads));
  
  setenv("EPSILLA_INTRA_QUERY_THREADS", "129", 1);
  Config config4;
  EXPECT_EQ(config4.IntraQueryThreads.load(), static_cast<int>(hw_threads));
  
  // Test non-numeric value
  setenv("EPSILLA_INTRA_QUERY_THREADS", "invalid", 1);
  Config config5;
  EXPECT_EQ(config5.IntraQueryThreads.load(), static_cast<int>(hw_threads));
}

TEST_F(ConfigTest, SetIntraQueryThreads) {
  Config config;
  
  // Test valid values
  EXPECT_NO_THROW(config.setIntraQueryThreads(1));
  EXPECT_EQ(config.IntraQueryThreads.load(), 1);
  
  EXPECT_NO_THROW(config.setIntraQueryThreads(64));
  EXPECT_EQ(config.IntraQueryThreads.load(), 64);
  
  EXPECT_NO_THROW(config.setIntraQueryThreads(128));
  EXPECT_EQ(config.IntraQueryThreads.load(), 128);
  
  // Test invalid values
  EXPECT_THROW(config.setIntraQueryThreads(0), std::invalid_argument);
  EXPECT_THROW(config.setIntraQueryThreads(-1), std::invalid_argument);
  EXPECT_THROW(config.setIntraQueryThreads(129), std::invalid_argument);
}

TEST_F(ConfigTest, SetSearchQueueSize) {
  Config config;
  
  // Test valid values
  EXPECT_NO_THROW(config.setSearchQueueSize(500));
  EXPECT_EQ(config.MasterQueueSize.load(), 500);
  EXPECT_EQ(config.LocalQueueSize.load(), 500);
  
  EXPECT_NO_THROW(config.setSearchQueueSize(10000));
  EXPECT_EQ(config.MasterQueueSize.load(), 10000);
  EXPECT_EQ(config.LocalQueueSize.load(), 10000);
  
  EXPECT_NO_THROW(config.setSearchQueueSize(10000000));
  EXPECT_EQ(config.MasterQueueSize.load(), 10000000);
  EXPECT_EQ(config.LocalQueueSize.load(), 10000000);
  
  // Test invalid values
  EXPECT_THROW(config.setSearchQueueSize(499), std::invalid_argument);
  EXPECT_THROW(config.setSearchQueueSize(10000001), std::invalid_argument);
}

TEST_F(ConfigTest, SetNumExecutorPerField) {
  Config config;
  
  // Test valid values
  EXPECT_NO_THROW(config.setNumExecutorPerField(1));
  EXPECT_EQ(config.NumExecutorPerField.load(), 1);
  
  EXPECT_NO_THROW(config.setNumExecutorPerField(64));
  EXPECT_EQ(config.NumExecutorPerField.load(), 64);
  
  EXPECT_NO_THROW(config.setNumExecutorPerField(128));
  EXPECT_EQ(config.NumExecutorPerField.load(), 128);
  
  // Test invalid values
  EXPECT_THROW(config.setNumExecutorPerField(0), std::invalid_argument);
  EXPECT_THROW(config.setNumExecutorPerField(129), std::invalid_argument);
}

TEST_F(ConfigTest, AtomicOperations) {
  Config config;
  
  // Test that atomic operations are thread-safe
  std::vector<std::thread> threads;
  const int num_threads = 8;
  const int iterations = 100;
  
  // Test concurrent modifications
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&config, iterations]() {
      for (int j = 0; j < iterations; ++j) {
        config.setIntraQueryThreads((j % 64) + 1);
        config.setSearchQueueSize(1000 + (j % 1000));
        config.setNumExecutorPerField((j % 32) + 1);
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  // Check that final values are within valid ranges
  EXPECT_GE(config.IntraQueryThreads.load(), 1);
  EXPECT_LE(config.IntraQueryThreads.load(), 128);
  
  EXPECT_GE(config.MasterQueueSize.load(), 500);
  EXPECT_LE(config.MasterQueueSize.load(), 10000000);
  
  EXPECT_GE(config.NumExecutorPerField.load(), 1);
  EXPECT_LE(config.NumExecutorPerField.load(), 128);
}

TEST_F(ConfigTest, ConcurrentRead) {
  Config config;
  config.setIntraQueryThreads(32);
  config.setSearchQueueSize(5000);
  config.setNumExecutorPerField(8);
  
  std::vector<std::thread> threads;
  const int num_threads = 16;
  const int iterations = 1000;
  std::atomic<int> read_count{0};
  
  // Test concurrent reads
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&config, iterations, &read_count]() {
      for (int j = 0; j < iterations; ++j) {
        int query_threads = config.IntraQueryThreads.load();
        int queue_size = config.MasterQueueSize.load();
        int num_executors = config.NumExecutorPerField.load();
        
        // Verify expected values
        if (query_threads == 32 && queue_size == 5000 && num_executors == 8) {
          read_count++;
        }
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  // All reads should have succeeded
  EXPECT_EQ(read_count.load(), num_threads * iterations);
}

TEST_F(ConfigTest, MemoryOrdering) {
  Config config;
  
  // Test that memory ordering is correct for atomic operations
  std::thread writer([&config]() {
    for (int i = 1; i <= 10; ++i) {
      config.setIntraQueryThreads(i);
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  });
  
  std::thread reader([&config]() {
    int prev_value = 0;
    for (int i = 0; i < 100; ++i) {
      int current_value = config.IntraQueryThreads.load();
      // Value should only increase (monotonic)
      EXPECT_GE(current_value, prev_value);
      prev_value = current_value;
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  });
  
  writer.join();
  reader.join();
}

TEST_F(ConfigTest, MultipleInstances) {
  // Test that multiple config instances work independently
  Config config1, config2;
  
  config1.setIntraQueryThreads(16);
  config2.setIntraQueryThreads(32);
  
  EXPECT_EQ(config1.IntraQueryThreads.load(), 16);
  EXPECT_EQ(config2.IntraQueryThreads.load(), 32);
  
  config1.setSearchQueueSize(2000);
  config2.setSearchQueueSize(8000);
  
  EXPECT_EQ(config1.MasterQueueSize.load(), 2000);
  EXPECT_EQ(config2.MasterQueueSize.load(), 8000);
}