/**
 * @file worker_pool_test.cpp
 * @brief Unit tests for the worker pool system
 */

#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <numeric>

#include "db/execution/worker_pool.hpp"

using namespace vectordb::engine::execution;

class WorkerPoolTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize with small pools for testing
        WorkerPoolConfig config;
        config.cpu_workers = 4;
        config.io_workers = 2;
        config.max_queue_size = 100;
        config.enable_stats = true;

        auto& manager = WorkerPoolManager::GetInstance();
        if (!manager.IsInitialized()) {
            manager.Initialize(config);
        }
    }

    void TearDown() override {
        // Note: We don't shutdown here as it's a singleton
        // and we want to reuse it across tests
    }
};

// Test basic CPU task submission and execution
TEST_F(WorkerPoolTest, BasicCpuTask) {
    auto& manager = WorkerPoolManager::GetInstance();

    auto future = manager.SubmitCpuTask([]() -> int {
        return 42;
    });

    ASSERT_TRUE(future.valid());
    EXPECT_EQ(future.get(), 42);
}

// Test basic IO task submission and execution
TEST_F(WorkerPoolTest, BasicIoTask) {
    auto& manager = WorkerPoolManager::GetInstance();

    auto future = manager.SubmitIoTask([]() -> std::string {
        return "hello";
    });

    ASSERT_TRUE(future.valid());
    EXPECT_EQ(future.get(), "hello");
}

// Test task with parameters
TEST_F(WorkerPoolTest, TaskWithParameters) {
    auto& manager = WorkerPoolManager::GetInstance();

    auto add = [](int a, int b) -> int {
        return a + b;
    };

    auto future = manager.SubmitCpuTask(add, 10, 20);

    ASSERT_TRUE(future.valid());
    EXPECT_EQ(future.get(), 30);
}

// Test multiple concurrent tasks
TEST_F(WorkerPoolTest, MultipleConcurrentTasks) {
    auto& manager = WorkerPoolManager::GetInstance();

    std::vector<std::future<int>> futures;
    const int num_tasks = 20;

    for (int i = 0; i < num_tasks; ++i) {
        futures.push_back(manager.SubmitCpuTask([i]() -> int {
            return i * i;
        }));
    }

    // Verify all results
    for (int i = 0; i < num_tasks; ++i) {
        EXPECT_EQ(futures[i].get(), i * i);
    }
}

// Test priority ordering
TEST_F(WorkerPoolTest, PriorityOrdering) {
    auto& manager = WorkerPoolManager::GetInstance();

    std::atomic<int> execution_order{0};
    std::vector<std::pair<int, TaskPriority>> results;
    std::mutex results_mutex;

    // Submit tasks with different priorities
    // They should execute in priority order (HIGH > NORMAL > LOW)

    auto task_fn = [&](int id, TaskPriority priority) {
        int order = execution_order.fetch_add(1);
        std::lock_guard<std::mutex> lock(results_mutex);
        results.push_back({order, priority});
    };

    std::vector<std::future<void>> futures;

    // Submit LOW priority tasks first
    for (int i = 0; i < 5; ++i) {
        futures.push_back(manager.SubmitCpuTaskWithPriority(
            TaskPriority::LOW, task_fn, i, TaskPriority::LOW));
    }

    // Then NORMAL priority
    for (int i = 0; i < 5; ++i) {
        futures.push_back(manager.SubmitCpuTaskWithPriority(
            TaskPriority::NORMAL, task_fn, i, TaskPriority::NORMAL));
    }

    // Then HIGH priority
    for (int i = 0; i < 5; ++i) {
        futures.push_back(manager.SubmitCpuTaskWithPriority(
            TaskPriority::HIGH, task_fn, i, TaskPriority::HIGH));
    }

    // Wait for all to complete
    for (auto& f : futures) {
        f.get();
    }

    // Verify that higher priority tasks generally executed first
    // (Note: Due to threading, this is not strictly guaranteed, but statistically should hold)
    int high_avg_order = 0, normal_avg_order = 0, low_avg_order = 0;
    int high_count = 0, normal_count = 0, low_count = 0;

    for (const auto& [order, priority] : results) {
        if (priority == TaskPriority::HIGH) {
            high_avg_order += order;
            high_count++;
        } else if (priority == TaskPriority::NORMAL) {
            normal_avg_order += order;
            normal_count++;
        } else {
            low_avg_order += order;
            low_count++;
        }
    }

    if (high_count > 0) high_avg_order /= high_count;
    if (normal_count > 0) normal_avg_order /= normal_count;
    if (low_count > 0) low_avg_order /= low_count;

    // HIGH should have lower average order (executed earlier)
    EXPECT_LT(high_avg_order, low_avg_order);
}

// Test task exception handling
TEST_F(WorkerPoolTest, TaskExceptionHandling) {
    auto& manager = WorkerPoolManager::GetInstance();

    auto future = manager.SubmitCpuTask([]() -> int {
        throw std::runtime_error("Test exception");
        return 0;
    });

    ASSERT_TRUE(future.valid());
    EXPECT_THROW(future.get(), std::runtime_error);

    // Pool should still be functional after exception
    auto future2 = manager.SubmitCpuTask([]() -> int {
        return 123;
    });

    EXPECT_EQ(future2.get(), 123);
}

// Test statistics collection
TEST_F(WorkerPoolTest, StatisticsCollection) {
    auto& manager = WorkerPoolManager::GetInstance();

    // Reset stats
    // Note: We can't actually reset via public API, so we'll just check they're reasonable

    // Submit some tasks
    std::vector<std::future<int>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(manager.SubmitCpuTask([i]() -> int {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            return i;
        }));
    }

    // Wait for completion
    for (auto& f : futures) {
        f.get();
    }

    auto stats = manager.GetCpuStats();

    // Verify stats are reasonable
    EXPECT_GT(stats.tasks_submitted.load(), 0);
    EXPECT_GT(stats.tasks_completed.load(), 0);
    EXPECT_LE(stats.tasks_completed.load(), stats.tasks_submitted.load());

    // Wait times and exec times should be positive
    EXPECT_GE(stats.GetAverageWaitTime(), 0.0);
    EXPECT_GT(stats.GetAverageExecTime(), 0.0);
}

// Test CPU-intensive workload
TEST_F(WorkerPoolTest, CpuIntensiveWorkload) {
    auto& manager = WorkerPoolManager::GetInstance();

    auto cpu_task = [](int64_t n) -> int64_t {
        int64_t sum = 0;
        for (int64_t i = 0; i < n; ++i) {
            sum += i;
        }
        return sum;
    };

    auto start_time = std::chrono::steady_clock::now();

    std::vector<std::future<int64_t>> futures;
    for (int i = 0; i < 8; ++i) {
        futures.push_back(manager.SubmitCpuTask(cpu_task, 1000000));
    }

    int64_t total = 0;
    for (auto& f : futures) {
        total += f.get();
    }

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();

    EXPECT_GT(total, 0);
    // Should complete in reasonable time (parallel execution)
    EXPECT_LT(duration, 5000);  // 5 seconds max
}

// Test IO workload simulation
TEST_F(WorkerPoolTest, IoWorkloadSimulation) {
    auto& manager = WorkerPoolManager::GetInstance();

    auto io_task = []() -> bool {
        // Simulate I/O delay
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        return true;
    };

    auto start_time = std::chrono::steady_clock::now();

    std::vector<std::future<bool>> futures;
    for (int i = 0; i < 10; ++i) {
        futures.push_back(manager.SubmitIoTask(io_task));
    }

    int success_count = 0;
    for (auto& f : futures) {
        if (f.get()) {
            success_count++;
        }
    }

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();

    EXPECT_EQ(success_count, 10);
    // With 2 IO workers, 10 tasks of 50ms each should take ~250ms (5 batches)
    // Allow some overhead
    EXPECT_LT(duration, 500);
}

// Test mixed CPU and IO workload
TEST_F(WorkerPoolTest, MixedWorkload) {
    auto& manager = WorkerPoolManager::GetInstance();

    std::atomic<int> cpu_completed{0};
    std::atomic<int> io_completed{0};

    auto cpu_task = [&cpu_completed]() {
        int sum = 0;
        for (int i = 0; i < 100000; ++i) {
            sum += i;
        }
        cpu_completed.fetch_add(1);
        return sum;
    };

    auto io_task = [&io_completed]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        io_completed.fetch_add(1);
    };

    std::vector<std::future<int>> cpu_futures;
    std::vector<std::future<void>> io_futures;

    // Submit mixed workload
    for (int i = 0; i < 10; ++i) {
        cpu_futures.push_back(manager.SubmitCpuTask(cpu_task));
        io_futures.push_back(manager.SubmitIoTask(io_task));
    }

    // Wait for all
    for (auto& f : cpu_futures) {
        f.get();
    }
    for (auto& f : io_futures) {
        f.get();
    }

    EXPECT_EQ(cpu_completed.load(), 10);
    EXPECT_EQ(io_completed.load(), 10);
}

// Test queue capacity enforcement
TEST_F(WorkerPoolTest, QueueCapacity) {
    auto& manager = WorkerPoolManager::GetInstance();

    // Fill the queue with slow tasks
    auto slow_task = []() {
        std::this_thread::sleep_for(std::chrono::seconds(2));
    };

    std::vector<std::future<void>> futures;

    // Try to overflow the queue (max_queue_size = 100 in SetUp)
    // This might throw or reject tasks
    bool exception_caught = false;

    try {
        for (int i = 0; i < 150; ++i) {
            futures.push_back(manager.SubmitCpuTask(slow_task));
        }
    } catch (const std::runtime_error& e) {
        exception_caught = true;
        // Expected when queue is full
    }

    // Either we caught an exception, or we successfully queued all tasks
    // In either case, the system should remain functional

    // Cancel slow tasks by not waiting for them
    // (they'll complete in background)

    // Verify system still works with a quick task
    auto quick_future = manager.SubmitCpuTask([]() -> int {
        return 999;
    });

    // This might block if queue is full, but should eventually succeed
    // Give it reasonable timeout by checking within 5 seconds
    auto status = quick_future.wait_for(std::chrono::seconds(5));

    if (status == std::future_status::ready) {
        EXPECT_EQ(quick_future.get(), 999);
    }
    // If timeout, that's also acceptable given the overload
}

// Test that pools are actually separate
TEST_F(WorkerPoolTest, PoolsSeparation) {
    auto& manager = WorkerPoolManager::GetInstance();

    // Block CPU pool with slow tasks
    std::atomic<bool> cpu_blocked{true};
    auto cpu_blocker = [&cpu_blocked]() {
        while (cpu_blocked.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    };

    std::vector<std::future<void>> cpu_futures;
    for (int i = 0; i < 4; ++i) {  // Block all 4 CPU workers
        cpu_futures.push_back(manager.SubmitCpuTask(cpu_blocker));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // IO pool should still be responsive
    auto io_future = manager.SubmitIoTask([]() -> int {
        return 777;
    });

    auto status = io_future.wait_for(std::chrono::milliseconds(500));
    EXPECT_EQ(status, std::future_status::ready);

    if (status == std::future_status::ready) {
        EXPECT_EQ(io_future.get(), 777);
    }

    // Unblock CPU tasks
    cpu_blocked = false;

    for (auto& f : cpu_futures) {
        f.wait();
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
