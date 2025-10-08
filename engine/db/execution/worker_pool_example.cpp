/**
 * @file worker_pool_example.cpp
 * @brief Example usage of the CPU/IO worker pool system
 *
 * This file demonstrates how to use the worker pool manager for separating
 * CPU-bound and I/O-bound tasks to improve performance.
 */

#include "db/execution/worker_pool.hpp"
#include <iostream>
#include <chrono>
#include <thread>
#include <vector>
#include <numeric>

using namespace vectordb::engine::execution;

// Example CPU-intensive task: Calculate sum of squares
int64_t CalculateSumOfSquares(int64_t start, int64_t end) {
    int64_t sum = 0;
    for (int64_t i = start; i < end; ++i) {
        sum += i * i;
    }
    return sum;
}

// Example I/O task simulation: Write to file
void SimulatedIOTask(const std::string& filename, const std::string& data) {
    // Simulate disk I/O delay
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // In real code, would do: std::ofstream(filename) << data;
}

int main() {
    std::cout << "=== Worker Pool Example ===" << std::endl;

    // Initialize worker pool manager
    // Priority: Environment variables > Default config
    WorkerPoolConfig config;

    if (std::getenv("VECTORDB_CPU_WORKERS") || std::getenv("VECTORDB_IO_WORKERS")) {
        // Use environment variables (K8S scenario)
        config = WorkerPoolConfig::FromEnvironment();
        std::cout << "Using configuration from environment variables" << std::endl;
    } else {
        // Use auto-detection (K8S cgroup aware)
        config = WorkerPoolConfig::AutoDetect();
        std::cout << "Using auto-detected configuration (K8S cgroup aware)" << std::endl;
    }

    auto& manager = WorkerPoolManager::GetInstance();
    manager.Initialize(config);

    std::cout << "Initialized with " << config.cpu_workers << " CPU workers and "
              << config.io_workers << " IO workers" << std::endl;

    // Example 1: Submit CPU-bound tasks
    std::cout << "\n=== Example 1: CPU-bound tasks ===" << std::endl;
    auto start_time = std::chrono::steady_clock::now();

    std::vector<std::future<int64_t>> cpu_futures;
    for (int i = 0; i < 10; ++i) {
        cpu_futures.push_back(
            manager.SubmitCpuTask(CalculateSumOfSquares, i * 1000000, (i + 1) * 1000000)
        );
    }

    // Wait for results
    int64_t total_sum = 0;
    for (auto& future : cpu_futures) {
        total_sum += future.get();
    }

    auto cpu_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();

    std::cout << "Completed 10 CPU-bound tasks in " << cpu_duration << "ms" << std::endl;
    std::cout << "Total sum: " << total_sum << std::endl;

    // Example 2: Submit I/O-bound tasks
    std::cout << "\n=== Example 2: I/O-bound tasks ===" << std::endl;
    start_time = std::chrono::steady_clock::now();

    std::vector<std::future<void>> io_futures;
    for (int i = 0; i < 20; ++i) {
        io_futures.push_back(
            manager.SubmitIoTask(SimulatedIOTask, "file_" + std::to_string(i) + ".txt", "data")
        );
    }

    // Wait for completion
    for (auto& future : io_futures) {
        future.get();
    }

    auto io_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_time).count();

    std::cout << "Completed 20 I/O-bound tasks in " << io_duration << "ms" << std::endl;

    // Example 3: Mixed workload with priorities
    std::cout << "\n=== Example 3: Mixed workload with priorities ===" << std::endl;

    // Submit high-priority CPU task
    auto high_priority_future = manager.SubmitCpuTaskWithPriority(
        TaskPriority::HIGH,
        CalculateSumOfSquares, 0, 10000000
    );

    // Submit normal priority I/O tasks
    std::vector<std::future<void>> mixed_futures;
    for (int i = 0; i < 5; ++i) {
        mixed_futures.push_back(
            manager.SubmitIoTaskWithPriority(
                TaskPriority::NORMAL,
                SimulatedIOTask, "mixed_" + std::to_string(i) + ".txt", "data"
            )
        );
    }

    // Submit low-priority CPU task
    auto low_priority_future = manager.SubmitCpuTaskWithPriority(
        TaskPriority::LOW,
        CalculateSumOfSquares, 0, 1000000
    );

    // Wait for all
    high_priority_future.get();
    low_priority_future.get();
    for (auto& f : mixed_futures) {
        f.get();
    }

    std::cout << "Completed mixed workload with priorities" << std::endl;

    // Display statistics
    std::cout << "\n=== Worker Pool Statistics ===" << std::endl;

    auto cpu_stats = manager.GetCpuStats();
    std::cout << "\nCPU Pool:" << std::endl;
    std::cout << "  Tasks submitted:  " << cpu_stats.tasks_submitted << std::endl;
    std::cout << "  Tasks completed:  " << cpu_stats.tasks_completed << std::endl;
    std::cout << "  Tasks failed:     " << cpu_stats.tasks_failed << std::endl;
    std::cout << "  Avg wait time:    " << cpu_stats.GetAverageWaitTime() << " us" << std::endl;
    std::cout << "  Avg exec time:    " << cpu_stats.GetAverageExecTime() << " us" << std::endl;
    std::cout << "  Peak queue size:  " << cpu_stats.peak_queue_size << std::endl;

    auto io_stats = manager.GetIoStats();
    std::cout << "\nIO Pool:" << std::endl;
    std::cout << "  Tasks submitted:  " << io_stats.tasks_submitted << std::endl;
    std::cout << "  Tasks completed:  " << io_stats.tasks_completed << std::endl;
    std::cout << "  Tasks failed:     " << io_stats.tasks_failed << std::endl;
    std::cout << "  Avg wait time:    " << io_stats.GetAverageWaitTime() << " us" << std::endl;
    std::cout << "  Avg exec time:    " << io_stats.GetAverageExecTime() << " us" << std::endl;
    std::cout << "  Peak queue size:  " << io_stats.peak_queue_size << std::endl;

    // Shutdown
    manager.Shutdown();
    std::cout << "\nWorker pool manager shut down successfully" << std::endl;

    return 0;
}
