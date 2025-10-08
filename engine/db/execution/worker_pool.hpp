#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <cstring>
#include <fstream>
#include <cmath>
#include <cstdlib>

// Platform-specific headers for CPU affinity
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
#include <pthread.h>
#include <mach/thread_policy.h>
#include <mach/thread_act.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

#include "logger/logger.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {
namespace execution {

/**
 * @brief Configuration for worker pools
 */
struct WorkerPoolConfig {
    // Number of CPU-bound workers (0 = auto-detect)
    size_t cpu_workers = 0;

    // Number of I/O-bound workers (0 = auto-detect)
    size_t io_workers = 0;

    // Maximum queue size for pending tasks
    size_t max_queue_size = 10000;

    // Enable CPU affinity for CPU workers
    bool enable_cpu_affinity = false;

    // Enable statistics tracking
    bool enable_stats = true;

    // Thread name prefix
    std::string thread_name_prefix = "worker";

    WorkerPoolConfig() {
        // Auto-detect CPU limit (K8S cgroup aware)
        size_t cpu_limit = DetectCpuLimit();
        ApplyOptimalConfig(cpu_limit);
    }

    /**
     * @brief Auto-detect configuration based on CPU limit
     */
    static WorkerPoolConfig AutoDetect() {
        WorkerPoolConfig config;
        // Constructor already does auto-detection
        return config;
    }

    /**
     * @brief Load configuration from environment variables
     * Environment variables override auto-detection
     */
    static WorkerPoolConfig FromEnvironment() {
        WorkerPoolConfig config = AutoDetect();

        const char* cpu_workers_env = std::getenv("VECTORDB_CPU_WORKERS");
        if (cpu_workers_env) {
            config.cpu_workers = std::stoi(cpu_workers_env);
        }

        const char* io_workers_env = std::getenv("VECTORDB_IO_WORKERS");
        if (io_workers_env) {
            config.io_workers = std::stoi(io_workers_env);
        }

        const char* max_queue_env = std::getenv("VECTORDB_MAX_QUEUE_SIZE");
        if (max_queue_env) {
            config.max_queue_size = std::stoi(max_queue_env);
        }

        const char* cpu_affinity_env = std::getenv("VECTORDB_CPU_AFFINITY");
        if (cpu_affinity_env) {
            config.enable_cpu_affinity = (std::string(cpu_affinity_env) == "true" ||
                                         std::string(cpu_affinity_env) == "1");
        }

        return config;
    }

private:
    /**
     * @brief Detect CPU limit from cgroup (K8S aware)
     * Supports both cgroup v1 and v2
     */
    static size_t DetectCpuLimit() {
        size_t hardware_cores = std::thread::hardware_concurrency();
        if (hardware_cores == 0) hardware_cores = 4;

        #ifdef __linux__
        // Try cgroup v1 (K8S default)
        try {
            std::ifstream quota_file("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
            std::ifstream period_file("/sys/fs/cgroup/cpu/cpu.cfs_period_us");

            if (quota_file && period_file) {
                int64_t quota = -1, period = 0;
                quota_file >> quota;
                period_file >> period;

                if (quota > 0 && period > 0) {
                    double cores = static_cast<double>(quota) / period;
                    size_t cgroup_cores = static_cast<size_t>(std::ceil(cores));
                    if (cgroup_cores > 0 && cgroup_cores < hardware_cores) {
                        return cgroup_cores;
                    }
                }
            }
        } catch (...) {}

        // Try cgroup v2 (newer K8S)
        try {
            std::ifstream cgroup_v2("/sys/fs/cgroup/cpu.max");
            if (cgroup_v2) {
                std::string quota_str, period_str;
                cgroup_v2 >> quota_str >> period_str;

                if (quota_str != "max") {
                    int64_t quota = std::stoll(quota_str);
                    int64_t period = std::stoll(period_str);
                    if (quota > 0 && period > 0) {
                        double cores = static_cast<double>(quota) / period;
                        size_t cgroup_cores = static_cast<size_t>(std::ceil(cores));
                        if (cgroup_cores > 0 && cgroup_cores < hardware_cores) {
                            return cgroup_cores;
                        }
                    }
                }
            }
        } catch (...) {}
        #endif

        return hardware_cores;
    }

    /**
     * @brief Apply optimal worker configuration based on CPU count
     *
     * Strategy:
     * - 1 core:  cpu=1, io=2  (total 3 workers, IO needs concurrency for latency)
     * - 2 cores: cpu=2, io=2  (total 4 workers, balanced)
     * - 4 cores: cpu=3, io=3  (total 6 workers)
     * - 8+ cores: cpu=cores-2, io=4 (leave headroom for system)
     */
    void ApplyOptimalConfig(size_t cpu_limit) {
        if (cpu_limit <= 1) {
            // 1-core environment (K8S pods with cpu: "1000m")
            cpu_workers = 1;
            io_workers = 2;  // IO still needs concurrency for disk/network latency
            max_queue_size = 500;
            enable_cpu_affinity = false;  // No benefit on single core

        } else if (cpu_limit == 2) {
            // 2-core environment (cpu: "2000m")
            cpu_workers = 2;
            io_workers = 2;
            max_queue_size = 1000;
            enable_cpu_affinity = false;

        } else if (cpu_limit <= 4) {
            // 4-core environment (cpu: "4000m")
            cpu_workers = 3;
            io_workers = 3;
            max_queue_size = 2000;
            enable_cpu_affinity = true;

        } else {
            // 8+ core environment
            cpu_workers = cpu_limit - 2;  // Leave 2 cores for system
            io_workers = std::min(size_t(4), cpu_limit / 2);
            max_queue_size = 5000;
            enable_cpu_affinity = true;
        }

        enable_stats = true;
    }
};

/**
 * @brief Statistics snapshot for a worker pool
 * This is a copyable snapshot of the atomic statistics
 */
struct WorkerPoolStats {
    uint64_t tasks_submitted = 0;
    uint64_t tasks_completed = 0;
    uint64_t tasks_failed = 0;
    uint64_t tasks_rejected = 0;
    uint64_t total_wait_time_us = 0;
    uint64_t total_exec_time_us = 0;
    size_t current_queue_size = 0;
    size_t peak_queue_size = 0;
    size_t active_workers = 0;

    double GetAverageWaitTime() const {
        return tasks_completed > 0 ?
            static_cast<double>(total_wait_time_us) / tasks_completed : 0.0;
    }

    double GetAverageExecTime() const {
        return tasks_completed > 0 ?
            static_cast<double>(total_exec_time_us) / tasks_completed : 0.0;
    }

    double GetThroughput() const {
        // Tasks per second (approximate)
        return total_exec_time_us > 0 ?
            static_cast<double>(tasks_completed) * 1000000.0 / total_exec_time_us : 0.0;
    }
};

/**
 * @brief Task priority levels
 */
enum class TaskPriority {
    LOW = 0,
    NORMAL = 1,
    HIGH = 2,
    CRITICAL = 3
};

/**
 * @brief Base class for worker pool
 */
class WorkerPool {
public:
    using Task = std::function<void()>;

    WorkerPool(const std::string& pool_name, size_t num_workers, size_t max_queue_size, bool enable_stats)
        : pool_name_(pool_name),
          num_workers_(num_workers),
          max_queue_size_(max_queue_size),
          enable_stats_(enable_stats),
          shutdown_(false) {
        logger_.Info("Initializing worker pool '" + pool_name_ + "' with " +
                    std::to_string(num_workers) + " workers");
    }

    virtual ~WorkerPool() {
        Shutdown();
    }

    /**
     * @brief Submit a task with default priority
     */
    template<typename Func, typename... Args>
    auto Submit(Func&& func, Args&&... args)
        -> std::future<typename std::result_of<Func(Args...)>::type> {
        return SubmitWithPriority(TaskPriority::NORMAL,
                                 std::forward<Func>(func),
                                 std::forward<Args>(args)...);
    }

    /**
     * @brief Submit a task with specific priority
     */
    template<typename Func, typename... Args>
    auto SubmitWithPriority(TaskPriority priority, Func&& func, Args&&... args)
        -> std::future<typename std::result_of<Func(Args...)>::type> {

        using ReturnType = typename std::result_of<Func(Args...)>::type;

        auto submit_time = std::chrono::steady_clock::now();

        // Create packaged task
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<Func>(func), std::forward<Args>(args)...)
        );

        std::future<ReturnType> result = task->get_future();

        // Wrap task with timing and error handling
        auto wrapped_task = [this, task, submit_time]() {
            auto start_time = std::chrono::steady_clock::now();

            if (enable_stats_) {
                auto wait_time = std::chrono::duration_cast<std::chrono::microseconds>(
                    start_time - submit_time).count();
                stats_total_wait_time_us_.fetch_add(wait_time, std::memory_order_relaxed);
                stats_active_workers_.fetch_add(1, std::memory_order_relaxed);
            }

            try {
                (*task)();
                if (enable_stats_) {
                    stats_tasks_completed_.fetch_add(1, std::memory_order_relaxed);
                }
            } catch (const std::exception& e) {
                logger_.Error("Task execution failed in pool '" + pool_name_ + "': " + e.what());
                if (enable_stats_) {
                    stats_tasks_failed_.fetch_add(1, std::memory_order_relaxed);
                }
            } catch (...) {
                logger_.Error("Task execution failed with unknown error in pool '" + pool_name_ + "'");
                if (enable_stats_) {
                    stats_tasks_failed_.fetch_add(1, std::memory_order_relaxed);
                }
            }

            if (enable_stats_) {
                auto end_time = std::chrono::steady_clock::now();
                auto exec_time = std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time).count();
                stats_total_exec_time_us_.fetch_add(exec_time, std::memory_order_relaxed);
                stats_active_workers_.fetch_sub(1, std::memory_order_relaxed);
            }
        };

        // Add to queue
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            if (shutdown_) {
                throw std::runtime_error("Cannot submit task to shutdown pool '" + pool_name_ + "'");
            }

            // CRITICAL FIX: For CRITICAL priority tasks (e.g., WAL writes), wait for space instead of rejecting
            // This prevents data loss due to queue saturation
            if (priority == TaskPriority::CRITICAL) {
                // ENHANCEMENT (BUG-EXE-001): Spurious wakeup handling
                // The while loop correctly re-checks the condition after wait_for() returns,
                // which handles spurious wakeups properly. The loop continues waiting if:
                // 1. Queue is still full (tasks_.size() >= max_queue_size_)
                // 2. Pool is not shutting down (!shutdown_)
                // This is the correct pattern for condition variable usage.
                while (tasks_.size() >= max_queue_size_ && !shutdown_) {
                    logger_.Warning("CRITICAL task waiting for queue space in pool '" + pool_name_ +
                                  "' (queue size: " + std::to_string(tasks_.size()) + ")");
                    // Wait with timeout to periodically check shutdown flag
                    // Note: wait_for() may return due to timeout OR spurious wakeup OR actual notification
                    // The while loop condition handles all cases correctly
                    condition_.wait_for(lock, std::chrono::milliseconds(100));
                }

                // Check again if shutdown occurred during wait
                if (shutdown_) {
                    throw std::runtime_error("Cannot submit task to shutdown pool '" + pool_name_ + "'");
                }
            } else {
                // For non-critical tasks, reject if queue is full
                if (tasks_.size() >= max_queue_size_) {
                    if (enable_stats_) {
                        stats_tasks_rejected_.fetch_add(1, std::memory_order_relaxed);
                    }
                    throw std::runtime_error("Task queue full in pool '" + pool_name_ +
                                           "' (size: " + std::to_string(tasks_.size()) +
                                           ", max: " + std::to_string(max_queue_size_) + ")");
                }
            }

            tasks_.emplace(priority, wrapped_task);

            if (enable_stats_) {
                stats_tasks_submitted_.fetch_add(1, std::memory_order_relaxed);
                size_t queue_size = tasks_.size();
                stats_current_queue_size_.store(queue_size, std::memory_order_relaxed);

                // CRITICAL BUG FIX (BUG-EXE-002): Reload queue_size in CAS loop
                // Previously: Used stale queue_size value, missing actual peak if queue grew
                // Solution: Reload queue_size in the loop to capture latest value
                size_t peak = stats_peak_queue_size_.load(std::memory_order_relaxed);
                while (queue_size > peak &&
                       !stats_peak_queue_size_.compare_exchange_weak(peak, queue_size)) {
                    // CAS failed: peak was updated by another thread
                    // Reload current queue size to ensure we capture the true peak
                    queue_size = tasks_.size();
                }
            }
        }

        // Notify one worker
        condition_.notify_one();

        return result;
    }

    /**
     * @brief Get pool statistics snapshot
     */
    WorkerPoolStats GetStats() const {
        WorkerPoolStats snapshot;
        snapshot.tasks_submitted = stats_tasks_submitted_.load(std::memory_order_relaxed);
        snapshot.tasks_completed = stats_tasks_completed_.load(std::memory_order_relaxed);
        snapshot.tasks_failed = stats_tasks_failed_.load(std::memory_order_relaxed);
        snapshot.tasks_rejected = stats_tasks_rejected_.load(std::memory_order_relaxed);
        snapshot.total_wait_time_us = stats_total_wait_time_us_.load(std::memory_order_relaxed);
        snapshot.total_exec_time_us = stats_total_exec_time_us_.load(std::memory_order_relaxed);
        snapshot.current_queue_size = stats_current_queue_size_.load(std::memory_order_relaxed);
        snapshot.peak_queue_size = stats_peak_queue_size_.load(std::memory_order_relaxed);
        snapshot.active_workers = stats_active_workers_.load(std::memory_order_relaxed);
        return snapshot;
    }

    /**
     * @brief Reset statistics
     */
    void ResetStats() {
        stats_tasks_submitted_.store(0);
        stats_tasks_completed_.store(0);
        stats_tasks_failed_.store(0);
        stats_tasks_rejected_.store(0);
        stats_total_wait_time_us_.store(0);
        stats_total_exec_time_us_.store(0);
        stats_current_queue_size_.store(0);
        stats_peak_queue_size_.store(0);
        stats_active_workers_.store(0);
    }

    /**
     * @brief Get pool name
     */
    const std::string& GetName() const {
        return pool_name_;
    }

    /**
     * @brief Get number of workers
     */
    size_t GetNumWorkers() const {
        return num_workers_;
    }

    /**
     * @brief Shutdown the pool
     */
    void Shutdown() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (shutdown_) {
                return;
            }
            shutdown_ = true;
        }

        condition_.notify_all();

        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }

        workers_.clear();

        logger_.Info("Worker pool '" + pool_name_ + "' shutdown complete");
    }

protected:
    struct PrioritizedTask {
        TaskPriority priority;
        Task task;

        PrioritizedTask(TaskPriority p, Task t)
            : priority(p), task(std::move(t)) {}

        bool operator<(const PrioritizedTask& other) const {
            // Higher priority comes first (reverse order for priority_queue)
            return static_cast<int>(priority) < static_cast<int>(other.priority);
        }
    };

    void WorkerLoop(size_t worker_id) {
        logger_.Debug("Worker " + std::to_string(worker_id) +
                     " started in pool '" + pool_name_ + "'");

        while (true) {
            PrioritizedTask task(TaskPriority::NORMAL, nullptr);

            {
                std::unique_lock<std::mutex> lock(queue_mutex_);

                condition_.wait(lock, [this] {
                    return shutdown_ || !tasks_.empty();
                });

                if (shutdown_ && tasks_.empty()) {
                    break;
                }

                if (!tasks_.empty()) {
                    task = tasks_.top();
                    tasks_.pop();

                    if (enable_stats_) {
                        stats_current_queue_size_.store(tasks_.size(), std::memory_order_relaxed);
                    }
                }
            }

            if (task.task) {
                task.task();
            }
        }

        logger_.Debug("Worker " + std::to_string(worker_id) +
                     " stopped in pool '" + pool_name_ + "'");
    }

    std::string pool_name_;
    size_t num_workers_;
    size_t max_queue_size_;
    bool enable_stats_;

    std::vector<std::thread> workers_;
    std::priority_queue<PrioritizedTask> tasks_;

    mutable std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::atomic<bool> shutdown_;

    // Atomic statistics members
    mutable std::atomic<uint64_t> stats_tasks_submitted_{0};
    mutable std::atomic<uint64_t> stats_tasks_completed_{0};
    mutable std::atomic<uint64_t> stats_tasks_failed_{0};
    mutable std::atomic<uint64_t> stats_tasks_rejected_{0};
    mutable std::atomic<uint64_t> stats_total_wait_time_us_{0};
    mutable std::atomic<uint64_t> stats_total_exec_time_us_{0};
    mutable std::atomic<size_t> stats_current_queue_size_{0};
    mutable std::atomic<size_t> stats_peak_queue_size_{0};
    mutable std::atomic<size_t> stats_active_workers_{0};

    Logger logger_;
};

/**
 * @brief CPU worker pool for compute-intensive tasks
 *
 * Optimized for:
 * - Vector distance calculations
 * - Index building
 * - Data compression/decompression
 * - Query processing
 */
class CpuWorkerPool : public WorkerPool {
public:
    CpuWorkerPool(size_t num_workers, size_t max_queue_size, bool enable_stats = true,
                  bool enable_affinity = false)
        : WorkerPool("CPU", num_workers, max_queue_size, enable_stats),
          enable_affinity_(enable_affinity) {

        // Create worker threads
        for (size_t i = 0; i < num_workers_; ++i) {
            workers_.emplace_back([this, i] {
                if (enable_affinity_) {
                    SetCpuAffinity(i);
                }
                this->WorkerLoop(i);
            });
        }
    }

private:
    bool enable_affinity_;

    void SetCpuAffinity(size_t worker_id) {
#ifdef __linux__
        // Linux: use pthread_setaffinity_np
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);

        // Bind to specific CPU core
        // Use modulo to handle case where num_workers > num_cores
        size_t cpu_id = worker_id % std::thread::hardware_concurrency();
        CPU_SET(cpu_id, &cpuset);

        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            logger_.Warning("Failed to set CPU affinity for worker " +
                          std::to_string(worker_id) + ": " + std::strerror(rc));
        } else {
            logger_.Debug("Worker " + std::to_string(worker_id) +
                         " bound to CPU " + std::to_string(cpu_id));
        }
#elif defined(__APPLE__) || defined(__FreeBSD__)
        // macOS/FreeBSD: use thread_policy_set
        // Note: macOS doesn't support hard CPU binding, this is a soft hint
        thread_affinity_policy_data_t policy = { static_cast<integer_t>(worker_id) };
        thread_policy_set(pthread_mach_thread_np(pthread_self()),
                         THREAD_AFFINITY_POLICY,
                         (thread_policy_t)&policy,
                         THREAD_AFFINITY_POLICY_COUNT);
        logger_.Debug("Worker " + std::to_string(worker_id) +
                     " affinity hint set (macOS/BSD)");
#elif defined(_WIN32)
        // Windows: use SetThreadAffinityMask
        DWORD_PTR mask = 1ULL << (worker_id % std::thread::hardware_concurrency());
        DWORD_PTR result = SetThreadAffinityMask(GetCurrentThread(), mask);
        if (result == 0) {
            logger_.Warning("Failed to set CPU affinity for worker " +
                          std::to_string(worker_id));
        } else {
            logger_.Debug("Worker " + std::to_string(worker_id) +
                         " bound to CPU mask " + std::to_string(mask));
        }
#else
        // Unsupported platform
        logger_.Warning("CPU affinity not supported on this platform");
#endif
    }
};

/**
 * @brief I/O worker pool for I/O-intensive tasks
 *
 * Optimized for:
 * - WAL writes
 * - Segment persistence
 * - Compaction I/O
 * - Network operations
 * - Metadata updates
 */
class IoWorkerPool : public WorkerPool {
public:
    IoWorkerPool(size_t num_workers, size_t max_queue_size, bool enable_stats = true)
        : WorkerPool("IO", num_workers, max_queue_size, enable_stats) {

        // Create worker threads
        for (size_t i = 0; i < num_workers_; ++i) {
            workers_.emplace_back([this, i] {
                this->WorkerLoop(i);
            });
        }
    }
};

/**
 * @brief Unified worker pool manager
 *
 * Manages both CPU and I/O worker pools with intelligent task routing.
 */
class WorkerPoolManager {
public:
    static WorkerPoolManager& GetInstance() {
        static WorkerPoolManager instance;
        return instance;
    }

    void Initialize(const WorkerPoolConfig& config = WorkerPoolConfig()) {
        std::lock_guard<std::mutex> lock(init_mutex_);

        if (initialized_) {
            logger_.Warning("WorkerPoolManager already initialized");
            return;
        }

        config_ = config;

        cpu_pool_ = std::make_unique<CpuWorkerPool>(
            config_.cpu_workers,
            config_.max_queue_size,
            config_.enable_stats,
            config_.enable_cpu_affinity
        );

        io_pool_ = std::make_unique<IoWorkerPool>(
            config_.io_workers,
            config_.max_queue_size,
            config_.enable_stats
        );

        initialized_ = true;

        logger_.Info("WorkerPoolManager initialized with " +
                    std::to_string(config_.cpu_workers) + " CPU workers and " +
                    std::to_string(config_.io_workers) + " IO workers");
    }

    void Shutdown() {
        std::lock_guard<std::mutex> lock(init_mutex_);

        if (!initialized_) {
            return;
        }

        cpu_pool_->Shutdown();
        io_pool_->Shutdown();

        cpu_pool_.reset();
        io_pool_.reset();

        initialized_ = false;

        logger_.Info("WorkerPoolManager shutdown complete");
    }

    /**
     * @brief Submit CPU-bound task
     */
    template<typename Func, typename... Args>
    auto SubmitCpuTask(Func&& func, Args&&... args)
        -> std::future<typename std::result_of<Func(Args...)>::type> {
        EnsureInitialized();
        return cpu_pool_->Submit(std::forward<Func>(func), std::forward<Args>(args)...);
    }

    /**
     * @brief Submit CPU-bound task with priority
     */
    template<typename Func, typename... Args>
    auto SubmitCpuTaskWithPriority(TaskPriority priority, Func&& func, Args&&... args)
        -> std::future<typename std::result_of<Func(Args...)>::type> {
        EnsureInitialized();
        return cpu_pool_->SubmitWithPriority(priority, std::forward<Func>(func), std::forward<Args>(args)...);
    }

    /**
     * @brief Submit I/O-bound task
     */
    template<typename Func, typename... Args>
    auto SubmitIoTask(Func&& func, Args&&... args)
        -> std::future<typename std::result_of<Func(Args...)>::type> {
        EnsureInitialized();
        return io_pool_->Submit(std::forward<Func>(func), std::forward<Args>(args)...);
    }

    /**
     * @brief Submit I/O-bound task with priority
     */
    template<typename Func, typename... Args>
    auto SubmitIoTaskWithPriority(TaskPriority priority, Func&& func, Args&&... args)
        -> std::future<typename std::result_of<Func(Args...)>::type> {
        EnsureInitialized();
        return io_pool_->SubmitWithPriority(priority, std::forward<Func>(func), std::forward<Args>(args)...);
    }

    /**
     * @brief Get CPU pool statistics
     */
    WorkerPoolStats GetCpuStats() const {
        if (cpu_pool_) {
            return cpu_pool_->GetStats();
        }
        return WorkerPoolStats{};
    }

    /**
     * @brief Get I/O pool statistics
     */
    WorkerPoolStats GetIoStats() const {
        if (io_pool_) {
            return io_pool_->GetStats();
        }
        return WorkerPoolStats{};
    }

    /**
     * @brief Check if initialized
     */
    bool IsInitialized() const {
        return initialized_;
    }

private:
    WorkerPoolManager() : initialized_(false) {}
    ~WorkerPoolManager() {
        Shutdown();
    }

    WorkerPoolManager(const WorkerPoolManager&) = delete;
    WorkerPoolManager& operator=(const WorkerPoolManager&) = delete;

    void EnsureInitialized() {
        if (!initialized_) {
            throw std::runtime_error("WorkerPoolManager not initialized. Call Initialize() first.");
        }
    }

    WorkerPoolConfig config_;
    std::unique_ptr<CpuWorkerPool> cpu_pool_;
    std::unique_ptr<IoWorkerPool> io_pool_;

    std::atomic<bool> initialized_;
    std::mutex init_mutex_;

    Logger logger_;
};

} // namespace execution
} // namespace engine
} // namespace vectordb
