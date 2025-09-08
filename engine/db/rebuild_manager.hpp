#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_set>
#include "logger/logger.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

// Forward declarations
class Database;
class Table;

/**
 * Advanced rebuild manager with incremental and priority-based rebuilding
 */
class RebuildManager {
public:
    enum class RebuildPriority {
        LOW = 0,
        NORMAL = 1,
        HIGH = 2,
        CRITICAL = 3
    };

    struct RebuildTask {
        std::string db_name;
        std::string table_name;
        RebuildPriority priority;
        std::chrono::steady_clock::time_point scheduled_time;
        bool force_rebuild;
        
        // For priority queue ordering (higher priority first)
        bool operator<(const RebuildTask& other) const {
            if (priority != other.priority) {
                return priority < other.priority;
            }
            return scheduled_time > other.scheduled_time;
        }
    };

    struct RebuildConfig {
        // Rebuild intervals based on priority
        std::chrono::milliseconds low_priority_interval{300000};      // 5 minutes
        std::chrono::milliseconds normal_priority_interval{60000};    // 1 minute
        std::chrono::milliseconds high_priority_interval{30000};      // 30 seconds
        std::chrono::milliseconds critical_priority_interval{5000};   // 5 seconds
        
        // Resource limits
        size_t max_concurrent_rebuilds = 2;
        size_t max_memory_per_rebuild_mb = 1024;
        double max_cpu_usage_percent = 50.0;
        
        // Adaptive settings
        bool enable_adaptive_scheduling = true;
        bool enable_incremental_rebuild = true;
        bool enable_memory_monitoring = true;
        
        // Thresholds for triggering rebuild
        double rebuild_threshold_modification_ratio = 0.1;  // 10% modifications
        size_t rebuild_threshold_record_count = 10000;
        std::chrono::milliseconds min_rebuild_interval{10000};  // 10 seconds minimum
    };

    class RebuildStats {
    public:
        std::atomic<size_t> total_rebuilds{0};
        std::atomic<size_t> successful_rebuilds{0};
        std::atomic<size_t> failed_rebuilds{0};
        std::atomic<size_t> skipped_rebuilds{0};
        std::atomic<int64_t> total_rebuild_time_ms{0};
        std::atomic<int64_t> total_records_rebuilt{0};
        std::atomic<size_t> current_active_rebuilds{0};
        
        double GetAverageRebuildTime() const {
            size_t total = total_rebuilds.load();
            if (total == 0) return 0.0;
            return static_cast<double>(total_rebuild_time_ms.load()) / total;
        }
        
        double GetSuccessRate() const {
            size_t total = total_rebuilds.load();
            if (total == 0) return 100.0;
            return static_cast<double>(successful_rebuilds.load()) * 100.0 / total;
        }
    };

public:
    explicit RebuildManager(const RebuildConfig& config = RebuildConfig())
        : config_(config), running_(false), paused_(false) {}
    
    ~RebuildManager() {
        Stop();
    }
    
    // Start the rebuild manager
    void Start() {
        if (running_.exchange(true)) {
            return;  // Already running
        }
        
        worker_thread_ = std::thread([this]() {
            WorkerLoop();
        });
        
        if (config_.enable_adaptive_scheduling) {
            monitor_thread_ = std::thread([this]() {
                MonitorLoop();
            });
        }
        
        logger_.Info("RebuildManager started with " + 
                    std::to_string(config_.max_concurrent_rebuilds) + " max concurrent rebuilds");
    }
    
    // Stop the rebuild manager
    void Stop() {
        if (!running_.exchange(false)) {
            return;  // Not running
        }
        
        cv_.notify_all();
        monitor_cv_.notify_all();
        
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        
        if (monitor_thread_.joinable()) {
            monitor_thread_.join();
        }
        
        logger_.Info("RebuildManager stopped");
    }
    
    // Schedule a rebuild task
    void ScheduleRebuild(const std::string& db_name,
                         const std::string& table_name = "",
                         RebuildPriority priority = RebuildPriority::NORMAL,
                         bool force = false) {
        RebuildTask task;
        task.db_name = db_name;
        task.table_name = table_name;
        task.priority = priority;
        task.scheduled_time = std::chrono::steady_clock::now();
        task.force_rebuild = force;
        
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            
            // Check if task already exists (deduplication)
            std::string task_id = db_name + ":" + table_name;
            if (!force && scheduled_tasks_.count(task_id) > 0) {
                logger_.Debug("Rebuild task already scheduled for " + task_id);
                return;
            }
            
            task_queue_.push(task);
            scheduled_tasks_.insert(task_id);
        }
        
        cv_.notify_one();
        logger_.Debug("Scheduled rebuild for " + db_name + 
                     (table_name.empty() ? "" : ":" + table_name) +
                     " with priority " + std::to_string(static_cast<int>(priority)));
    }
    
    // Pause/resume rebuilding
    void Pause() {
        paused_ = true;
        logger_.Info("RebuildManager paused");
    }
    
    void Resume() {
        paused_ = false;
        cv_.notify_all();
        logger_.Info("RebuildManager resumed");
    }
    
    // Get statistics
    RebuildStats GetStats() const {
        return stats_;
    }
    
    // Update configuration
    void UpdateConfig(const RebuildConfig& config) {
        config_ = config;
        logger_.Info("RebuildManager configuration updated");
    }
    
    // Check if a table needs rebuild
    bool NeedsRebuild(const std::shared_ptr<Table>& table,
                      int64_t current_records,
                      int64_t graph_records,
                      int64_t modifications) {
        if (current_records < config_.rebuild_threshold_record_count) {
            return false;  // Too small to rebuild
        }
        
        if (graph_records == 0) {
            return true;  // No graph exists
        }
        
        double modification_ratio = static_cast<double>(current_records - graph_records) / graph_records;
        return modification_ratio >= config_.rebuild_threshold_modification_ratio;
    }

private:
    void WorkerLoop() {
        logger_.Info("RebuildManager worker thread started");
        
        while (running_) {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            
            // Wait for tasks or timeout for periodic checks
            cv_.wait_for(lock, std::chrono::seconds(1), [this] {
                return !running_ || (!paused_ && !task_queue_.empty());
            });
            
            if (!running_) {
                break;
            }
            
            if (paused_ || task_queue_.empty()) {
                continue;
            }
            
            // Check if we can start more rebuilds
            if (stats_.current_active_rebuilds >= config_.max_concurrent_rebuilds) {
                continue;
            }
            
            // Get next task
            RebuildTask task = task_queue_.top();
            task_queue_.pop();
            
            std::string task_id = task.db_name + ":" + task.table_name;
            scheduled_tasks_.erase(task_id);
            
            lock.unlock();
            
            // Execute rebuild asynchronously
            std::thread([this, task]() {
                ExecuteRebuild(task);
            }).detach();
        }
        
        logger_.Info("RebuildManager worker thread stopped");
    }
    
    void MonitorLoop() {
        logger_.Info("RebuildManager monitor thread started");
        
        while (running_) {
            std::unique_lock<std::mutex> lock(monitor_mutex_);
            monitor_cv_.wait_for(lock, std::chrono::seconds(30));
            
            if (!running_) {
                break;
            }
            
            // Monitor system resources and adjust configuration
            if (config_.enable_memory_monitoring) {
                CheckMemoryUsage();
            }
            
            // Adaptive scheduling based on system load
            if (config_.enable_adaptive_scheduling) {
                AdjustScheduling();
            }
        }
        
        logger_.Info("RebuildManager monitor thread stopped");
    }
    
    void ExecuteRebuild(const RebuildTask& task) {
        stats_.current_active_rebuilds++;
        stats_.total_rebuilds++;
        
        auto start_time = std::chrono::steady_clock::now();
        
        try {
            logger_.Info("Starting rebuild for " + task.db_name + 
                        (task.table_name.empty() ? "" : ":" + task.table_name));
            
            // Actual rebuild logic would go here
            // This would call the appropriate DB/Table rebuild methods
            // For now, this is a placeholder
            
            // Simulate rebuild work
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            auto duration = std::chrono::steady_clock::now() - start_time;
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            
            stats_.total_rebuild_time_ms += ms;
            stats_.successful_rebuilds++;
            
            logger_.Info("Completed rebuild for " + task.db_name + 
                        " in " + std::to_string(ms) + "ms");
                        
        } catch (const std::exception& e) {
            stats_.failed_rebuilds++;
            logger_.Error("Rebuild failed for " + task.db_name + ": " + e.what());
        }
        
        stats_.current_active_rebuilds--;
        
        // Reschedule if needed based on priority
        if (!task.force_rebuild) {
            auto interval = GetIntervalForPriority(task.priority);
            std::thread([this, task, interval]() {
                std::this_thread::sleep_for(interval);
                ScheduleRebuild(task.db_name, task.table_name, task.priority, false);
            }).detach();
        }
    }
    
    std::chrono::milliseconds GetIntervalForPriority(RebuildPriority priority) {
        switch (priority) {
            case RebuildPriority::CRITICAL:
                return config_.critical_priority_interval;
            case RebuildPriority::HIGH:
                return config_.high_priority_interval;
            case RebuildPriority::NORMAL:
                return config_.normal_priority_interval;
            case RebuildPriority::LOW:
            default:
                return config_.low_priority_interval;
        }
    }
    
    void CheckMemoryUsage() {
        // Placeholder for memory monitoring
        // Would check system memory and adjust max_concurrent_rebuilds if needed
    }
    
    void AdjustScheduling() {
        // Placeholder for adaptive scheduling
        // Would adjust rebuild intervals based on system load and rebuild success rate
        double success_rate = stats_.GetSuccessRate();
        if (success_rate < 90.0 && config_.max_concurrent_rebuilds > 1) {
            config_.max_concurrent_rebuilds--;
            logger_.Info("Reduced max concurrent rebuilds to " + 
                        std::to_string(config_.max_concurrent_rebuilds));
        } else if (success_rate > 95.0 && stats_.current_active_rebuilds == config_.max_concurrent_rebuilds) {
            config_.max_concurrent_rebuilds++;
            logger_.Info("Increased max concurrent rebuilds to " + 
                        std::to_string(config_.max_concurrent_rebuilds));
        }
    }

private:
    RebuildConfig config_;
    RebuildStats stats_;
    
    std::atomic<bool> running_;
    std::atomic<bool> paused_;
    
    std::thread worker_thread_;
    std::thread monitor_thread_;
    
    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::priority_queue<RebuildTask> task_queue_;
    std::unordered_set<std::string> scheduled_tasks_;
    
    std::mutex monitor_mutex_;
    std::condition_variable monitor_cv_;
    
    Logger logger_;
};

} // namespace engine
} // namespace vectordb