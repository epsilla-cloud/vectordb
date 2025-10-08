#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include "db/table_segment.hpp"
#include "logger/logger.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

// Advanced compaction configuration with adaptive strategies
struct AdaptiveCompactionConfig {
    // Basic thresholds
    double deletion_ratio_threshold = 0.15;     // Lower threshold for incremental
    double urgent_threshold = 0.4;              // Urgent compaction threshold
    
    // Incremental settings
    size_t incremental_batch_size = 1000;       // Records per batch
    size_t max_batches_per_cycle = 10;          // Max batches in one cycle
    
    // Performance tuning
    size_t max_memory_per_batch_mb = 100;       // Memory limit per batch
    int priority_level = 5;                     // Thread priority (0-10)
    bool use_copy_on_write = true;              // COW for zero-downtime
    
    // Adaptive parameters
    bool adaptive_scheduling = true;            // Adjust based on load
    double load_threshold = 0.8;                // System load threshold
    size_t min_interval_ms = 5000;              // Min interval between runs
    size_t max_interval_ms = 60000;             // Max interval between runs
    
    // Statistics tracking
    bool track_stats = true;                    // Track compaction stats
};

// Statistics for monitoring compaction performance
struct CompactionStats {
    std::atomic<size_t> total_compactions{0};
    std::atomic<size_t> incremental_compactions{0};
    std::atomic<size_t> full_compactions{0};
    std::atomic<size_t> records_processed{0};
    std::atomic<size_t> records_reclaimed{0};
    std::atomic<size_t> bytes_reclaimed{0};
    std::atomic<double> total_time_ms{0};
    std::atomic<double> avg_throughput_rps{0};  // Records per second
    
    void Reset() {
        total_compactions = 0;
        incremental_compactions = 0;
        full_compactions = 0;
        records_processed = 0;
        records_reclaimed = 0;
        bytes_reclaimed = 0;
        total_time_ms = 0;
        avg_throughput_rps = 0;
    }
};

// Adaptive compactor with non-blocking incremental compaction
class AdaptiveCompactor {
public:
    explicit AdaptiveCompactor(const AdaptiveCompactionConfig& config = AdaptiveCompactionConfig())
        : config_(config), running_(false), paused_(false) {}
    
    ~AdaptiveCompactor() {
        Stop();
    }
    
    // Start the compaction service
    void Start() {
        if (running_.exchange(true)) {
            return;  // Already running
        }
        
        worker_thread_ = std::thread([this] { WorkerLoop(); });
        logger_.Info("Adaptive compactor started");
    }
    
    // Stop the compaction service
    void Stop() {
        if (!running_.exchange(false)) {
            return;  // Not running
        }
        
        cv_.notify_all();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
        
        logger_.Info("Adaptive compactor stopped");
    }
    
    // Register a segment for compaction monitoring
    void RegisterSegment(std::shared_ptr<TableSegment> segment, const std::string& name) {
        std::lock_guard<std::mutex> lock(segments_mutex_);
        segments_[name] = segment;
        segment_stats_[name] = SegmentStats{};
    }
    
    // Unregister a segment
    void UnregisterSegment(const std::string& name) {
        std::lock_guard<std::mutex> lock(segments_mutex_);
        segments_.erase(name);
        segment_stats_.erase(name);
    }
    
    // Trigger immediate compaction
    void TriggerCompaction(const std::string& segment_name = "") {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (segment_name.empty()) {
            // Trigger for all segments
            for (const auto& [name, _] : segments_) {
                pending_queue_.push({name, true});  // urgent = true
            }
        } else {
            pending_queue_.push({segment_name, true});
        }
        cv_.notify_one();
    }
    
    // Pause compaction
    void Pause() {
        paused_ = true;
        logger_.Info("Compaction paused");
    }
    
    // Resume compaction
    void Resume() {
        paused_ = false;
        cv_.notify_one();
        logger_.Info("Compaction resumed");
    }
    
    // Get statistics
    CompactionStats GetStats() const {
        return global_stats_;
    }
    
    // Update configuration
    void UpdateConfig(const AdaptiveCompactionConfig& config) {
        config_ = config;
        cv_.notify_one();  // Wake up to apply new config
    }
    
private:
    struct SegmentStats {
        size_t last_compaction_time = 0;
        size_t compaction_count = 0;
        double avg_deletion_ratio = 0;
        bool in_progress = false;
    };
    
    struct CompactionRequest {
        std::string segment_name;
        bool urgent = false;
    };
    
    // Main worker loop
    void WorkerLoop() {
        SetThreadPriority(config_.priority_level);
        
        while (running_) {
            if (paused_) {
                std::unique_lock<std::mutex> lock(cv_mutex_);
                cv_.wait_for(lock, std::chrono::milliseconds(1000));
                continue;
            }
            
            // Get next compaction task
            auto task = GetNextTask();
            if (task.segment_name.empty()) {
                // No task, wait
                std::unique_lock<std::mutex> lock(cv_mutex_);
                auto wait_time = CalculateWaitTime();
                cv_.wait_for(lock, std::chrono::milliseconds(wait_time));
                continue;
            }
            
            // Perform compaction
            PerformAdaptiveCompaction(task);
        }
    }
    
    // Get next compaction task based on priority
    CompactionRequest GetNextTask() {
        // First check urgent queue
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (!pending_queue_.empty()) {
                auto task = pending_queue_.top();
                pending_queue_.pop();
                return task;
            }
        }
        
        // Check segments for compaction needs
        std::lock_guard<std::mutex> lock(segments_mutex_);
        
        CompactionRequest best_task;
        double highest_priority = 0;
        
        for (const auto& [name, segment] : segments_) {
            if (!segment || segment_stats_[name].in_progress) {
                continue;
            }
            
            double deletion_ratio = segment->GetDeletedRatio();
            double priority = CalculatePriority(name, deletion_ratio);
            
            if (priority > highest_priority && priority > config_.deletion_ratio_threshold) {
                highest_priority = priority;
                best_task.segment_name = name;
                best_task.urgent = (deletion_ratio > config_.urgent_threshold);
            }
        }
        
        return best_task;
    }
    
    // Perform adaptive compaction
    void PerformAdaptiveCompaction(const CompactionRequest& task) {
        auto segment_it = segments_.find(task.segment_name);
        if (segment_it == segments_.end() || !segment_it->second) {
            return;
        }
        
        auto segment = segment_it->second;
        segment_stats_[task.segment_name].in_progress = true;
        
        auto start_time = std::chrono::steady_clock::now();
        size_t records_reclaimed = 0;
        
        try {
            if (task.urgent || segment->GetDeletedRatio() > config_.urgent_threshold) {
                // Full compaction for urgent cases
                PerformFullCompaction(segment, records_reclaimed);
                global_stats_.full_compactions++;
            } else {
                // Incremental compaction
                PerformIncrementalCompaction(segment, records_reclaimed);
                global_stats_.incremental_compactions++;
            }
            
            auto end_time = std::chrono::steady_clock::now();
            auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - start_time).count();
            
            // Update statistics
            global_stats_.total_compactions++;
            global_stats_.records_reclaimed += records_reclaimed;
            global_stats_.total_time_ms += duration_ms;
            
            if (duration_ms > 0) {
                double throughput = (records_reclaimed * 1000.0) / duration_ms;
                global_stats_.avg_throughput_rps = 
                    (global_stats_.avg_throughput_rps * 0.9 + throughput * 0.1);
            }
            
            segment_stats_[task.segment_name].last_compaction_time = 
                std::chrono::system_clock::now().time_since_epoch().count();
            segment_stats_[task.segment_name].compaction_count++;
            
            logger_.Info("Compaction completed for " + task.segment_name + 
                        ": " + std::to_string(records_reclaimed) + " records reclaimed in " +
                        std::to_string(duration_ms) + "ms");
            
        } catch (const std::exception& e) {
            logger_.Error("Compaction failed for " + task.segment_name + ": " + e.what());
        }
        
        segment_stats_[task.segment_name].in_progress = false;
    }
    
    // Perform incremental compaction (non-blocking)
    void PerformIncrementalCompaction(std::shared_ptr<TableSegment> segment, 
                                     size_t& records_reclaimed) {
        // Use copy-on-write strategy for zero downtime
        if (config_.use_copy_on_write) {
            // TODO: Implement COW compaction
            // For now, fall back to batch processing
        }
        
        // Process in small batches to avoid blocking
        size_t batch_size = config_.incremental_batch_size;
        size_t batches_processed = 0;
        
        while (batches_processed < config_.max_batches_per_cycle && running_ && !paused_) {
            // Process one batch
            // Note: This is a simplified version. Real implementation would need
            // to track position and continue from where it left off
            
            size_t batch_reclaimed = 0;
            // TODO: Implement actual batch compaction logic
            
            records_reclaimed += batch_reclaimed;
            batches_processed++;
            
            // Yield to other operations
            if (batches_processed % 2 == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }
    
    // Perform full compaction
    void PerformFullCompaction(std::shared_ptr<TableSegment> segment, 
                              size_t& records_reclaimed) {
        // Use the existing CompactSegment method
        size_t before_count = segment->GetRecordCount();
        auto status = segment->CompactSegment();
        
        if (status.ok()) {
            size_t after_count = segment->GetRecordCount();
            records_reclaimed = before_count - after_count;
        }
    }
    
    // Calculate priority for compaction
    double CalculatePriority(const std::string& name, double deletion_ratio) {
        auto& stats = segment_stats_[name];
        
        // Base priority is deletion ratio
        double priority = deletion_ratio;
        
        // Boost priority if hasn't been compacted recently
        auto now = std::chrono::system_clock::now().time_since_epoch().count();
        auto time_since_last = now - stats.last_compaction_time;
        if (time_since_last > 3600000000) {  // 1 hour in microseconds
            priority *= 1.5;
        }
        
        // Consider historical deletion ratio trend
        stats.avg_deletion_ratio = stats.avg_deletion_ratio * 0.7 + deletion_ratio * 0.3;
        if (deletion_ratio > stats.avg_deletion_ratio * 1.2) {
            priority *= 1.2;  // Deletion rate increasing
        }
        
        return priority;
    }
    
    // Calculate adaptive wait time
    size_t CalculateWaitTime() {
        if (!config_.adaptive_scheduling) {
            return config_.min_interval_ms;
        }
        
        // Get system load (simplified)
        double system_load = GetSystemLoad();
        
        if (system_load > config_.load_threshold) {
            // High load, wait longer
            return config_.max_interval_ms;
        } else {
            // Scale wait time based on load
            size_t wait = config_.min_interval_ms + 
                         static_cast<size_t>((config_.max_interval_ms - config_.min_interval_ms) * 
                                           system_load / config_.load_threshold);
            return wait;
        }
    }
    
    // Get system load (simplified implementation)
    double GetSystemLoad() {
        // TODO: Implement actual system load monitoring
        // For now, return a moderate value
        return 0.5;
    }
    
    // Set thread priority
    void SetThreadPriority(int priority) {
        // Platform-specific thread priority setting
        // TODO: Implement for different platforms
    }
    
    AdaptiveCompactionConfig config_;
    std::atomic<bool> running_;
    std::atomic<bool> paused_;
    
    std::thread worker_thread_;
    std::mutex cv_mutex_;
    std::condition_variable cv_;
    
    std::mutex segments_mutex_;
    std::unordered_map<std::string, std::shared_ptr<TableSegment>> segments_;
    std::unordered_map<std::string, SegmentStats> segment_stats_;
    
    std::mutex queue_mutex_;
    std::priority_queue<CompactionRequest, std::vector<CompactionRequest>, 
                       std::function<bool(const CompactionRequest&, const CompactionRequest&)>> 
        pending_queue_{[](const auto& a, const auto& b) { return !a.urgent && b.urgent; }};
    
    CompactionStats global_stats_;
    Logger logger_;
};

} // namespace engine
} // namespace vectordb