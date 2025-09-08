#pragma once

#include <atomic>
#include <thread>
#include <condition_variable>
#include <queue>
#include <memory>
#include "db/table_segment.hpp"
#include "logger/logger.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

// Configuration for incremental compaction
struct CompactionConfig {
    // Threshold for triggering compaction (ratio of deleted records)
    double deletion_ratio_threshold = 0.2;  // 20% deleted triggers compaction
    
    // Maximum records to compact in one batch
    size_t batch_size = 10000;
    
    // Maximum time to spend on one compaction cycle (milliseconds)
    int64_t max_cycle_time_ms = 100;
    
    // Minimum interval between compaction attempts (milliseconds)
    int64_t min_interval_ms = 5000;
    
    // Whether to run compaction in background thread
    bool background_compaction = true;
    
    // Priority level for background compaction (0-10, higher = more aggressive)
    int priority_level = 5;
};

// Incremental compaction strategy
class IncrementalCompactor {
public:
    IncrementalCompactor(const CompactionConfig& config = CompactionConfig())
        : config_(config), running_(false), stop_requested_(false) {}
    
    ~IncrementalCompactor() {
        Stop();
    }
    
    // Start background compaction thread
    void Start() {
        if (running_.exchange(true)) {
            return;  // Already running
        }
        
        stop_requested_ = false;
        compaction_thread_ = std::thread([this]() {
            BackgroundCompactionLoop();
        });
        
        logger_.Info(std::string("Incremental compactor started with config: ") +
                     "threshold=" + std::to_string(config_.deletion_ratio_threshold) +
                     ", batch_size=" + std::to_string(config_.batch_size));
    }
    
    // Stop background compaction thread
    void Stop() {
        if (!running_.exchange(false)) {
            return;  // Not running
        }
        
        stop_requested_ = true;
        cv_.notify_all();
        
        if (compaction_thread_.joinable()) {
            compaction_thread_.join();
        }
        
        logger_.Info("Incremental compactor stopped");
    }
    
    // Register a segment for compaction monitoring
    void RegisterSegment(std::shared_ptr<TableSegment> segment, const std::string& segment_name) {
        std::lock_guard<std::mutex> lock(segments_mutex_);
        segments_[segment_name] = segment;
        logger_.Debug("Registered segment for compaction: " + segment_name);
    }
    
    // Unregister a segment
    void UnregisterSegment(const std::string& segment_name) {
        std::lock_guard<std::mutex> lock(segments_mutex_);
        segments_.erase(segment_name);
        logger_.Debug("Unregistered segment from compaction: " + segment_name);
    }
    
    // Trigger immediate compaction check
    void TriggerCompaction() {
        cv_.notify_all();
    }
    
    // Perform incremental compaction on a segment
    Status CompactSegmentIncremental(TableSegment* segment) {
        if (segment == nullptr) {
            return Status(DB_UNEXPECTED_ERROR, "Null segment provided for compaction");
        }
        
        size_t total_records = segment->record_number_.load();
        if (total_records == 0) {
            return Status::OK();  // Nothing to compact
        }
        
        size_t deleted_count = segment->deleted_->count(total_records);
        double deletion_ratio = static_cast<double>(deleted_count) / total_records;
        
        if (deletion_ratio < config_.deletion_ratio_threshold) {
            return Status::OK();  // No need to compact yet
        }
        
        logger_.Info("Starting incremental compaction: " + 
                     std::to_string(deleted_count) + "/" + std::to_string(total_records) + 
                     " records deleted (" + std::to_string(deletion_ratio * 100) + "%)");
        
        auto start_time = std::chrono::steady_clock::now();
        
        // Phase 1: Create a compaction plan
        CompactionPlan plan = CreateCompactionPlan(segment, deleted_count);
        
        // Phase 2: Execute compaction in batches
        Status status = ExecuteCompactionPlan(segment, plan);
        
        auto duration = std::chrono::steady_clock::now() - start_time;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        
        if (status.ok()) {
            logger_.Info("Incremental compaction completed in " + std::to_string(ms) + "ms, " +
                        "compacted " + std::to_string(plan.records_to_move) + " records");
        } else {
            logger_.Error("Incremental compaction failed: " + status.message());
        }
        
        return status;
    }
    
    // Update configuration
    void UpdateConfig(const CompactionConfig& config) {
        config_ = config;
        logger_.Info("Compaction config updated");
    }
    
    // Get compaction statistics
    struct CompactionStats {
        size_t total_compactions = 0;
        size_t records_compacted = 0;
        size_t bytes_reclaimed = 0;
        int64_t total_time_ms = 0;
    };
    
    CompactionStats GetStats() const {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        return stats_;
    }
    
private:
    struct CompactionPlan {
        size_t records_to_move;
        std::vector<size_t> source_indices;
        std::vector<size_t> target_indices;
        size_t new_segment_size;
    };
    
    CompactionPlan CreateCompactionPlan(TableSegment* segment, size_t deleted_count) {
        CompactionPlan plan;
        size_t total_records = segment->record_number_.load();
        plan.new_segment_size = total_records - deleted_count;
        
        // Find holes (deleted records) and records to move
        size_t write_pos = 0;
        for (size_t read_pos = 0; read_pos < total_records; ++read_pos) {
            if (segment->deleted_->test(read_pos)) {
                // Found a hole
                continue;
            }
            
            if (read_pos != write_pos) {
                // This record needs to be moved
                plan.source_indices.push_back(read_pos);
                plan.target_indices.push_back(write_pos);
            }
            write_pos++;
        }
        
        plan.records_to_move = plan.source_indices.size();
        
        // Limit batch size for incremental processing
        if (plan.records_to_move > config_.batch_size) {
            plan.source_indices.resize(config_.batch_size);
            plan.target_indices.resize(config_.batch_size);
            plan.records_to_move = config_.batch_size;
        }
        
        return plan;
    }
    
    Status ExecuteCompactionPlan(TableSegment* segment, const CompactionPlan& plan) {
        if (plan.records_to_move == 0) {
            return Status::OK();
        }
        
        // Lock for write during compaction
        std::unique_lock<std::shared_mutex> lock(segment->data_rw_mutex_);
        
        // Move records in batches
        for (size_t i = 0; i < plan.records_to_move; ++i) {
            size_t src = plan.source_indices[i];
            size_t dst = plan.target_indices[i];
            
            // Check if we should stop
            if (stop_requested_) {
                return Status(DB_UNEXPECTED_ERROR, "Compaction interrupted");
            }
            
            // Move primitive attributes
            if (segment->primitive_offset_ > 0 && segment->attribute_table_) {
                std::memcpy(
                    segment->attribute_table_.get() + dst * segment->primitive_offset_,
                    segment->attribute_table_.get() + src * segment->primitive_offset_,
                    segment->primitive_offset_
                );
            }
            
            // Move vector data
            for (int v = 0; v < segment->dense_vector_num_; ++v) {
                if (segment->vector_tables_[v]) {
                    std::memcpy(
                        segment->vector_tables_[v].get() + dst * segment->vector_dims_[v],
                        segment->vector_tables_[v].get() + src * segment->vector_dims_[v],
                        segment->vector_dims_[v] * sizeof(float)
                    );
                }
            }
            
            // Move variable length attributes
            for (int v = 0; v < segment->var_len_attr_num_; ++v) {
                if (dst != src) {
                    segment->var_len_attr_table_[v][dst] = 
                        std::move(segment->var_len_attr_table_[v][src]);
                }
            }
            
            // Update primary key mapping
            UpdatePrimaryKeyMapping(segment, src, dst);
            
            // Clear the deleted bit for the destination
            segment->deleted_->clear(dst);
            
            // Mark source as deleted (will be truncated later)
            if (src >= plan.new_segment_size) {
                segment->deleted_->set(src);
            }
        }
        
        // Update record count if we've moved all records
        if (plan.records_to_move == plan.source_indices.size()) {
            // This was a complete compaction
            segment->record_number_ = plan.new_segment_size;
            
            // Resize variable length containers
            for (auto& container : segment->var_len_attr_table_) {
                container.resize(plan.new_segment_size);
            }
        }
        
        // Mark segment as modified
        segment->skip_sync_disk_.store(false);
        
        // Update statistics
        {
            std::lock_guard<std::mutex> stats_lock(stats_mutex_);
            stats_.total_compactions++;
            stats_.records_compacted += plan.records_to_move;
            // Rough estimate of bytes reclaimed
            stats_.bytes_reclaimed += plan.records_to_move * 
                (segment->primitive_offset_ + 
                 segment->dense_vector_num_ * 4 * 16);  // Rough estimate
        }
        
        return Status::OK();
    }
    
    void UpdatePrimaryKeyMapping(TableSegment* segment, size_t old_idx, size_t new_idx) {
        if (segment->pk_field_idx_ == nullptr) {
            return;  // No primary key
        }
        
        if (segment->isIntPK()) {
            auto offset = segment->field_id_mem_offset_map_[*segment->pk_field_idx_] + 
                         old_idx * segment->primitive_offset_;
            
            switch (segment->pkType()) {
                case meta::FieldType::INT1: {
                    int8_t pk;
                    std::memcpy(&pk, &segment->attribute_table_.get()[offset], sizeof(int8_t));
                    segment->primary_key_.updateKey(pk, new_idx);
                    break;
                }
                case meta::FieldType::INT2: {
                    int16_t pk;
                    std::memcpy(&pk, &segment->attribute_table_.get()[offset], sizeof(int16_t));
                    segment->primary_key_.updateKey(pk, new_idx);
                    break;
                }
                case meta::FieldType::INT4: {
                    int32_t pk;
                    std::memcpy(&pk, &segment->attribute_table_.get()[offset], sizeof(int32_t));
                    segment->primary_key_.updateKey(pk, new_idx);
                    break;
                }
                case meta::FieldType::INT8: {
                    int64_t pk;
                    std::memcpy(&pk, &segment->attribute_table_.get()[offset], sizeof(int64_t));
                    segment->primary_key_.updateKey(pk, new_idx);
                    break;
                }
                default:
                    break;
            }
        } else if (segment->isStringPK() && segment->string_pk_offset_) {
            auto& pk = std::get<std::string>(
                segment->var_len_attr_table_[*segment->string_pk_offset_][old_idx]
            );
            segment->primary_key_.updateKey(pk, new_idx);
        }
    }
    
    void BackgroundCompactionLoop() {
        logger_.Info("Background compaction thread started");
        
        while (running_) {
            std::unique_lock<std::mutex> lock(cv_mutex_);
            
            // Wait for next compaction interval or trigger
            cv_.wait_for(lock, std::chrono::milliseconds(config_.min_interval_ms),
                        [this] { return !running_ || stop_requested_; });
            
            if (!running_ || stop_requested_) {
                break;
            }
            
            // Check all registered segments for compaction needs
            std::vector<std::pair<std::string, std::shared_ptr<TableSegment>>> to_compact;
            {
                std::lock_guard<std::mutex> seg_lock(segments_mutex_);
                for (const auto& [name, segment] : segments_) {
                    if (segment && segment->NeedsCompaction(config_.deletion_ratio_threshold)) {
                        to_compact.push_back({name, segment});
                    }
                }
            }
            
            // Perform compaction on segments that need it
            for (const auto& [name, segment] : to_compact) {
                if (!running_ || stop_requested_) {
                    break;
                }
                
                logger_.Debug("Compacting segment: " + name);
                auto status = CompactSegmentIncremental(segment.get());
                
                if (!status.ok()) {
                    logger_.Error("Failed to compact segment " + name + ": " + status.message());
                }
                
                // Sleep based on priority level
                int sleep_ms = (10 - config_.priority_level) * 10;
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
            }
        }
        
        logger_.Info("Background compaction thread stopped");
    }
    
    CompactionConfig config_;
    std::atomic<bool> running_;
    std::atomic<bool> stop_requested_;
    std::thread compaction_thread_;
    
    std::mutex cv_mutex_;
    std::condition_variable cv_;
    
    std::mutex segments_mutex_;
    std::unordered_map<std::string, std::shared_ptr<TableSegment>> segments_;
    
    mutable std::mutex stats_mutex_;
    CompactionStats stats_;
    
    Logger logger_;
};

} // namespace engine
} // namespace vectordb