#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>
#include <unordered_map>
#include <chrono>

#include "db/table_segment.hpp"
#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {
namespace compaction {

/**
 * @brief Copy-on-Write compaction configuration
 */
struct COWCompactionConfig {
    // Threshold for triggering compaction (ratio of deleted records)
    double deletion_ratio_threshold = 0.2;
    
    // Maximum memory overhead for shadow copy (MB)
    size_t max_shadow_memory_mb = 512;
    
    // Whether to perform validation after compaction
    bool validate_after_compaction = true;
    
    // Number of records to process in each batch
    size_t batch_size = 1000;
    
    // Enable parallel compaction
    bool enable_parallel = true;
    
    // Number of parallel workers
    size_t parallel_workers = 4;
    
    // Whether to keep old version for rollback
    bool enable_rollback = true;
    
    // Rollback window in seconds
    int rollback_window_seconds = 60;
};

/**
 * @brief Shadow segment for COW compaction
 * 
 * This class represents a shadow copy of the original segment
 * that can be atomically swapped after compaction
 */
class ShadowSegment {
public:
    ShadowSegment(size_t record_capacity, 
                  size_t primitive_offset,
                  const std::vector<size_t>& vector_dims,
                  size_t var_len_attr_num)
        : capacity_(record_capacity),
          primitive_offset_(primitive_offset),
          vector_dims_(vector_dims),
          var_len_attr_num_(var_len_attr_num),
          record_count_(0) {
        
        // Allocate shadow storage
        if (primitive_offset_ > 0) {
            attribute_table_ = std::make_unique<char[]>(capacity_ * primitive_offset_);
        }
        
        // Allocate vector tables
        vector_tables_.reserve(vector_dims_.size());
        for (size_t dim : vector_dims_) {
            vector_tables_.push_back(std::make_unique<float[]>(capacity_ * dim));
        }
        
        // Initialize variable length attribute containers
        var_len_attrs_.resize(var_len_attr_num_);
        for (auto& container : var_len_attrs_) {
            container.reserve(capacity_);
        }
        
        // Initialize deleted bitmap
        deleted_bitmap_ = std::make_unique<ConcurrentBitset>(capacity_);
    }
    
    /**
     * @brief Copy a record from source to shadow
     */
    Status CopyRecord(const TableSegmentMVP* source, size_t src_idx, size_t dst_idx) {
        if (!source || src_idx >= source->GetRecordCount() || dst_idx >= capacity_) {
            return Status(INVALID_ARGUMENT, "Invalid index for record copy");
        }
        
        try {
            // Copy primitive attributes
            if (primitive_offset_ > 0 && source->attribute_table_) {
                std::memcpy(
                    attribute_table_.get() + dst_idx * primitive_offset_,
                    source->attribute_table_->GetData() + src_idx * source->primitive_offset_,
                    primitive_offset_
                );
            }
            
            // Copy vector data
            for (size_t v = 0; v < vector_dims_.size(); ++v) {
                if (v < source->vector_tables_.size() && source->vector_tables_[v]) {
                    std::memcpy(
                        vector_tables_[v].get() + dst_idx * vector_dims_[v],
                        source->vector_tables_[v]->GetData() + src_idx * source->vector_dims_[v],
                        vector_dims_[v] * sizeof(float)
                    );
                }
            }
            
            // Copy variable length attributes
            for (size_t v = 0; v < var_len_attr_num_; ++v) {
                if (v < source->var_len_attr_table_.size() && 
                    src_idx < source->var_len_attr_table_[v].size()) {
                    if (dst_idx >= var_len_attrs_[v].size()) {
                        var_len_attrs_[v].resize(dst_idx + 1);
                    }
                    var_len_attrs_[v][dst_idx] = source->var_len_attr_table_[v][src_idx];
                }
            }
            
            // Update record count
            if (dst_idx >= record_count_) {
                record_count_ = dst_idx + 1;
            }
            
            return Status::OK();
            
        } catch (const std::exception& e) {
            return Status(DB_UNEXPECTED_ERROR, 
                         "Failed to copy record: " + std::string(e.what()));
        }
    }
    
    size_t GetRecordCount() const { return record_count_; }
    
    // Storage members (public for atomic swap)
    std::unique_ptr<char[]> attribute_table_;
    std::vector<std::unique_ptr<float[]>> vector_tables_;
    std::vector<std::vector<std::variant<int64_t, double, std::string>>> var_len_attrs_;
    std::unique_ptr<ConcurrentBitset> deleted_bitmap_;
    
private:
    size_t capacity_;
    size_t primitive_offset_;
    std::vector<size_t> vector_dims_;
    size_t var_len_attr_num_;
    std::atomic<size_t> record_count_;
};

/**
 * @brief Copy-on-Write Compactor
 * 
 * This compactor creates a shadow copy of the segment data,
 * performs compaction on the shadow, then atomically swaps
 * the compacted data with the original, achieving zero-downtime
 * compaction.
 */
class COWCompactor {
public:
    COWCompactor(const COWCompactionConfig& config = COWCompactionConfig())
        : config_(config) {}
    
    /**
     * @brief Perform COW compaction on a segment
     * 
     * @param segment The segment to compact
     * @return Status indicating success or failure
     */
    Status CompactSegment(TableSegmentMVP* segment) {
        if (!segment) {
            return Status(INVALID_ARGUMENT, "Null segment provided");
        }
        
        auto start_time = std::chrono::steady_clock::now();
        
        // Check if compaction is needed
        size_t total_records = segment->GetRecordCount();
        size_t deleted_count = segment->GetDeletedCount();
        
        if (total_records == 0) {
            return Status::OK();  // Nothing to compact
        }
        
        double deletion_ratio = static_cast<double>(deleted_count) / total_records;
        if (deletion_ratio < config_.deletion_ratio_threshold) {
            return Status::OK();  // No need to compact
        }
        
        logger_.Info("Starting COW compaction: " + std::to_string(deleted_count) + 
                    "/" + std::to_string(total_records) + " records deleted (" + 
                    std::to_string(deletion_ratio * 100) + "%)");
        
        // Phase 1: Create shadow segment
        auto shadow_result = CreateShadowSegment(segment, total_records - deleted_count);
        if (!shadow_result.first.ok()) {
            return shadow_result.first;
        }
        auto shadow = std::move(shadow_result.second);
        
        // Phase 2: Copy live records to shadow
        auto copy_status = CopyLiveRecords(segment, shadow.get());
        if (!copy_status.ok()) {
            return copy_status;
        }
        
        // Phase 3: Build index mappings
        auto mapping = BuildIndexMapping(segment, shadow.get());
        
        // Phase 4: Validate shadow segment if configured
        if (config_.validate_after_compaction) {
            auto validate_status = ValidateShadow(segment, shadow.get(), mapping);
            if (!validate_status.ok()) {
                logger_.Error("Shadow validation failed: " + validate_status.message());
                return validate_status;
            }
        }
        
        // Phase 5: Save old version for rollback if configured
        std::unique_ptr<SegmentSnapshot> old_snapshot;
        if (config_.enable_rollback) {
            old_snapshot = CreateSnapshot(segment);
        }
        
        // Phase 6: Atomic swap
        auto swap_status = AtomicSwap(segment, shadow.get(), mapping);
        if (!swap_status.ok()) {
            // Rollback if swap failed
            if (old_snapshot) {
                RestoreSnapshot(segment, old_snapshot.get());
            }
            return swap_status;
        }
        
        // Phase 7: Schedule cleanup of old data
        if (config_.enable_rollback && old_snapshot) {
            ScheduleCleanup(std::move(old_snapshot), config_.rollback_window_seconds);
        }
        
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);
        
        logger_.Info("COW compaction completed in " + std::to_string(duration.count()) + 
                    "ms, compacted from " + std::to_string(total_records) + 
                    " to " + std::to_string(shadow->GetRecordCount()) + " records");
        
        return Status::OK();
    }
    
private:
    /**
     * @brief Create a shadow segment
     */
    std::pair<Status, std::unique_ptr<ShadowSegment>> CreateShadowSegment(
        const TableSegmentMVP* segment, size_t new_size) {
        
        try {
            // Check memory limit
            size_t estimated_memory = EstimateMemoryUsage(segment, new_size);
            if (estimated_memory > config_.max_shadow_memory_mb * 1024 * 1024) {
                return {Status(RESOURCE_EXHAUSTED, 
                              "Shadow segment would exceed memory limit"), nullptr};
            }
            
            auto shadow = std::make_unique<ShadowSegment>(
                new_size,
                segment->primitive_offset_,
                segment->vector_dims_,
                segment->var_len_attr_num_
            );
            
            return {Status::OK(), std::move(shadow)};
            
        } catch (const std::exception& e) {
            return {Status(DB_UNEXPECTED_ERROR, 
                          "Failed to create shadow segment: " + std::string(e.what())), 
                   nullptr};
        }
    }
    
    /**
     * @brief Copy live records from source to shadow
     */
    Status CopyLiveRecords(TableSegmentMVP* segment, ShadowSegment* shadow) {
        size_t total_records = segment->GetRecordCount();
        size_t write_pos = 0;
        
        if (config_.enable_parallel) {
            // Parallel copying
            return CopyLiveRecordsParallel(segment, shadow);
        }
        
        // Sequential copying
        for (size_t read_pos = 0; read_pos < total_records; ++read_pos) {
            // Skip deleted records
            if (segment->deleted_ && segment->deleted_->test(read_pos)) {
                continue;
            }
            
            // Copy live record
            auto status = shadow->CopyRecord(segment, read_pos, write_pos);
            if (!status.ok()) {
                return status;
            }
            
            // Update mapping
            old_to_new_[read_pos] = write_pos;
            new_to_old_[write_pos] = read_pos;
            
            write_pos++;
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Parallel copy of live records
     */
    Status CopyLiveRecordsParallel(TableSegmentMVP* segment, ShadowSegment* shadow) {
        size_t total_records = segment->GetRecordCount();
        size_t batch_size = config_.batch_size;
        size_t num_batches = (total_records + batch_size - 1) / batch_size;
        
        std::vector<std::thread> workers;
        std::atomic<size_t> next_batch(0);
        std::atomic<size_t> write_position(0);
        std::mutex mapping_mutex;
        std::atomic<bool> error_occurred(false);
        std::string error_message;
        
        for (size_t w = 0; w < config_.parallel_workers; ++w) {
            workers.emplace_back([&]() {
                while (!error_occurred) {
                    size_t batch_idx = next_batch.fetch_add(1);
                    if (batch_idx >= num_batches) {
                        break;
                    }
                    
                    size_t start = batch_idx * batch_size;
                    size_t end = std::min(start + batch_size, total_records);
                    
                    // Collect live records in this batch
                    std::vector<size_t> live_records;
                    for (size_t i = start; i < end; ++i) {
                        if (!segment->deleted_ || !segment->deleted_->test(i)) {
                            live_records.push_back(i);
                        }
                    }
                    
                    if (live_records.empty()) {
                        continue;
                    }
                    
                    // Reserve write positions
                    size_t write_start = write_position.fetch_add(live_records.size());
                    
                    // Copy records
                    for (size_t j = 0; j < live_records.size(); ++j) {
                        auto status = shadow->CopyRecord(segment, 
                                                        live_records[j], 
                                                        write_start + j);
                        if (!status.ok()) {
                            error_occurred = true;
                            error_message = status.message();
                            break;
                        }
                        
                        // Update mapping
                        {
                            std::lock_guard<std::mutex> lock(mapping_mutex);
                            old_to_new_[live_records[j]] = write_start + j;
                            new_to_old_[write_start + j] = live_records[j];
                        }
                    }
                }
            });
        }
        
        // Wait for all workers
        for (auto& worker : workers) {
            worker.join();
        }
        
        if (error_occurred) {
            return Status(DB_UNEXPECTED_ERROR, "Parallel copy failed: " + error_message);
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Build index mapping
     */
    std::unordered_map<size_t, size_t> BuildIndexMapping(
        const TableSegmentMVP* segment, const ShadowSegment* shadow) {
        // Return the already built mapping
        return old_to_new_;
    }
    
    /**
     * @brief Validate shadow segment
     */
    Status ValidateShadow(const TableSegmentMVP* original, 
                         const ShadowSegment* shadow,
                         const std::unordered_map<size_t, size_t>& mapping) {
        // Basic validation
        size_t expected_records = original->GetRecordCount() - original->GetDeletedCount();
        if (shadow->GetRecordCount() != expected_records) {
            return Status(DB_UNEXPECTED_ERROR, 
                         "Shadow record count mismatch: expected " + 
                         std::to_string(expected_records) + ", got " + 
                         std::to_string(shadow->GetRecordCount()));
        }
        
        // Sample validation - check a few records
        size_t samples = std::min(100UL, shadow->GetRecordCount());
        for (size_t i = 0; i < samples; ++i) {
            size_t sample_idx = (i * shadow->GetRecordCount()) / samples;
            
            // Find original index
            auto it = new_to_old_.find(sample_idx);
            if (it == new_to_old_.end()) {
                return Status(DB_UNEXPECTED_ERROR, 
                             "Missing mapping for shadow index " + 
                             std::to_string(sample_idx));
            }
            
            size_t orig_idx = it->second;
            
            // Validate primitive attributes match
            if (original->primitive_offset_ > 0 && original->attribute_table_) {
                if (std::memcmp(
                    shadow->attribute_table_.get() + sample_idx * original->primitive_offset_,
                    original->attribute_table_->GetData() + orig_idx * original->primitive_offset_,
                    original->primitive_offset_) != 0) {
                    return Status(DB_UNEXPECTED_ERROR, 
                                 "Attribute mismatch at index " + 
                                 std::to_string(sample_idx));
                }
            }
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Atomically swap shadow with original
     */
    Status AtomicSwap(TableSegmentMVP* segment, ShadowSegment* shadow,
                     const std::unordered_map<size_t, size_t>& mapping) {
        // Acquire exclusive lock
        std::unique_lock<std::mutex> lock(segment->data_update_mutex_);
        
        try {
            // Swap storage - need to handle DynamicMemoryBlock properly
            // For now, we'll need to copy the data since DynamicMemoryBlock doesn't support swap
            if (shadow->attribute_table_ && segment->attribute_table_) {
                size_t size = shadow->GetRecordCount() * segment->primitive_offset_;
                std::memcpy(segment->attribute_table_->GetData(),
                           shadow->attribute_table_.get(), size);
                segment->attribute_table_->Resize(shadow->GetRecordCount());
            }

            // Swap vector tables - need to copy data for DynamicVectorBlock
            for (size_t v = 0; v < segment->vector_tables_.size(); ++v) {
                if (v < shadow->vector_tables_.size() && shadow->vector_tables_[v]) {
                    size_t vec_size = shadow->GetRecordCount() * segment->vector_dims_[v];
                    std::memcpy(segment->vector_tables_[v]->GetData(),
                               shadow->vector_tables_[v].get(),
                               vec_size * sizeof(float));
                    segment->vector_tables_[v]->ResizeVectors(shadow->GetRecordCount());
                }
            }
            segment->var_len_attr_table_.swap(shadow->var_len_attrs_);
            
            // Update metadata
            segment->record_number_ = shadow->GetRecordCount();
            
            // Clear deleted bitmap
            segment->deleted_->clear_all();
            
            // Update primary key mappings
            UpdatePrimaryKeyMappings(segment, mapping);
            
            // Mark segment as modified
            segment->skip_sync_disk_ = false;
            
            return Status::OK();
            
        } catch (const std::exception& e) {
            return Status(DB_UNEXPECTED_ERROR, 
                         "Atomic swap failed: " + std::string(e.what()));
        }
    }
    
    /**
     * @brief Update primary key mappings after compaction
     */
    void UpdatePrimaryKeyMappings(TableSegmentMVP* segment,
                                  const std::unordered_map<size_t, size_t>& mapping) {
        // Update primary key index with new positions
        // This would need to iterate through the primary key map
        // and update all index positions using the mapping
        
        // Note: Actual implementation would depend on the 
        // primary key structure in TableSegmentMVP
    }
    
    /**
     * @brief Snapshot for rollback
     */
    struct SegmentSnapshot {
        std::unique_ptr<char[]> attribute_table;
        std::vector<std::unique_ptr<float[]>> vector_tables;
        std::vector<std::vector<std::variant<int64_t, double, std::string>>> var_len_attrs;
        std::unique_ptr<ConcurrentBitset> deleted_bitmap;
        size_t record_count;
        std::chrono::steady_clock::time_point timestamp;
    };
    
    /**
     * @brief Create a snapshot of the segment
     */
    std::unique_ptr<SegmentSnapshot> CreateSnapshot(const TableSegmentMVP* segment) {
        auto snapshot = std::make_unique<SegmentSnapshot>();
        
        // Deep copy current state
        size_t total_records = segment->GetRecordCount();
        
        if (segment->primitive_offset_ > 0 && segment->attribute_table_) {
            size_t size = total_records * segment->primitive_offset_;
            snapshot->attribute_table = std::make_unique<char[]>(size);
            std::memcpy(snapshot->attribute_table.get(),
                       segment->attribute_table_->GetData(), size);
        }
        
        // Copy vector tables
        for (size_t v = 0; v < segment->vector_tables_.size(); ++v) {
            if (segment->vector_tables_[v]) {
                size_t size = total_records * segment->vector_dims_[v];
                auto table = std::make_unique<float[]>(size);
                std::memcpy(table.get(), 
                           segment->vector_tables_[v]->GetData(), 
                           size * sizeof(float));
                snapshot->vector_tables.push_back(std::move(table));
            }
        }
        
        // Copy var-length attributes
        snapshot->var_len_attrs = segment->var_len_attr_table_;
        
        // Copy deleted bitmap
        if (segment->deleted_) {
            snapshot->deleted_bitmap = std::make_unique<ConcurrentBitset>(*segment->deleted_);
        }
        
        snapshot->record_count = total_records;
        snapshot->timestamp = std::chrono::steady_clock::now();
        
        return snapshot;
    }
    
    /**
     * @brief Restore from snapshot
     */
    void RestoreSnapshot(TableSegmentMVP* segment, const SegmentSnapshot* snapshot) {
        std::unique_lock<std::mutex> lock(segment->data_update_mutex_);
        
        // Restore all data - copy into DynamicMemoryBlock
        if (snapshot->attribute_table && segment->attribute_table_) {
            size_t size = snapshot->record_count * segment->primitive_offset_;
            std::memcpy(segment->attribute_table_->GetData(),
                       snapshot->attribute_table.get(), size);
            segment->attribute_table_->Resize(snapshot->record_count);
        }

        // Restore vector tables
        for (size_t v = 0; v < snapshot->vector_tables.size(); ++v) {
            if (v < segment->vector_tables_.size() && snapshot->vector_tables[v]) {
                size_t vec_size = snapshot->record_count * segment->vector_dims_[v];
                std::memcpy(segment->vector_tables_[v]->GetData(),
                           snapshot->vector_tables[v].get(),
                           vec_size * sizeof(float));
                segment->vector_tables_[v]->ResizeVectors(snapshot->record_count);
            }
        }
        segment->var_len_attr_table_ = snapshot->var_len_attrs;
        
        if (snapshot->deleted_bitmap) {
            *segment->deleted_ = *snapshot->deleted_bitmap;
        }
        
        segment->record_number_ = snapshot->record_count;
        
        logger_.Info("Restored segment from snapshot");
    }
    
    /**
     * @brief Schedule cleanup of old snapshot
     */
    void ScheduleCleanup(std::unique_ptr<SegmentSnapshot> snapshot, int delay_seconds) {
        // In production, this would use a proper scheduler
        // For now, we just let the snapshot go out of scope
        // after the rollback window
        
        std::thread cleanup_thread([snapshot = std::move(snapshot), delay_seconds]() {
            std::this_thread::sleep_for(std::chrono::seconds(delay_seconds));
            // Snapshot automatically cleaned up when it goes out of scope
        });
        
        cleanup_thread.detach();
    }
    
    /**
     * @brief Estimate memory usage for shadow segment
     */
    size_t EstimateMemoryUsage(const TableSegmentMVP* segment, size_t new_size) {
        size_t total = 0;
        
        // Primitive attributes
        if (segment->primitive_offset_ > 0) {
            total += new_size * segment->primitive_offset_;
        }
        
        // Vector tables
        for (size_t dim : segment->vector_dims_) {
            total += new_size * dim * sizeof(float);
        }
        
        // Variable length attributes (estimate)
        total += new_size * segment->var_len_attr_num_ * 64;  // Rough estimate
        
        // Bitmap
        total += (new_size + 7) / 8;
        
        return total;
    }
    
    COWCompactionConfig config_;
    Logger logger_;
    
    // Mapping between old and new indices
    std::unordered_map<size_t, size_t> old_to_new_;
    std::unordered_map<size_t, size_t> new_to_old_;
};

} // namespace compaction
} // namespace engine
} // namespace vectordb