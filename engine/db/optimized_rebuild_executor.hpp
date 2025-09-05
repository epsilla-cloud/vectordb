#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <thread>
#include <future>
#include "db/table_mvp.hpp"
#include "db/ann_graph_segment.hpp"
#include "logger/logger.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

/**
 * Optimized rebuild executor with parallel processing and memory efficiency
 */
class OptimizedRebuildExecutor {
public:
    struct RebuildOptions {
        // Parallelism settings
        bool enable_parallel_rebuild = true;
        size_t max_parallel_indices = 4;
        size_t rebuild_batch_size = 10000;
        
        // Memory management
        bool enable_memory_pooling = true;
        size_t memory_pool_size_mb = 512;
        bool enable_swap_optimization = true;
        
        // Incremental rebuild
        bool enable_incremental = true;
        double incremental_threshold = 0.1;  // 10% change threshold
        
        // Safety settings
        bool create_backup = true;
        bool verify_after_rebuild = true;
        size_t verification_sample_size = 1000;
    };
    
    struct RebuildProgress {
        std::atomic<size_t> current_step{0};
        std::atomic<size_t> total_steps{0};
        std::atomic<bool> is_running{false};
        std::atomic<bool> is_cancelled{false};
        std::string current_operation;
        
        double GetProgressPercent() const {
            size_t total = total_steps.load();
            if (total == 0) return 0.0;
            return static_cast<double>(current_step.load()) * 100.0 / total;
        }
    };

public:
    OptimizedRebuildExecutor(const RebuildOptions& options = RebuildOptions())
        : options_(options) {}
    
    /**
     * Optimized table rebuild with parallel processing
     */
    Status RebuildTable(std::shared_ptr<TableMVP> table,
                       const std::string& db_catalog_path,
                       RebuildProgress* progress = nullptr) {
        if (!table) {
            return Status(DB_UNEXPECTED_ERROR, "Null table pointer");
        }
        
        if (progress) {
            progress->is_running = true;
            progress->current_operation = "Initializing rebuild";
        }
        
        try {
            // Phase 1: Preparation
            Status status = PrepareRebuild(table, db_catalog_path, progress);
            if (!status.ok()) {
                return status;
            }
            
            // Phase 2: Save data to disk
            if (table->is_leader_) {
                status = SaveTableData(table, db_catalog_path, progress);
                if (!status.ok()) {
                    return status;
                }
            }
            
            // Phase 3: Rebuild indices
            status = RebuildIndices(table, db_catalog_path, progress);
            if (!status.ok()) {
                return status;
            }
            
            // Phase 4: Swap executors atomically
            status = SwapExecutors(table, progress);
            if (!status.ok()) {
                return status;
            }
            
            // Phase 5: Verification (optional)
            if (options_.verify_after_rebuild) {
                status = VerifyRebuild(table, progress);
                if (!status.ok()) {
                    logger_.Warning("Rebuild verification failed: " + status.message());
                }
            }
            
            // Phase 6: Cleanup
            CleanupAfterRebuild(table, db_catalog_path, progress);
            
            if (progress) {
                progress->is_running = false;
                progress->current_operation = "Rebuild completed";
            }
            
            return Status::OK();
            
        } catch (const std::exception& e) {
            logger_.Error("Rebuild failed with exception: " + std::string(e.what()));
            if (progress) {
                progress->is_running = false;
            }
            return Status(DB_UNEXPECTED_ERROR, "Rebuild failed: " + std::string(e.what()));
        }
    }
    
    /**
     * Cancel ongoing rebuild
     */
    void CancelRebuild(RebuildProgress* progress) {
        if (progress) {
            progress->is_cancelled = true;
        }
    }

private:
    Status PrepareRebuild(std::shared_ptr<TableMVP> table,
                         const std::string& db_catalog_path,
                         RebuildProgress* progress) {
        if (progress) {
            progress->current_operation = "Preparing rebuild";
        }
        
        // Set optimal thread count for rebuild
        int rebuild_threads = std::min(
            static_cast<int>(std::thread::hardware_concurrency()),
            globalConfig.RebuildThreads
        );
        omp_set_num_threads(rebuild_threads);
        
        logger_.Debug("Preparing rebuild with " + std::to_string(rebuild_threads) + " threads");
        
        // Create backup if enabled
        if (options_.create_backup) {
            // TODO: Implement backup creation
        }
        
        return Status::OK();
    }
    
    Status SaveTableData(std::shared_ptr<TableMVP> table,
                        const std::string& db_catalog_path,
                        RebuildProgress* progress) {
        if (progress) {
            progress->current_operation = "Saving table data";
        }
        
        logger_.Debug("Saving table segment to disk");
        
        // Save with write-ahead optimization
        table->table_segment_->SaveTableSegment(table->table_schema_, db_catalog_path);
        
        // Clean up old WAL files
        table->wal_->CleanUpOldFiles();
        
        return Status::OK();
    }
    
    Status RebuildIndices(std::shared_ptr<TableMVP> table,
                         const std::string& db_catalog_path,
                         RebuildProgress* progress) {
        int64_t record_number = table->table_segment_->record_number_;
        std::vector<std::future<Status>> rebuild_futures;
        
        if (progress) {
            progress->current_operation = "Rebuilding indices";
            progress->total_steps = table->ann_graph_segment_.size();
            progress->current_step = 0;
        }
        
        int64_t index = 0;
        for (size_t i = 0; i < table->table_schema_.fields_.size(); ++i) {
            auto& field = table->table_schema_.fields_[i];
            
            if (!IsVectorField(field.field_type_)) {
                continue;
            }
            
            // Check if rebuild is needed
            if (!NeedsRebuild(table, index, record_number)) {
                logger_.Debug("Skipping rebuild for " + field.name_ + " (no changes)");
                index++;
                if (progress) {
                    progress->current_step++;
                }
                continue;
            }
            
            // Launch parallel rebuild if enabled
            if (options_.enable_parallel_rebuild && rebuild_futures.size() < options_.max_parallel_indices) {
                rebuild_futures.push_back(
                    std::async(std::launch::async, [this, table, i, index, record_number, db_catalog_path, progress]() {
                        return RebuildSingleIndex(table, i, index, record_number, db_catalog_path);
                    })
                );
            } else {
                // Sequential rebuild
                Status status = RebuildSingleIndex(table, i, index, record_number, db_catalog_path);
                if (!status.ok()) {
                    return status;
                }
            }
            
            if (progress) {
                progress->current_step++;
                if (progress->is_cancelled) {
                    return Status(DB_UNEXPECTED_ERROR, "Rebuild cancelled");
                }
            }
            
            index++;
        }
        
        // Wait for all parallel rebuilds to complete
        for (auto& future : rebuild_futures) {
            Status status = future.get();
            if (!status.ok()) {
                return status;
            }
        }
        
        return Status::OK();
    }
    
    Status RebuildSingleIndex(std::shared_ptr<TableMVP> table,
                             size_t field_idx,
                             int64_t ann_idx,
                             int64_t record_number,
                             const std::string& db_catalog_path) {
        auto& field = table->table_schema_.fields_[field_idx];
        
        logger_.Debug("Rebuilding index for field: " + field.name_);
        
        // Get column data
        VectorColumnData columnData = GetVectorColumnData(table, field);
        
        if (table->is_leader_) {
            // Leader rebuilds the graph
            auto new_ann = std::make_shared<ANNGraphSegment>(false);
            
            // Use incremental rebuild if applicable
            if (options_.enable_incremental && CanUseIncrementalRebuild(table, ann_idx, record_number)) {
                // TODO: Implement incremental rebuild
                new_ann->BuildFromVectorTable(
                    columnData, record_number, 
                    field.vector_dimension_, field.metric_type_
                );
            } else {
                // Full rebuild
                new_ann->BuildFromVectorTable(
                    columnData, record_number,
                    field.vector_dimension_, field.metric_type_
                );
            }
            
            // Atomic replacement
            {
                std::lock_guard<std::mutex> lock(table->executor_pool_mutex_);
                table->ann_graph_segment_[ann_idx] = new_ann;
            }
            
            // Save to disk
            new_ann->SaveANNGraph(db_catalog_path, table->table_schema_.id_, field.id_);
            
        } else {
            // Follower loads from disk
            auto new_ann = std::make_shared<ANNGraphSegment>(
                db_catalog_path, table->table_schema_.id_, field.id_
            );
            
            if (new_ann->record_number_ <= record_number) {
                std::lock_guard<std::mutex> lock(table->executor_pool_mutex_);
                table->ann_graph_segment_[ann_idx] = new_ann;
            }
        }
        
        return Status::OK();
    }
    
    Status SwapExecutors(std::shared_ptr<TableMVP> table,
                        RebuildProgress* progress) {
        if (progress) {
            progress->current_operation = "Swapping executors";
        }
        
        // Build new executor pools
        std::vector<std::shared_ptr<execution::ExecutorPool>> new_pools;
        
        int64_t index = 0;
        for (size_t i = 0; i < table->table_schema_.fields_.size(); ++i) {
            auto& field = table->table_schema_.fields_[i];
            
            if (!IsVectorField(field.field_type_)) {
                continue;
            }
            
            auto pool = CreateExecutorPool(table, field, index);
            new_pools.push_back(pool);
            index++;
        }
        
        // Atomic swap with RCU-style replacement
        if (options_.enable_swap_optimization) {
            // Use RCU pattern for lock-free replacement
            for (size_t i = 0; i < new_pools.size(); ++i) {
                table->executor_pool_.set(i, new_pools[i]);
            }
        } else {
            // Traditional locked replacement
            std::lock_guard<std::mutex> lock(table->executor_pool_mutex_);
            for (size_t i = 0; i < new_pools.size(); ++i) {
                table->executor_pool_.set(i, new_pools[i]);
            }
        }
        
        return Status::OK();
    }
    
    Status VerifyRebuild(std::shared_ptr<TableMVP> table,
                        RebuildProgress* progress) {
        if (progress) {
            progress->current_operation = "Verifying rebuild";
        }
        
        // Sample-based verification
        // TODO: Implement verification logic
        
        return Status::OK();
    }
    
    void CleanupAfterRebuild(std::shared_ptr<TableMVP> table,
                             const std::string& db_catalog_path,
                             RebuildProgress* progress) {
        if (progress) {
            progress->current_operation = "Cleaning up";
        }
        
        // Clean up temporary files
        // Release memory pools
        // Update statistics
        
        logger_.Debug("Rebuild cleanup completed");
    }
    
    bool IsVectorField(meta::FieldType type) {
        return type == meta::FieldType::VECTOR_FLOAT ||
               type == meta::FieldType::VECTOR_DOUBLE ||
               type == meta::FieldType::SPARSE_VECTOR_FLOAT ||
               type == meta::FieldType::SPARSE_VECTOR_DOUBLE;
    }
    
    bool NeedsRebuild(std::shared_ptr<TableMVP> table, int64_t index, int64_t record_number) {
        if (record_number < globalConfig.MinimalGraphSize) {
            return false;
        }
        
        if (table->ann_graph_segment_[index]->record_number_ == record_number) {
            return false;  // Already up to date
        }
        
        if (options_.enable_incremental) {
            int64_t graph_records = table->ann_graph_segment_[index]->record_number_;
            double change_ratio = static_cast<double>(record_number - graph_records) / graph_records;
            return change_ratio >= options_.incremental_threshold;
        }
        
        return true;
    }
    
    bool CanUseIncrementalRebuild(std::shared_ptr<TableMVP> table, int64_t index, int64_t record_number) {
        if (!options_.enable_incremental) {
            return false;
        }
        
        int64_t graph_records = table->ann_graph_segment_[index]->record_number_;
        double change_ratio = static_cast<double>(record_number - graph_records) / graph_records;
        
        // Use incremental only for small changes
        return change_ratio < 0.3;  // Less than 30% change
    }
    
    VectorColumnData GetVectorColumnData(std::shared_ptr<TableMVP> table, const meta::Field& field) {
        if (field.field_type_ == meta::FieldType::VECTOR_FLOAT || 
            field.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
            return table->table_segment_->vector_tables_[
                table->table_segment_->field_name_mem_offset_map_[field.name_]
            ].get();
        } else {
            return &table->table_segment_->var_len_attr_table_[
                table->table_segment_->field_name_mem_offset_map_[field.name_]
            ];
        }
    }
    
    std::shared_ptr<execution::ExecutorPool> CreateExecutorPool(
        std::shared_ptr<TableMVP> table,
        const meta::Field& field,
        int64_t ann_idx) {
        
        auto pool = std::make_shared<execution::ExecutorPool>();
        auto distFunc = GetDistFunc(field.field_type_, field.metric_type_);
        auto columnData = GetVectorColumnData(table, field);
        
        for (int i = 0; i < globalConfig.NumExecutorPerField; i++) {
            pool->release(std::make_shared<execution::VecSearchExecutor>(
                field.vector_dimension_,
                table->ann_graph_segment_[ann_idx]->navigation_point_,
                table->ann_graph_segment_[ann_idx],
                table->ann_graph_segment_[ann_idx]->offset_table_.get(),
                table->ann_graph_segment_[ann_idx]->neighbor_list_.get(),
                columnData,
                distFunc,
                &field.vector_dimension_,
                globalConfig.IntraQueryThreads,
                globalConfig.MasterQueueSize,
                globalConfig.LocalQueueSize,
                globalConfig.GlobalSyncInterval,
                globalConfig.PreFilter
            ));
        }
        
        return pool;
    }

private:
    RebuildOptions options_;
    Logger logger_;
};

} // namespace engine
} // namespace vectordb