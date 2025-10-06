#include "db/table.hpp"

#include <omp.h>

#include <numeric>

#include "db/catalog/meta_types.hpp"

#include "config/config.hpp"

#include "db/table_segment_dynamic_simple.hpp"

namespace vectordb {

extern Config globalConfig;

namespace engine {

Table::Table(meta::TableSchema &table_schema,
                   const std::string &db_catalog_path,
                   int64_t init_table_scale,
                   bool is_leader,
                   std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service,
                   std::unordered_map<std::string, std::string> &headers)
    : table_schema_(table_schema),
      // executors_num_(executors_num),
      table_segment_(nullptr),
      is_leader_(is_leader) {
  embedding_service_ = embedding_service;
  db_catalog_path_ = db_catalog_path;
  // Construct field name to field type map.
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    field_name_field_type_map_[table_schema_.fields_[i].name_] =
        table_schema_.fields_[i].field_type_;
    field_name_metric_type_map_[table_schema_.fields_[i].name_] =
        table_schema_.fields_[i].metric_type_;
  }

  // Load the table data from disk.
  // Use dynamic configuration instead of hardcoded capacity
  int64_t actual_capacity = DynamicConfigManager::GetInitialCapacity(init_table_scale);
  
  if (actual_capacity != init_table_scale) {
    Logger logger;
    logger.Info("Using dynamic capacity " + std::to_string(actual_capacity) + 
                " instead of " + std::to_string(init_table_scale));
  }
  
  table_segment_ = std::make_shared<TableSegmentMVP>(
      table_schema, db_catalog_path, actual_capacity, embedding_service_);

  // Replay operations in write ahead log.
  // Disable predictive expansion during WAL recovery to prevent deadlocks
  bool original_predictive_setting = table_segment_->IsPredictiveExpansionEnabled();
  table_segment_->EnablePredictiveExpansion(false);

  wal_ = std::make_shared<WriteAheadLog>(db_catalog_path_, table_schema.id_, is_leader_);
  wal_->Replay(table_schema, field_name_field_type_map_, table_segment_, headers);

  // Re-enable predictive expansion after WAL recovery is complete
  // Add a cooldown period to prevent immediate expansion after restart
  table_segment_->EnablePredictiveExpansion(original_predictive_setting);
  table_segment_->SetPredictiveExpansionCooldown(10.0); // 10 second cooldown after restart

  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    auto fType = table_schema_.fields_[i].field_type_;
    auto mType = table_schema_.fields_[i].metric_type_;
    if (fType == meta::FieldType::VECTOR_FLOAT ||
        fType == meta::FieldType::VECTOR_DOUBLE ||
        fType == meta::FieldType::SPARSE_VECTOR_FLOAT ||
        fType == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
      VectorColumnData columnData;
      if (fType == meta::FieldType::VECTOR_FLOAT || fType == meta::FieldType::VECTOR_DOUBLE) {
        columnData = table_segment_
                         ->vector_tables_[table_segment_->field_name_mem_offset_map_
                                              [table_schema_.fields_[i].name_]]->GetData();
      } else {
        // sparse vector
        columnData = &table_segment_
                          ->var_len_attr_table_[table_segment_->field_name_mem_offset_map_
                                                    [table_schema_.fields_[i].name_]];
      }
      // Load the ann graph from disk.
      ann_graph_segment_.push_back(
          std::make_shared<vectordb::engine::ANNGraphSegment>(
              db_catalog_path, table_schema_.id_,
              table_schema_.fields_[i].id_));

      // Construct the executor.
      auto distFunc = GetDistFunc(fType, mType);

      auto pool = std::make_shared<execution::ExecutorPool>();
      for (int executorIdx = 0; executorIdx < globalConfig.NumExecutorPerField;
           executorIdx++) {
        pool->release(std::make_shared<execution::VecSearchExecutor>(
            table_schema_.fields_[i].vector_dimension_,
            ann_graph_segment_.back()->navigation_point_,
            ann_graph_segment_.back(), ann_graph_segment_.back()->offset_table_.get(),
            ann_graph_segment_.back()->neighbor_list_.get(),
            columnData,
            distFunc,
            &table_schema_.fields_[i].vector_dimension_,
            globalConfig.IntraQueryThreads,
            globalConfig.MasterQueueSize,
            globalConfig.LocalQueueSize,
            globalConfig.GlobalSyncInterval,
            globalConfig.PreFilter,
            table_schema_.fields_[i].name_));
      }
      executor_pool_.push_back(pool);
    }
  }
}

Status Table::Rebuild(const std::string &db_catalog_path) {
  // Limit how many threads rebuild takes.
  omp_set_num_threads(globalConfig.RebuildThreads);
  logger_.Debug("Rebuild table segment with threads: " + std::to_string(globalConfig.RebuildThreads));

  // CRITICAL: Set flag to prevent resize during ANN graph rebuild
  // This prevents Insert from resizing vector_tables_ while we're reading them
  table_segment_->SetIndexRebuildInProgress(true);

  // Get the current record number.
  int64_t record_number = table_segment_->record_number_;

  // Only leader sync table to disk.
  if (is_leader_) {
    // Write the table data to disk.
    logger_.Debug("Save table segment.");
    table_segment_->SaveTableSegment(table_schema_, db_catalog_path);

    // Clean up old WAL files.
    wal_->CleanUpOldFiles();
  }

  int64_t index = 0;
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    auto fType = table_schema_.fields_[i].field_type_;
    auto mType = table_schema_.fields_[i].metric_type_;

    if (fType == meta::FieldType::VECTOR_FLOAT ||
        fType == meta::FieldType::VECTOR_DOUBLE ||
        fType == meta::FieldType::SPARSE_VECTOR_FLOAT ||
        fType == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
      // Check if ann_graph_segment_[index] is valid before accessing
      if (ann_graph_segment_[index] && 
          (ann_graph_segment_[index]->record_number_ == record_number ||
           record_number < globalConfig.MinimalGraphSize)) {
        // No need to rebuild the ann graph.
        logger_.Debug("Skip rebuild ANN graph for attribute: " + table_schema_.fields_[i].name_);
        ++index;
        continue;
      }

      VectorColumnData columnData;
      if (fType == meta::FieldType::VECTOR_FLOAT || fType == meta::FieldType::VECTOR_DOUBLE) {
        columnData = table_segment_
                         ->vector_tables_[table_segment_->field_name_mem_offset_map_
                                              [table_schema_.fields_[i].name_]]->GetData();
      } else {
        // sparse vector
        columnData = &table_segment_
                          ->var_len_attr_table_[table_segment_->field_name_mem_offset_map_
                                                    [table_schema_.fields_[i].name_]];
      }

      // Rebuild the ann graph.
      logger_.Debug("Rebuild ANN graph for attribute: " + table_schema_.fields_[i].name_);
      if (is_leader_) {
        // For leader, rebuild the ann graph.
        auto new_ann = std::make_shared<vectordb::engine::ANNGraphSegment>(false);
        new_ann->BuildFromVectorTable(
            columnData,
            record_number, table_schema_.fields_[i].vector_dimension_,
            mType);
        std::shared_ptr<vectordb::engine::ANNGraphSegment> ann_ptr =
            ann_graph_segment_[index];
        ann_graph_segment_[index] = new_ann;
        // Write the ANN graph to disk.
        logger_.Debug("Save ANN graph segment.");
        ann_graph_segment_[index]->SaveANNGraph(
            db_catalog_path, table_schema_.id_, table_schema_.fields_[i].id_);
      } else {
        // For follower, directly reload the ann graph from disk.
        auto new_ann = std::make_shared<vectordb::engine::ANNGraphSegment>(
            db_catalog_path, table_schema_.id_, table_schema_.fields_[i].id_);
        // Check if the ann graph has went ahead of the table. If so, skip.
        if (new_ann->record_number_ > record_number) {
          logger_.Debug("Skip sync ANN graph for attribute: " + table_schema_.fields_[i].name_);
          ++index;
          continue;
        }
        std::shared_ptr<vectordb::engine::ANNGraphSegment> ann_ptr =
            ann_graph_segment_[index];
        ann_graph_segment_[index] = new_ann;
      }

      // Replace the executors.
      auto pool = std::make_shared<execution::ExecutorPool>();
      auto distFunc = GetDistFunc(fType, mType);

      for (int executorIdx = 0; executorIdx < globalConfig.NumExecutorPerField;
           executorIdx++) {
        pool->release(std::make_shared<execution::VecSearchExecutor>(
            table_schema_.fields_[i].vector_dimension_,
            ann_graph_segment_[index]->navigation_point_,
            ann_graph_segment_[index],
            ann_graph_segment_[index]->offset_table_.get(),
            ann_graph_segment_[index]->neighbor_list_.get(),
            columnData,
            distFunc,
            &table_schema_.fields_[i].vector_dimension_,
            globalConfig.IntraQueryThreads,
            globalConfig.MasterQueueSize,
            globalConfig.LocalQueueSize,
            globalConfig.GlobalSyncInterval,
            globalConfig.PreFilter,
            table_schema_.fields_[i].name_));
      }
      std::unique_lock<std::mutex> lock(executor_pool_mutex_);
      executor_pool_.set(index, pool);
      lock.unlock();

      ++index;
    }
  }

  // CRITICAL: Clear flag to allow resize operations again
  table_segment_->SetIndexRebuildInProgress(false);

  logger_.Debug("Rebuild done.");
  return Status::OK();
}

// ============================================================================
// Smart Rebuild Implementation
// ============================================================================

Status Table::SmartRebuild(const std::string &db_catalog_path) {
  // Mutex protection: prevent concurrent rebuild and compaction
  std::lock_guard<std::mutex> lock(rebuild_compact_mutex_);
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  // Get current state
  int64_t record_number = table_segment_->record_number_;
  double deleted_ratio = table_segment_->GetDeletedRatio();
  
  // Get old record count from first ANN graph segment
  int64_t old_record_number = 0;
  if (!ann_graph_segment_.empty() && ann_graph_segment_[0]) {
    old_record_number = ann_graph_segment_[0]->record_number_;
  }
  
  int64_t delta = record_number - old_record_number;
  
  logger_.Info("[SmartRebuild] Table: " + table_schema_.name_ + 
               ", Records: " + std::to_string(record_number) + 
               ", Delta: " + std::to_string(delta) + 
               ", Deletion ratio: " + std::to_string(deleted_ratio * 100) + "%");
  
  // === Decision Tree: Handle different scenarios ===
  
  // Scenario 1: No changes, skip rebuild
  if (ShouldSkipRebuild(old_record_number, record_number, deleted_ratio)) {
    logger_.Info("[SmartRebuild] Skipping rebuild - no significant changes");
    return Status::OK();
  }
  
  // Scenario 2: HIGH DELETION RATIO (> threshold) - MUST compact first
  // This is critical for maintaining search quality
  if (deleted_ratio > globalConfig.CompactionBeforeRebuildThreshold) {
    logger_.Warning("[SmartRebuild] High deletion ratio (" + 
                    std::to_string(deleted_ratio * 100) + "%) detected!");
    logger_.Info("[SmartRebuild] STEP 1: Compacting to remove deleted nodes...");
    
    // Step 1: Compact to physically remove deleted nodes
    auto compact_status = table_segment_->CompactSegment();
    if (!compact_status.ok()) {
      logger_.Error("[SmartRebuild] Compaction failed: " + compact_status.message());
      return compact_status;
    }
    
    // Step 2: Save compacted data
    if (is_leader_) {
      table_segment_->SaveTableSegment(table_schema_, db_catalog_path, true);
    }
    
    logger_.Info("[SmartRebuild] STEP 2: Compaction completed, now performing FULL rebuild...");
    logger_.Info("[SmartRebuild] Reason: Node IDs changed after compaction, must rebuild entire graph");
    
    // Step 3: MUST do full rebuild (node IDs changed)
    auto rebuild_status = FullRebuild(db_catalog_path);
    
    // Reset incremental counter after compaction
    incremental_rebuild_count_.store(0);
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    logger_.Info("[SmartRebuild] Compact + Full rebuild completed in " + 
                 std::to_string(duration.count()) + "ms");
    
    return rebuild_status;
  }
  
  // Scenario 3: Incremental rebuild (if enabled and conditions met)
  bool use_incremental = 
    globalConfig.EnableIncrementalRebuild &&
    delta > 0 &&
    delta < globalConfig.IncrementalThreshold &&
    incremental_rebuild_count_.load() < globalConfig.FullRebuildInterval;
  
  if (use_incremental) {
    logger_.Info("[SmartRebuild] Using INCREMENTAL rebuild for " + 
                 std::to_string(delta) + " new nodes");
    logger_.Info("[SmartRebuild] Incremental count: " + 
                 std::to_string(incremental_rebuild_count_.load() + 1) + "/" + 
                 std::to_string(globalConfig.FullRebuildInterval));
    
    auto status = IncrementalRebuild(db_catalog_path);
    if (status.ok()) {
      incremental_rebuild_count_.fetch_add(1);
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    logger_.Info("[SmartRebuild] Incremental rebuild completed in " + 
                 std::to_string(duration.count()) + "ms");
    
    return status;
  }
  
  // Scenario 4: Full rebuild (default fallback)
  logger_.Info("[SmartRebuild] Using FULL rebuild");
  if (delta >= globalConfig.IncrementalThreshold) {
    logger_.Info("[SmartRebuild] Reason: Delta (" + std::to_string(delta) + 
                 ") >= threshold (" + std::to_string(globalConfig.IncrementalThreshold) + ")");
  } else if (incremental_rebuild_count_.load() >= globalConfig.FullRebuildInterval) {
    logger_.Info("[SmartRebuild] Reason: Periodic full rebuild (every " + 
                 std::to_string(globalConfig.FullRebuildInterval) + " incremental rebuilds)");
  }
  
  auto status = FullRebuild(db_catalog_path);
  
  // Reset incremental counter after full rebuild
  incremental_rebuild_count_.store(0);
  
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  logger_.Info("[SmartRebuild] Full rebuild completed in " + 
               std::to_string(duration.count()) + "ms");
  
  return status;
}

bool Table::ShouldSkipRebuild(int64_t old_count, int64_t new_count, double deleted_ratio) {
  int64_t delta = new_count - old_count;
  
  // No new data
  if (delta == 0) {
    return true;
  }
  
  // New data is too small (< 1% of total)
  if (new_count > 0) {
    float delta_ratio = static_cast<float>(delta) / static_cast<float>(new_count);
    if (delta_ratio < 0.01f) {
      logger_.Debug("[ShouldSkipRebuild] Delta ratio " + std::to_string(delta_ratio) + " < 0.01");
      return true;
    }
  }
  
  // High deletion ratio - don't skip (need to compact)
  if (deleted_ratio > globalConfig.CompactionBeforeRebuildThreshold) {
    return false;
  }
  
  return false;
}

Status Table::IncrementalRebuild(const std::string &db_catalog_path) {
  logger_.Info("[IncrementalRebuild] Starting incremental rebuild...");
  
  // Note: Incremental rebuild is a placeholder for now
  // Full implementation would require:
  // 1. Insert new nodes into existing graph
  // 2. Filter deleted nodes when finding neighbors
  // 3. Add bidirectional links
  // 4. Update navigation point if needed
  
  // For now, fall back to full rebuild
  logger_.Warning("[IncrementalRebuild] Incremental rebuild not fully implemented yet, using full rebuild");
  return FullRebuild(db_catalog_path);
}

Status Table::FullRebuild(const std::string &db_catalog_path) {
  // This is essentially the same as the original Rebuild() method
  return Rebuild(db_catalog_path);
}

bool Table::ShouldSaveToDisk() {
  total_rebuild_count_.fetch_add(1);
  int count = total_rebuild_count_.load();
  
  // Save every N rebuilds
  if (count % globalConfig.RebuildSaveInterval == 0) {
    logger_.Debug("[ShouldSaveToDisk] Saving due to rebuild count: " + std::to_string(count));
    return true;
  }
  
  // Or save every N seconds
  auto now = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_save_time_).count();
  
  if (elapsed >= globalConfig.RebuildSaveIntervalSeconds) {
    logger_.Debug("[ShouldSaveToDisk] Saving due to time elapsed: " + std::to_string(elapsed) + "s");
    last_save_time_ = now;
    return true;
  }
  
  return false;
}

// ============================================================================
// End of Smart Rebuild Implementation
// ============================================================================

Status Table::SwapExecutors() {
  // Get the current record number.
  int64_t record_number = table_segment_->record_number_;

  int64_t index = 0;
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    auto fType = table_schema_.fields_[i].field_type_;
    auto mType = table_schema_.fields_[i].metric_type_;

    if (fType == meta::FieldType::VECTOR_FLOAT ||
        fType == meta::FieldType::VECTOR_DOUBLE ||
        fType == meta::FieldType::SPARSE_VECTOR_FLOAT ||
        fType == meta::FieldType::SPARSE_VECTOR_DOUBLE) {

      VectorColumnData columnData;
      if (fType == meta::FieldType::VECTOR_FLOAT || fType == meta::FieldType::VECTOR_DOUBLE) {
        columnData = table_segment_
                         ->vector_tables_[table_segment_->field_name_mem_offset_map_
                                              [table_schema_.fields_[i].name_]]->GetData();
      } else {
        // sparse vector
        columnData = &table_segment_
                          ->var_len_attr_table_[table_segment_->field_name_mem_offset_map_
                                                    [table_schema_.fields_[i].name_]];
      }

      // Rebuild the ann graph.
      logger_.Debug("Swap executors for attribute: " + table_schema_.fields_[i].name_);

      // Replace the executors.
      auto pool = std::make_shared<execution::ExecutorPool>();
      auto distFunc = GetDistFunc(fType, mType);

      for (int executorIdx = 0; executorIdx < globalConfig.NumExecutorPerField;
           executorIdx++) {
        pool->release(std::make_shared<execution::VecSearchExecutor>(
            table_schema_.fields_[i].vector_dimension_,
            ann_graph_segment_[index]->navigation_point_,
            ann_graph_segment_[index],
            ann_graph_segment_[index]->offset_table_.get(),
            ann_graph_segment_[index]->neighbor_list_.get(),
            columnData,
            distFunc,
            &table_schema_.fields_[i].vector_dimension_,
            globalConfig.IntraQueryThreads,
            globalConfig.MasterQueueSize,
            globalConfig.LocalQueueSize,
            globalConfig.GlobalSyncInterval,
            globalConfig.PreFilter,
            table_schema_.fields_[i].name_));
      }
      std::unique_lock<std::mutex> lock(executor_pool_mutex_);
      executor_pool_.set(index, pool);
      lock.unlock();

      ++index;
    }
  }

  logger_.Debug("Swap executors done.");
  return Status::OK();
}

Status Table::Release() {
  // Release the table segment.
  return table_segment_->Release();
}

Status Table::Compact(double threshold) {
  // Mutex protection: prevent concurrent rebuild and compaction
  std::lock_guard<std::mutex> lock(rebuild_compact_mutex_);
  
  if (table_segment_ == nullptr) {
    return Status(DB_UNEXPECTED_ERROR, "Table segment is null");
  }
  
  if (!table_segment_->NeedsCompaction(threshold)) {
    return Status(DB_SUCCESS, "Compaction not needed (threshold: " + 
                 std::to_string(threshold) + ")");
  }
  
  logger_.Info("[Compact] Starting compaction for table " + table_schema_.name_ + 
               " (deleted ratio: " + std::to_string(table_segment_->GetDeletedRatio() * 100) + "%)");
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  // Step 1: Compact the segment (physically remove deleted records)
  auto status = table_segment_->CompactSegment();
  if (!status.ok()) {
    logger_.Error("[Compact] Compaction failed for table " + table_schema_.name_ + ": " + status.message());
    return status;
  }
  
  // Step 2: Save the compacted segment to disk
  if (is_leader_) {
    status = table_segment_->SaveTableSegment(table_schema_, db_catalog_path_, true);
    if (!status.ok()) {
      logger_.Error("[Compact] Failed to save compacted segment for table " + table_schema_.name_);
      return status;
    }
  }
  
  auto compact_time = std::chrono::high_resolution_clock::now();
  auto compact_duration = std::chrono::duration_cast<std::chrono::milliseconds>(compact_time - start_time);
  logger_.Info("[Compact] Compaction completed in " + std::to_string(compact_duration.count()) + "ms");
  
  // Step 3: CRITICAL - Force full rebuild after compaction
  // Reason: Compaction changes node IDs, making the ANN graph invalid
  if (globalConfig.ForceFullRebuildAfterCompaction) {
    logger_.Warning("[Compact] Node IDs changed after compaction - FORCING FULL REBUILD");
    logger_.Info("[Compact] This is required to maintain data consistency");
    
    auto rebuild_status = FullRebuild(db_catalog_path_);
    if (!rebuild_status.ok()) {
      logger_.Error("[Compact] Full rebuild after compaction failed: " + rebuild_status.message());
      return rebuild_status;
    }
    
    // Reset incremental rebuild counter
    incremental_rebuild_count_.store(0);
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    logger_.Info("[Compact] Compaction + Full rebuild completed in " + 
                 std::to_string(total_duration.count()) + "ms");
  } else {
    logger_.Warning("[Compact] ForceFullRebuildAfterCompaction is disabled - ANN graph may be INVALID!");
  }
  
  return Status::OK();
}

bool Table::NeedsCompaction(double threshold) const {
  if (table_segment_ == nullptr) {
    return false;
  }
  return table_segment_->NeedsCompaction(threshold);
}

Status Table::Insert(vectordb::Json &record, std::unordered_map<std::string, std::string> &headers, bool upsert) {
  int64_t wal_id =
      wal_->WriteEntry(upsert ? LogEntryType::UPSERT : LogEntryType::INSERT, record.DumpToString());
  return table_segment_->Insert(table_schema_, record, wal_id, headers, upsert);
}

Status Table::InsertPrepare(vectordb::Json &pks, vectordb::Json &result) {
  return table_segment_->InsertPrepare(table_schema_, pks, result);
}

Status Table::Delete(
    vectordb::Json &records,
    const std::string &filter,
    std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes) {
  
  // Log deletion request at table level
  size_t pk_count = records.GetSize();
  std::string log_msg = "[Table] Processing deletion request for table=" + table_schema_.name_;
  if (pk_count > 0) {
    log_msg += ", primaryKeys=" + std::to_string(pk_count) + " items";
  }
  if (!filter.empty()) {
    log_msg += ", filter=[" + filter + "]";
  }
  logger_.Debug(log_msg);
  
  // Get deletion statistics before operation
  double pre_deletion_ratio = table_segment_->GetDeletedRatio();
  size_t pre_record_count = table_segment_->record_number_;
  size_t pre_deleted_count = static_cast<size_t>(pre_deletion_ratio * pre_record_count);
  
  logger_.Debug("[Table] Pre-deletion stats: total_records=" + std::to_string(pre_record_count) + 
               ", deleted_records=" + std::to_string(pre_deleted_count) + 
               ", deletion_ratio=" + std::to_string(pre_deletion_ratio));
  
  vectordb::Json delete_wal;
  delete_wal.LoadFromString("{}");
  delete_wal.SetObject("pk", records);
  delete_wal.SetString("filter", filter);
  
  logger_.Debug("[Table] Writing deletion to WAL");
  int64_t wal_id =
      wal_->WriteEntry(LogEntryType::DELETE, delete_wal.DumpToString());
  logger_.Debug("[Table] WAL entry written with id=" + std::to_string(wal_id));
  
  logger_.Debug("[Table] Forwarding deletion to table segment");
  auto status = table_segment_->Delete(records, filter_nodes, wal_id);
  
  if (status.ok()) {
    // Log post-deletion statistics
    double post_deletion_ratio = table_segment_->GetDeletedRatio();
    size_t post_record_count = table_segment_->record_number_;
    size_t post_deleted_count = static_cast<size_t>(post_deletion_ratio * post_record_count);
    
    logger_.Debug("[Table] Post-deletion stats: total_records=" + std::to_string(post_record_count) + 
                 ", deleted_records=" + std::to_string(post_deleted_count) + 
                 ", deletion_ratio=" + std::to_string(post_deletion_ratio));
    logger_.Debug("[Table] TableSegment deletion completed successfully: " + status.message());
    
    // Eager compaction: immediately compact if enabled and threshold exceeded
    bool eager = vectordb::globalConfig.EagerCompactionOnDelete.load();
    double threshold = vectordb::globalConfig.CompactionThreshold.load();
    int min_deleted_vectors = vectordb::globalConfig.MinDeletedVectorsForEagerCompaction.load();
    size_t deleted_count = table_segment_->GetDeletedRatio() * table_segment_->GetRecordCount();

    // Check both percentage threshold and minimum deleted vector count
    bool should_compact = eager && post_deletion_ratio >= threshold && deleted_count >= min_deleted_vectors;

    if (should_compact) {
      logger_.Info("[Table] Eager compaction enabled: deletion ratio " + std::to_string(post_deletion_ratio) +
                   " >= threshold " + std::to_string(threshold) + " and deleted vectors " +
                   std::to_string(deleted_count) + " >= minimum " + std::to_string(min_deleted_vectors) +
                   ", flushing WAL and compacting now...");
      // Ensure WAL is flushed so compaction is crash-safe
      auto wal_status = FlushWAL();
      if (!wal_status.ok()) {
        logger_.Warning("[Table] WAL flush failed before eager compaction: " + wal_status.message());
      }
      auto compact_status = Compact(threshold);
      if (!compact_status.ok()) {
        logger_.Warning("[Table] Eager compaction failed: " + compact_status.message());
      }
    } else if (eager) {
      logger_.Debug("[Table] Eager compaction skipped: deletion ratio " + std::to_string(post_deletion_ratio) +
                   " >= threshold " + std::to_string(threshold) + " but deleted vectors " +
                   std::to_string(deleted_count) + " < minimum " + std::to_string(min_deleted_vectors));
    } else {
      // Trigger incremental compaction if available (background), using a fixed 20% hint as before
      if (compactor_) {
        if (post_deletion_ratio > 0.2) {
          logger_.Debug("[Table] Deletion ratio " + std::to_string(post_deletion_ratio) +
                       " exceeds 20% threshold, triggering incremental compaction");
          compactor_->TriggerCompaction();
        } else {
          logger_.Debug("[Table] Deletion ratio " + std::to_string(post_deletion_ratio) +
                       " below 20% threshold, no compaction needed");
        }
      } else {
        logger_.Debug("[Table] No compactor available, skipping compaction check");
      }
    }
  } else {
    logger_.Error("[Table] TableSegment deletion failed: " + status.message());
  }
  
  return status;
}

Status Table::Search(const std::string &field_name,
                        std::vector<std::string> &query_fields,
                        int64_t query_dimension,
                        const VectorPtr query_data,
                        const int64_t limit,
                        vectordb::Json &result,
                        std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
                        bool with_distance,
                        std::vector<vectordb::engine::execution::FacetExecutor> &facet_executors,
                        vectordb::Json &facets) {
  // Check if rebuild is in progress - prevent concurrent access to data structures
  if (table_segment_->IsIndexRebuildInProgress()) {
    return Status(DB_UNEXPECTED_ERROR,
                 "Index rebuild is in progress. Please retry the query after rebuild completes.");
  }

  // Check if field_name exists.
  if (field_name_field_type_map_.find(field_name) == field_name_field_type_map_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "Field name not found: " + field_name);
  }
  for (auto field : query_fields) {
    if (field_name_field_type_map_.find(field) == field_name_field_type_map_.end()) {
      return Status(DB_UNEXPECTED_ERROR, "Field name not found: " + field);
    }
  }

  // Get the field data type. If the type is not VECTOR, return error.
  auto field_type = field_name_field_type_map_[field_name];
  if (field_type != meta::FieldType::VECTOR_FLOAT &&
      field_type != meta::FieldType::VECTOR_DOUBLE &&
      field_type != meta::FieldType::SPARSE_VECTOR_FLOAT &&
      field_type != meta::FieldType::SPARSE_VECTOR_DOUBLE) {
    return Status(USER_ERROR, "Field type is not vector.");
  }

  if (std::holds_alternative<DenseVectorPtr>(query_data) && field_type != meta::FieldType::VECTOR_FLOAT &&
          field_type != meta::FieldType::VECTOR_DOUBLE ||
      std::holds_alternative<SparseVectorPtr>(query_data) && field_type != meta::FieldType::SPARSE_VECTOR_FLOAT &&
          field_type != meta::FieldType::SPARSE_VECTOR_DOUBLE) {
    return Status(USER_ERROR, "Query vector and field vector type must be both dense or sparse");
  }

  auto metric_type = field_name_metric_type_map_[field_name];

  // normalize the query data
  VectorPtr updatedQueryData = query_data;
  std::vector<DenseVectorElement> denseVec;
  auto sparseVecPtr = std::make_shared<SparseVector>();
  if (metric_type == meta::MetricType::COSINE) {
    if (std::holds_alternative<DenseVectorPtr>(query_data)) {
      auto q = std::get<DenseVectorPtr>(query_data);
      denseVec.resize(query_dimension);
      denseVec.insert(denseVec.begin(), q, q + query_dimension);
      Normalize((DenseVectorPtr)(denseVec.data()), query_dimension);
      updatedQueryData = denseVec.data();
    } else if (std::holds_alternative<SparseVectorPtr>(query_data)) {
      *sparseVecPtr = *std::get<SparseVectorPtr>(query_data);
      Normalize(*sparseVecPtr);
      updatedQueryData = sparseVecPtr;
    }
  }

  // Get the field offset in the vector table.
  int64_t field_offset = table_segment_->vec_field_name_executor_pool_idx_map_[field_name];

  // [Note] the following invocation is wrong
  //   execution::RAIIVecSearchExecutor(executor_pool_[field_offset],
  //   executor_pool_[field_offset]->acquire);
  // because the value of executor_pool_[field_offset] could be different when
  // evaluating twice, which will result in memory leak or core dump
  std::unique_lock<std::mutex> lock(executor_pool_mutex_);
  auto pool = executor_pool_.at(field_offset);
  auto executor = execution::RAIIVecSearchExecutor(pool, pool->acquire());
  lock.unlock();

  // The query dimension needs to match the vector dimension.
  if (std::holds_alternative<DenseVectorPtr>(query_data) && query_dimension != executor.exec_->dimension_) {
    return Status(DB_UNEXPECTED_ERROR,
                  "Query dimension doesn't match the vector field dimension.");
  }

  // Search.
  int64_t result_num = 0;
  executor.exec_->Search(updatedQueryData, table_segment_.get(), limit,
                         filter_nodes, result_num);
  result_num = result_num > limit ? limit : result_num;

  // If facets are provided, only project if query_fields if not empty.
  if (query_fields.size() > 0 || facet_executors.size() == 0) {
    auto status =
        Project(query_fields, result_num, executor.exec_->search_result_, result,
                with_distance, executor.exec_->distance_);
    if (!status.ok()) {
      return status;
    }
  }

  if (facet_executors.size() > 0) {
    facets.LoadFromString("[]");
    for (auto &facet_executor : facet_executors) {
      facet_executor.Aggregate(
        table_segment_.get(),
        result_num,
        executor.exec_->search_result_,
        true,
        executor.exec_->distance_
      );
      vectordb::Json facet;
      facet_executor.Project(facet);
      facets.AddObjectToArray(std::move(facet));
    }
  }
  return Status::OK();
}

Status Table::SearchByAttribute(
    std::vector<std::string> &query_fields,
    vectordb::Json &primary_keys,
    std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
    const int64_t skip,
    const int64_t limit,
    vectordb::Json &projects,
    std::vector<vectordb::engine::execution::FacetExecutor> &facet_executors,
    vectordb::Json &facets) {
  // Check if rebuild is in progress - prevent concurrent access to data structures
  if (table_segment_->IsIndexRebuildInProgress()) {
    return Status(DB_UNEXPECTED_ERROR,
                 "Index rebuild is in progress. Please retry the query after rebuild completes.");
  }

  // TODO: create a separate pool for search by attribute.
  int64_t field_offset = 0;
  // [Note] the following invocation is wrong
  //   execution::RAIIVecSearchExecutor(executor_pool_[field_offset],
  //   executor_pool_[field_offset]->acquire);
  // because the value of executor_pool_[field_offset] could be different when
  // evaluating twice, which will result in memory leak or core dump
  std::unique_lock<std::mutex> lock(executor_pool_mutex_);
  auto pool = executor_pool_.at(field_offset);
  auto executor = execution::RAIIVecSearchExecutor(pool, pool->acquire());
  lock.unlock();

  // Search.
  int64_t result_num = 0;
  executor.exec_->SearchByAttribute(
      table_schema_,
      table_segment_.get(),
      skip,
      limit,
      primary_keys,
      filter_nodes,
      result_num);
  // If facets are provided, only project if query_fields if not empty.
  if (query_fields.size() > 0 || facet_executors.size() == 0) {
    auto status =
        Project(query_fields, result_num, executor.exec_->search_result_, projects,
                false, executor.exec_->distance_);
    if (!status.ok()) {
      return status;
    }
  }
  if (facet_executors.size() > 0) {
    facets.LoadFromString("[]");
    for (auto &facet_executor : facet_executors) {
      facet_executor.Aggregate(
        table_segment_.get(),
        result_num,
        executor.exec_->search_result_,
        false,
        executor.exec_->distance_
      );
      vectordb::Json facet;
      facet_executor.Project(facet);
      facets.AddObjectToArray(std::move(facet));
    }
  }
  return Status::OK();
}

Status Table::Project(
    std::vector<std::string> &query_fields,
    int64_t idlist_size,        // -1 means project all.
    std::vector<int64_t> &ids,  // doesn't matter if idlist_size is -1.
    vectordb::Json &result,
    bool with_distance,
    std::vector<double> &distances) {
  // Construct the result.
  result.LoadFromString("[]");
  // If query fields is empty, fill in with all fields.
  if (query_fields.size() == 0) {
    for (int i = 0; i < table_schema_.fields_.size(); ++i) {
      // Index fields are not responsed by default.
      if (table_schema_.fields_[i].is_index_field_) {
        continue;
      }
      query_fields.push_back(table_schema_.fields_[i].name_);
    }
  }

  // If idlist_size = -1, actually project everything from table.
  bool from_id_list = true;
  if (idlist_size == -1) {
    idlist_size = table_segment_->record_number_.load();
    from_id_list = false;
  }

  for (auto i = 0; i < idlist_size; ++i) {
    int64_t id = from_id_list ? ids[i] : i;

    // Skip deleted records when projecting all records
    if (!from_id_list && table_segment_->deleted_->test(id)) {
      continue;
    }

    vectordb::Json record;
    record.LoadFromString("{}");
    for (auto field : query_fields) {
      if (field_name_field_type_map_[field] == meta::FieldType::VECTOR_FLOAT ||
          field_name_field_type_map_[field] == meta::FieldType::VECTOR_DOUBLE) {
        // Vector field.
        vectordb::Json vector;
        vector.LoadFromString("[]");
        int64_t offset = table_segment_->field_name_mem_offset_map_[field];
        int64_t dim = table_segment_->vector_dims_[offset];
        for (int k = 0; k < dim; ++k) {
          vector.AddDoubleToArray(
              table_segment_->vector_tables_[offset]->GetData()[id * dim + k]);
        }
        record.SetObject(field, vector);
      } else if (field_name_field_type_map_[field] == meta::FieldType::SPARSE_VECTOR_FLOAT ||
                 field_name_field_type_map_[field] == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
        // Vector field.
        vectordb::Json vector;
        vector.LoadFromString("{}");
        int64_t offset = table_segment_->field_name_mem_offset_map_[field];
        auto vec = std::get<SparseVectorPtr>(table_segment_->var_len_attr_table_[offset][id]);
        record.SetObject(field, ToJson(*vec));
      } else {
        // Primitive field.
        auto offset = table_segment_->field_name_mem_offset_map_[field] +
                      id * table_segment_->primitive_offset_;
        switch (field_name_field_type_map_[field]) {
          case meta::FieldType::INT1: {
            int8_t *ptr = reinterpret_cast<int8_t *>(
                &table_segment_->attribute_table_->GetData()[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT2: {
            int16_t *ptr = reinterpret_cast<int16_t *>(
                &table_segment_->attribute_table_->GetData()[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT4: {
            int32_t *ptr = reinterpret_cast<int32_t *>(
                &table_segment_->attribute_table_->GetData()[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT8: {
            int64_t *ptr = reinterpret_cast<int64_t *>(
                &table_segment_->attribute_table_->GetData()[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::FLOAT: {
            float *ptr = reinterpret_cast<float *>(
                &table_segment_->attribute_table_->GetData()[offset]);
            record.SetDouble(field, (double)(*ptr));
            break;
          }
          case meta::FieldType::DOUBLE: {
            double *ptr = reinterpret_cast<double *>(
                &table_segment_->attribute_table_->GetData()[offset]);
            record.SetDouble(field, *ptr);
            break;
          }
          case meta::FieldType::BOOL: {
            bool *ptr = reinterpret_cast<bool *>(
                &table_segment_->attribute_table_->GetData()[offset]);
            record.SetBool(field, *ptr);
            break;
          }
          case meta::FieldType::STRING: {
            record.SetString(field,
                             std::get<std::string>(table_segment_->var_len_attr_table_[table_segment_->field_name_mem_offset_map_[field]][id]));
            break;
          }
          case meta::FieldType::JSON: {
            vectordb::Json json;
            json.LoadFromString(std::get<std::string>(table_segment_->var_len_attr_table_[table_segment_->field_name_mem_offset_map_[field]][id]));
            record.SetObject(field, json);
            break;
          }
          case meta::FieldType::GEO_POINT: {
            vectordb::Json geoPoint;
            double *lat = reinterpret_cast<double *>(
                &table_segment_->attribute_table_->GetData()[offset]);
            double *lon = reinterpret_cast<double *>(
                &table_segment_->attribute_table_->GetData()[offset + sizeof(double)]);
            geoPoint.LoadFromString("{\"latitude\": " + std::to_string(*lat) + ", \"longitude\": " + std::to_string(*lon) + "}");
            record.SetObject(field, geoPoint);
            break;
          }
          default:
            return Status(DB_UNEXPECTED_ERROR, "Unknown field type.");
        }
      }
    }
    // Add distance if needed.
    if (with_distance) {
      record.SetDouble("@distance", distances[i]);
    }
    result.AddObjectToArray(std::move(record));
  }
  return Status::OK();
}

Status Table::Dump(const std::string &db_catalog_path) {
  // Create the folder if not exist.
  auto table_dump_path = db_catalog_path + "/" + std::to_string(table_schema_.id_);
  if (!server::CommonUtil::CreateDirectory(table_dump_path).ok()) {
    return Status(DB_UNEXPECTED_ERROR, "Failed to create directory: " + table_dump_path);
  }
  // Get the current record number.
  int64_t record_number = table_segment_->record_number_;

  // Dump the table segment.
  logger_.Debug("Dump table segment.");
  auto segment_dump_status = table_segment_->SaveTableSegment(table_schema_, db_catalog_path, true);
  if (!segment_dump_status.ok()) {
    return segment_dump_status;
  }
  // Dump the ann graphs.
  int64_t index = 0;
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    auto fType = table_schema_.fields_[i].field_type_;
    auto mType = table_schema_.fields_[i].metric_type_;

    if (fType == meta::FieldType::VECTOR_FLOAT ||
        fType == meta::FieldType::VECTOR_DOUBLE ||
        fType == meta::FieldType::SPARSE_VECTOR_FLOAT ||
        fType == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
      if (record_number < globalConfig.MinimalGraphSize) {
        // No need to rebuild the ann graph.
        logger_.Debug("Skip dump ANN graph for attribute: " + table_schema_.fields_[i].name_);
        ++index;
        continue;
      }

      std::shared_ptr<vectordb::engine::ANNGraphSegment> ann_ptr = ann_graph_segment_[index];
      // Write the ANN graph to disk.
      logger_.Debug("Dump ANN graph segment.");
      ann_graph_segment_[index]->SaveANNGraph(db_catalog_path, table_schema_.id_, table_schema_.fields_[i].id_, true);

      ++index;
    }
  }
  return Status::OK();
}

size_t Table::GetRecordCount() {
  return table_segment_->GetRecordCount();
}

size_t Table::GetCapacity() {
  return table_segment_->size_limit_;
}

void Table::SetLeader(bool is_leader) {
  wal_->SetLeader(is_leader);
  is_leader_ = is_leader;
}

Table::~Table() {
  if (compactor_) {
    compactor_->Stop();
  }
}

void Table::EnableIncrementalCompaction(const CompactionConfig& config) {
  if (!compactor_) {
    compactor_ = std::make_unique<IncrementalCompactor>(config);
  } else {
    compactor_->UpdateConfig(config);
  }
  
  // Register the table segment for compaction
  if (table_segment_) {
    compactor_->RegisterSegment(table_segment_, table_schema_.name_);
  }
  
  // Start background compaction if configured
  if (config.background_compaction) {
    compactor_->Start();
  }
  
  logger_.Info("Incremental compaction enabled for table " + table_schema_.name_);
}

void Table::DisableIncrementalCompaction() {
  if (compactor_) {
    compactor_->Stop();
    compactor_->UnregisterSegment(table_schema_.name_);
    logger_.Info("Incremental compaction disabled for table " + table_schema_.name_);
  }
}

}  // namespace engine
}  // namespace vectordb
