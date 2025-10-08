#include "db/segment_manager.hpp"
#include <algorithm>
#include <execution>
#include <future>

namespace vectordb {
namespace engine {

Status SegmentManager::Insert(Json& record, std::unordered_map<std::string, std::string>& headers) {
    // Calculate which shard this record belongs to
    size_t shard_id = CalculateShardId(record);
    
    if (shard_id >= shards_.size()) {
        return Status(DB_UNEXPECTED_ERROR, "Invalid shard ID calculated");
    }
    
    // Get the appropriate segment from the shard
    auto segment = shards_[shard_id]->SelectSegmentForInsert(segment_config_.max_segment_size);
    
    // If no suitable segment, create a new one
    if (!segment) {
        segment = CreateNewSegment(shard_id);
    }
    
    // Lock the segment for writing
    std::unique_lock<std::shared_mutex> lock(segment->mutex);
    
    // Insert into the segment
    auto status = segment->segment->Insert(table_schema_, record, 0, headers, false);
    
    if (status.ok()) {
        segment->record_count.fetch_add(1);
        segment->last_modified = std::chrono::steady_clock::now();
        
        // Check if segment needs splitting
        if (segment_config_.auto_split && segment->NeedsSplit(segment_config_)) {
            // Schedule split in background
            std::thread([this, segment]() {
                PerformSplit(segment);
            }).detach();
        }
    }
    
    return status;
}

Status SegmentManager::BatchInsert(Json& records, std::unordered_map<std::string, std::string>& headers) {
    if (!records.IsArray()) {
        return Status(INVALID_PAYLOAD, "Records must be an array");
    }
    
    size_t record_count = records.GetSize();
    
    // Group records by shard
    std::unordered_map<size_t, std::vector<Json>> shard_records;
    
    for (size_t i = 0; i < record_count; ++i) {
        auto record = records.GetArrayElement(i);
        size_t shard_id = CalculateShardId(record);
        shard_records[shard_id].push_back(record);
    }
    
    // Insert records in parallel to different shards
    std::vector<std::future<Status>> futures;
    
    for (auto& [shard_id, shard_batch] : shard_records) {
        futures.push_back(std::async(std::launch::async, [this, &headers](size_t sid, std::vector<Json> batch) {
            for (auto& record : batch) {
                auto status = this->Insert(record, headers);
                if (!status.ok()) {
                    return status;
                }
            }
            return Status::OK();
        }, shard_id, shard_batch));
    }
    
    // Wait for all insertions to complete
    for (auto& future : futures) {
        auto status = future.get();
        if (!status.ok()) {
            return status;
        }
    }
    
    return Status::OK();
}

Status SegmentManager::Delete(Json& primary_keys, const std::string& filter) {
    // Parse filter if provided
    std::vector<vectordb::query::expr::ExprNodePtr> filter_nodes;
    // TODO: Parse filter string to expression nodes
    
    // Delete from all segments (could be optimized with bloom filters)
    std::vector<std::future<Status>> futures;
    
    for (auto& shard : shards_) {
        for (auto& segment : shard->GetSegments()) {
            futures.push_back(std::async(std::launch::async, [segment, &primary_keys, &filter_nodes]() {
                std::unique_lock<std::shared_mutex> lock(segment->mutex);
                
                auto status = segment->segment->Delete(primary_keys, filter_nodes, 0);
                
                if (status.ok()) {
                    // Update deleted count
                    // Note: This is approximate as we don't know exact count
                    segment->deleted_count.fetch_add(1);
                }
                
                return status;
            }));
        }
    }
    
    // Collect results
    size_t total_deleted = 0;
    for (auto& future : futures) {
        auto status = future.get();
        // Aggregate deleted counts from status messages
    }
    
    return Status(DB_SUCCESS, "Deleted records from multiple segments");
}

Status SegmentManager::Search(
    const std::string& field_name,
    const VectorPtr query_data,
    size_t query_dimension,
    size_t limit,
    Json& result,
    const std::string& filter,
    bool with_distance) {
    
    // Parse filter
    std::vector<vectordb::query::expr::ExprNodePtr> filter_nodes;
    // TODO: Parse filter string
    
    // Search all segments in parallel
    struct SearchResult {
        Json record;
        float distance;
        size_t segment_id;
    };
    
    std::vector<std::future<std::vector<SearchResult>>> futures;
    
    // Launch parallel searches on all segments
    for (auto& shard : shards_) {
        for (auto& segment : shard->GetSegments()) {
            futures.push_back(std::async(std::launch::async, 
                [segment, &field_name, query_data, query_dimension, limit, &filter_nodes, with_distance]() {
                
                std::shared_lock<std::shared_mutex> lock(segment->mutex);
                
                Json segment_result;
                std::vector<std::string> query_fields;  // TODO: Populate
                
                // Create a temporary result container
                std::vector<SearchResult> results;
                
                // Perform search on this segment
                // Note: This would need adaptation of the Search method
                // For now, we simulate the results
                
                return results;
            }));
        }
    }
    
    // Collect and merge results
    std::vector<SearchResult> all_results;
    
    for (auto& future : futures) {
        auto segment_results = future.get();
        all_results.insert(all_results.end(), segment_results.begin(), segment_results.end());
    }
    
    // Sort by distance and take top-k
    std::partial_sort(all_results.begin(), 
                     all_results.begin() + std::min(limit, all_results.size()),
                     all_results.end(),
                     [](const SearchResult& a, const SearchResult& b) {
                         return a.distance < b.distance;
                     });
    
    // Build final result
    result.LoadFromString("[]");
    for (size_t i = 0; i < std::min(limit, all_results.size()); ++i) {
        result.AddObjectToArray(std::move(all_results[i].record));
    }
    
    return Status::OK();
}

SegmentStats SegmentManager::GetStats() const {
    SegmentStats stats;
    
    stats.total_shards = shards_.size();
    
    for (const auto& shard : shards_) {
        stats.total_segments += shard->GetSegmentCount();
        
        for (const auto& segment : shard->GetSegments()) {
            stats.total_records += segment->record_count.load();
            stats.deleted_records += segment->deleted_count.load();
            
            // Estimate bytes used
            stats.bytes_used += segment->record_count * 
                (sizeof(float) * 128 + 100);  // Rough estimate
        }
    }
    
    return stats;
}

Status SegmentManager::MergeSegments(const std::vector<size_t>& segment_ids) {
    if (segment_ids.size() < 2) {
        return Status(INVALID_PAYLOAD, "Need at least 2 segments to merge");
    }
    
    // Find the segments
    std::vector<std::shared_ptr<Segment>> segments_to_merge;
    
    for (auto& shard : shards_) {
        for (auto& segment : shard->GetSegments()) {
            if (std::find(segment_ids.begin(), segment_ids.end(), 
                         segment->segment_id) != segment_ids.end()) {
                segments_to_merge.push_back(segment);
            }
        }
    }
    
    if (segments_to_merge.size() < 2) {
        return Status(DB_UNEXPECTED_ERROR, "Could not find segments to merge");
    }
    
    // Perform merge
    auto result = segments_to_merge[0];
    for (size_t i = 1; i < segments_to_merge.size(); ++i) {
        auto status = PerformMerge(result, segments_to_merge[i]);
        if (!status.ok()) {
            return status;
        }
    }
    
    return Status::OK();
}

Status SegmentManager::SplitSegment(size_t segment_id) {
    // Find the segment
    for (auto& shard : shards_) {
        for (auto& segment : shard->GetSegments()) {
            if (segment->segment_id == segment_id) {
                return PerformSplit(segment);
            }
        }
    }
    
    return Status(DB_UNEXPECTED_ERROR, "Segment not found: " + std::to_string(segment_id));
}

Status SegmentManager::RebalanceShards() {
    logger_.Info("Starting shard rebalancing");
    
    // Calculate target records per shard
    size_t total_records = 0;
    for (const auto& shard : shards_) {
        total_records += shard->GetRecordCount();
    }
    
    size_t target_per_shard = total_records / shards_.size();
    
    // Move segments between shards to balance load
    for (size_t i = 0; i < shards_.size(); ++i) {
        size_t shard_records = shards_[i]->GetRecordCount();
        
        if (shard_records > target_per_shard * 1.2) {  // 20% over target
            // Move segments to underloaded shards
            for (size_t j = 0; j < shards_.size(); ++j) {
                if (i == j) continue;
                
                if (shards_[j]->GetRecordCount() < target_per_shard * 0.8) {
                    // Move a segment from shard i to shard j
                    auto segments = shards_[i]->GetSegments();
                    if (!segments.empty()) {
                        auto segment = segments.back();
                        shards_[i]->RemoveSegment(segment->segment_id);
                        shards_[j]->AddSegment(segment);
                        
                        logger_.Info("Moved segment " + std::to_string(segment->segment_id) +
                                   " from shard " + std::to_string(i) + 
                                   " to shard " + std::to_string(j));
                    }
                }
            }
        }
    }
    
    logger_.Info("Shard rebalancing complete");
    return Status::OK();
}

Status SegmentManager::AddShard() {
    size_t new_shard_id = shards_.size();
    shards_.push_back(std::make_unique<Shard>(new_shard_id));
    
    // Create initial segment for the new shard
    CreateNewSegment(new_shard_id);
    
    // Update sharding config
    sharding_config_.num_shards = shards_.size();
    
    // Trigger rebalancing to distribute data to new shard
    if (sharding_config_.dynamic_resharding) {
        RebalanceShards();
    }
    
    logger_.Info("Added new shard " + std::to_string(new_shard_id));
    
    return Status::OK();
}

Status SegmentManager::RemoveShard(size_t shard_id) {
    if (shard_id >= shards_.size()) {
        return Status(INVALID_PAYLOAD, "Invalid shard ID");
    }
    
    if (shards_.size() <= 1) {
        return Status(DB_UNEXPECTED_ERROR, "Cannot remove the last shard");
    }
    
    // Move all segments from this shard to other shards
    auto segments = shards_[shard_id]->GetSegments();
    
    for (auto& segment : segments) {
        // Distribute to other shards
        size_t target_shard = (shard_id + 1) % shards_.size();
        if (target_shard == shard_id) {
            target_shard = (target_shard + 1) % shards_.size();
        }
        
        shards_[target_shard]->AddSegment(segment);
    }
    
    // Remove the shard
    shards_.erase(shards_.begin() + shard_id);
    
    // Update sharding config
    sharding_config_.num_shards = shards_.size();
    
    logger_.Info("Removed shard " + std::to_string(shard_id));
    
    return Status::OK();
}

void SegmentManager::UpdateSegmentConfig(const SegmentConfig& config) {
    segment_config_ = config;
    
    // Restart maintenance thread if needed
    if (maintenance_running_) {
        StopMaintenanceThread();
        StartMaintenanceThread();
    }
    
    logger_.Info("Updated segment configuration");
}

void SegmentManager::UpdateShardingConfig(const ShardingConfig& config) {
    sharding_config_ = config;
    
    // If number of shards changed, adjust
    while (shards_.size() < config.num_shards) {
        AddShard();
    }
    
    while (shards_.size() > config.num_shards) {
        RemoveShard(shards_.size() - 1);
    }
    
    logger_.Info("Updated sharding configuration");
}

} // namespace engine
} // namespace vectordb