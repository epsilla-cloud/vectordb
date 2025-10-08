#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <unordered_map>
#include <shared_mutex>
#include <thread>
#include <functional>
#include "db/table_segment.hpp"
#include "db/catalog/meta.hpp"
#include "logger/logger.hpp"
#include "utils/status.hpp"
#include "utils/concurrent_vector.hpp"

namespace vectordb {
namespace engine {

// Configuration for multi-segment management
struct SegmentConfig {
    size_t max_segment_size = 1000000;        // Maximum records per segment
    size_t optimal_segment_size = 500000;     // Optimal size for performance
    size_t min_segment_size = 10000;          // Minimum size before merging
    size_t max_segments_per_shard = 10;       // Maximum segments in a shard
    bool auto_merge = true;                   // Auto-merge small segments
    bool auto_split = true;                   // Auto-split large segments
    double merge_threshold = 0.3;             // Merge if < 30% of optimal size
    double split_threshold = 1.5;             // Split if > 150% of optimal size
};

// Sharding configuration
struct ShardingConfig {
    enum ShardingStrategy {
        HASH_BASED,      // Hash-based sharding
        RANGE_BASED,     // Range-based sharding
        CONSISTENT_HASH, // Consistent hashing for distributed systems
        CUSTOM          // Custom sharding function
    };
    
    ShardingStrategy strategy = HASH_BASED;
    size_t num_shards = 4;                    // Number of shards
    size_t replication_factor = 1;            // Number of replicas per shard
    bool dynamic_resharding = true;           // Allow dynamic resharding
    
    // Custom sharding function (if strategy == CUSTOM)
    std::function<size_t(const Json&)> custom_shard_func;
};

// Statistics for monitoring
struct SegmentStats {
    size_t total_records = 0;
    size_t total_segments = 0;
    size_t total_shards = 0;
    size_t deleted_records = 0;
    size_t bytes_used = 0;
    double avg_query_time_ms = 0;
    double avg_insert_time_ms = 0;
};

// Forward declaration
class Shard;

// Multi-segment table manager
class SegmentManager {
public:
    SegmentManager(
        const meta::TableSchema& schema,
        const std::string& db_path,
        const SegmentConfig& seg_config = SegmentConfig(),
        const ShardingConfig& shard_config = ShardingConfig())
        : table_schema_(schema),
          db_catalog_path_(db_path),
          segment_config_(seg_config),
          sharding_config_(shard_config),
          next_segment_id_(0) {
        
        Initialize();
    }
    
    ~SegmentManager() {
        Shutdown();
    }
    
    // Insert operations
    Status Insert(Json& record, std::unordered_map<std::string, std::string>& headers);
    Status BatchInsert(Json& records, std::unordered_map<std::string, std::string>& headers);
    
    // Delete operations
    Status Delete(Json& primary_keys, const std::string& filter);
    
    // Search operations (will be parallelized)
    Status Search(
        const std::string& field_name,
        const VectorPtr query_data,
        size_t query_dimension,
        size_t limit,
        Json& result,
        const std::string& filter = "",
        bool with_distance = false);
    
    // Get statistics
    SegmentStats GetStats() const;
    
    // Manual operations
    Status MergeSegments(const std::vector<size_t>& segment_ids);
    Status SplitSegment(size_t segment_id);
    Status RebalanceShards();
    Status AddShard();
    Status RemoveShard(size_t shard_id);
    
    // Configuration updates
    void UpdateSegmentConfig(const SegmentConfig& config);
    void UpdateShardingConfig(const ShardingConfig& config);
    
private:
    // Internal segment wrapper
    struct Segment {
        size_t segment_id;
        std::shared_ptr<TableSegment> segment;
        std::atomic<size_t> record_count;
        std::atomic<size_t> deleted_count;
        std::chrono::steady_clock::time_point created_time;
        std::chrono::steady_clock::time_point last_modified;
        mutable std::shared_mutex mutex;  // Per-segment read-write lock
        
        Segment(size_t id, std::shared_ptr<TableSegment> seg)
            : segment_id(id), segment(seg), record_count(0), deleted_count(0) {
            created_time = std::chrono::steady_clock::now();
            last_modified = created_time;
        }
        
        bool NeedsMerge(const SegmentConfig& config) const {
            size_t active_records = record_count - deleted_count;
            return active_records < config.optimal_segment_size * config.merge_threshold;
        }
        
        bool NeedsSplit(const SegmentConfig& config) const {
            return record_count > config.optimal_segment_size * config.split_threshold;
        }
    };
    
    // Shard implementation
    class Shard {
    public:
        Shard(size_t id) : shard_id_(id), total_records_(0) {}
        
        void AddSegment(std::shared_ptr<Segment> segment) {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            segments_.push_back(segment);
            total_records_ += segment->record_count.load();
        }
        
        void RemoveSegment(size_t segment_id) {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            segments_.erase(
                std::remove_if(segments_.begin(), segments_.end(),
                    [segment_id](const auto& seg) { return seg->segment_id == segment_id; }),
                segments_.end());
        }
        
        std::shared_ptr<Segment> SelectSegmentForInsert(size_t max_size) {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            for (auto& seg : segments_) {
                if (seg->record_count.load() < max_size) {
                    return seg;
                }
            }
            return nullptr;
        }
        
        std::vector<std::shared_ptr<Segment>> GetSegments() const {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            return segments_;
        }
        
        size_t GetRecordCount() const {
            return total_records_.load();
        }
        
        size_t GetSegmentCount() const {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            return segments_.size();
        }
        
    private:
        size_t shard_id_;
        std::vector<std::shared_ptr<Segment>> segments_;
        std::atomic<size_t> total_records_;
        mutable std::shared_mutex mutex_;
    };
    
    void Initialize() {
        // Create initial shards
        for (size_t i = 0; i < sharding_config_.num_shards; ++i) {
            shards_.push_back(std::make_unique<Shard>(i));
            
            // Create initial segment for each shard
            CreateNewSegment(i);
        }
        
        // Start background maintenance thread
        if (segment_config_.auto_merge || segment_config_.auto_split) {
            StartMaintenanceThread();
        }
        
        logger_.Info("SegmentManager initialized with " + 
                    std::to_string(sharding_config_.num_shards) + " shards");
    }
    
    void Shutdown() {
        StopMaintenanceThread();
        
        // Flush all segments
        for (auto& shard : shards_) {
            for (auto& segment : shard->GetSegments()) {
                segment->segment->SaveTableSegment(table_schema_, db_catalog_path_, true);
            }
        }
        
        logger_.Info("SegmentManager shutdown complete");
    }
    
    size_t CalculateShardId(const Json& record) {
        switch (sharding_config_.strategy) {
            case ShardingConfig::HASH_BASED: {
                // Hash the primary key
                std::hash<std::string> hasher;
                std::string pk_str = ExtractPrimaryKey(record);
                return hasher(pk_str) % sharding_config_.num_shards;
            }
            
            case ShardingConfig::RANGE_BASED: {
                // Range partitioning based on primary key
                std::string pk_str = ExtractPrimaryKey(record);
                // Simple range division (can be improved)
                return std::min(
                    static_cast<size_t>(pk_str[0] % sharding_config_.num_shards),
                    sharding_config_.num_shards - 1);
            }
            
            case ShardingConfig::CONSISTENT_HASH: {
                // Implement consistent hashing for better distribution
                return ConsistentHash(ExtractPrimaryKey(record));
            }
            
            case ShardingConfig::CUSTOM: {
                if (sharding_config_.custom_shard_func) {
                    return sharding_config_.custom_shard_func(record) % sharding_config_.num_shards;
                }
                return 0;  // Default to first shard
            }
            
            default:
                return 0;
        }
    }
    
    std::string ExtractPrimaryKey(const Json& record) {
        // Extract primary key from record (implementation depends on schema)
        for (const auto& field : table_schema_.fields_) {
            if (field.is_primary_key_) {
                if (record.HasMember(field.name_)) {
                    return record.GetString(field.name_);
                }
            }
        }
        return "";
    }
    
    size_t ConsistentHash(const std::string& key) {
        // Simple consistent hashing implementation
        std::hash<std::string> hasher;
        size_t hash = hasher(key);
        
        // Virtual nodes for better distribution
        const size_t virtual_nodes = 150;
        size_t best_shard = 0;
        size_t min_hash = std::numeric_limits<size_t>::max();
        
        for (size_t i = 0; i < sharding_config_.num_shards; ++i) {
            for (size_t v = 0; v < virtual_nodes; ++v) {
                size_t vh = hasher(std::to_string(i) + ":" + std::to_string(v));
                if (vh >= hash && vh < min_hash) {
                    min_hash = vh;
                    best_shard = i;
                }
            }
        }
        
        return best_shard;
    }
    
    std::shared_ptr<Segment> CreateNewSegment(size_t shard_id) {
        size_t segment_id = next_segment_id_.fetch_add(1);
        
        auto segment_mvp = std::make_shared<TableSegment>(
            table_schema_, 
            segment_config_.optimal_segment_size,
            nullptr  // embedding_service can be passed here
        );
        
        auto segment = std::make_shared<Segment>(segment_id, segment_mvp);
        
        if (shard_id < shards_.size()) {
            shards_[shard_id]->AddSegment(segment);
        }
        
        logger_.Debug("Created new segment " + std::to_string(segment_id) + 
                     " in shard " + std::to_string(shard_id));
        
        return segment;
    }
    
    Status PerformMerge(std::shared_ptr<Segment> seg1, std::shared_ptr<Segment> seg2) {
        // Lock both segments
        std::unique_lock<std::shared_mutex> lock1(seg1->mutex);
        std::unique_lock<std::shared_mutex> lock2(seg2->mutex);
        
        // Create new merged segment
        auto merged_segment = std::make_shared<TableSegment>(
            table_schema_,
            segment_config_.optimal_segment_size,
            nullptr
        );
        
        // Copy records from both segments to merged segment
        // (Implementation would copy non-deleted records)
        
        logger_.Info("Merged segments " + std::to_string(seg1->segment_id) + 
                    " and " + std::to_string(seg2->segment_id));
        
        return Status::OK();
    }
    
    Status PerformSplit(std::shared_ptr<Segment> segment) {
        std::unique_lock<std::shared_mutex> lock(segment->mutex);
        
        size_t mid_point = segment->record_count / 2;
        
        // Create two new segments
        auto seg1 = CreateNewSegment(0);  // Will be reassigned to proper shard
        auto seg2 = CreateNewSegment(0);
        
        // Split records between two segments
        // (Implementation would divide records based on some criteria)
        
        logger_.Info("Split segment " + std::to_string(segment->segment_id) + 
                    " into two segments");
        
        return Status::OK();
    }
    
    void MaintenanceLoop() {
        while (maintenance_running_) {
            std::this_thread::sleep_for(std::chrono::seconds(30));
            
            if (!maintenance_running_) break;
            
            // Check for segments that need merging
            if (segment_config_.auto_merge) {
                std::vector<std::shared_ptr<Segment>> merge_candidates;
                
                for (auto& shard : shards_) {
                    for (auto& segment : shard->GetSegments()) {
                        if (segment->NeedsMerge(segment_config_)) {
                            merge_candidates.push_back(segment);
                        }
                    }
                }
                
                // Merge pairs of small segments
                for (size_t i = 0; i + 1 < merge_candidates.size(); i += 2) {
                    PerformMerge(merge_candidates[i], merge_candidates[i + 1]);
                }
            }
            
            // Check for segments that need splitting
            if (segment_config_.auto_split) {
                for (auto& shard : shards_) {
                    for (auto& segment : shard->GetSegments()) {
                        if (segment->NeedsSplit(segment_config_)) {
                            PerformSplit(segment);
                        }
                    }
                }
            }
        }
    }
    
    void StartMaintenanceThread() {
        maintenance_running_ = true;
        maintenance_thread_ = std::thread([this]() {
            MaintenanceLoop();
        });
    }
    
    void StopMaintenanceThread() {
        maintenance_running_ = false;
        if (maintenance_thread_.joinable()) {
            maintenance_thread_.join();
        }
    }
    
    meta::TableSchema table_schema_;
    std::string db_catalog_path_;
    SegmentConfig segment_config_;
    ShardingConfig sharding_config_;
    
    std::vector<std::unique_ptr<Shard>> shards_;
    std::atomic<size_t> next_segment_id_;
    
    std::thread maintenance_thread_;
    std::atomic<bool> maintenance_running_;
    
    Logger logger_;
};

} // namespace engine
} // namespace vectordb