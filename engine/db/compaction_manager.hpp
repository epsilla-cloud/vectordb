#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#include "utils/status.hpp"
#include "utils/concurrent_hashmap.hpp"
#include "utils/json.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

// Forward declarations
class TableSegment;
class Table;

/**
 * CompactionStats - Statistics for compaction operations
 */
struct CompactionStats {
  size_t total_vectors{0};
  size_t deleted_vectors{0};
  size_t compaction_count{0};
  size_t memory_freed_bytes{0};
  std::chrono::steady_clock::time_point last_compaction_time;
  std::chrono::milliseconds last_compaction_duration{0};

  double GetDeletedRatio() const {
    if (total_vectors == 0) return 0.0;
    return static_cast<double>(deleted_vectors) / total_vectors;
  }

  Json ToJson() const {
    Json json;
    json.SetInt("total_vectors", total_vectors);
    json.SetInt("deleted_vectors", deleted_vectors);
    json.SetDouble("deleted_ratio", GetDeletedRatio());
    json.SetInt("compaction_count", compaction_count);
    json.SetInt("memory_freed_mb", memory_freed_bytes / (1024 * 1024));
    json.SetInt("last_compaction_duration_ms", last_compaction_duration.count());
    return json;
  }
};

/**
 * CompactionManager - Manages automatic and manual compaction
 *
 * Inspired by:
 * - Qdrant's Vacuum Optimizer (20% threshold)
 * - Milvus's Compact mechanism
 * - Weaviate's Tombstone cleanup
 */
class CompactionManager {
 public:
  static CompactionManager& GetInstance() {
    static CompactionManager instance;
    return instance;
  }

  // Delete copy constructor and assignment
  CompactionManager(const CompactionManager&) = delete;
  CompactionManager& operator=(const CompactionManager&) = delete;

  // Start/stop automatic compaction
  Status Start();
  Status Stop();

  // Manual compaction
  Status CompactTable(const std::string& table_name);
  Status CompactAllTables();

  // Register/unregister tables
  void RegisterTable(const std::string& table_name, std::shared_ptr<Table> table);
  void UnregisterTable(const std::string& table_name);

  // Update statistics
  void UpdateStats(const std::string& table_name, size_t total_vectors, size_t deleted_vectors);

  // Get statistics
  CompactionStats GetGlobalStats() const;
  CompactionStats GetTableStats(const std::string& table_name) const;
  Json GetAllStatsAsJson() const;

  // Check if compaction is needed
  bool ShouldCompact(const std::string& table_name) const;
  bool IsCompacting() const { return is_compacting_.load(); }

  // Configuration
  void SetAutoCompaction(bool enabled);
  void SetCompactionThreshold(double threshold);
  void SetCompactionInterval(int seconds);

 private:
  CompactionManager();
  ~CompactionManager();

  // Worker thread function
  void CompactionWorker();

  // Compaction logic
  Status DoCompaction(const std::string& table_name);

  // Check if it's a good time to compact (low traffic period)
  bool IsLowTrafficPeriod() const;

  // Check memory pressure
  bool IsMemoryPressureHigh() const;

 private:
  // Tables registry
  ConcurrentHashMap<std::string, std::shared_ptr<Table>> tables_;

  // Statistics per table (mutable for const Get() calls)
  mutable ConcurrentHashMap<std::string, CompactionStats> table_stats_;

  // Track table names for iteration (since ConcurrentHashMap doesn't support it)
  mutable std::set<std::string> table_names_;
  mutable std::mutex table_names_mutex_;

  // Global statistics
  CompactionStats global_stats_;

  // Configuration (cached from global config)
  std::atomic<bool> auto_compaction_enabled_{true};
  std::atomic<double> compaction_threshold_{0.2};  // 20% deleted triggers compaction
  std::atomic<int> compaction_interval_{3600};     // Check every hour
  std::atomic<int> min_vectors_for_compaction_{1000};

  // Worker thread management
  std::unique_ptr<std::thread> worker_thread_;
  std::atomic<bool> stop_worker_{false};
  std::atomic<bool> is_compacting_{false};

  // CRITICAL BUG FIX (BUG-CMP-002): Track compacting tables per-table
  // Previously: Global is_compacting_ flag rejected ALL compaction requests
  // Solution: Track which specific tables are being compacted, allow concurrent compaction
  std::set<std::string> compacting_tables_;
  std::mutex compacting_tables_mutex_;

  // Synchronization
  mutable std::mutex compaction_mutex_;
  std::condition_variable compaction_cv_;

  // Logging
  Logger logger_;
};

/**
 * Helper class for scoped compaction tracking
 */
class ScopedCompactionTracker {
 public:
  explicit ScopedCompactionTracker(const std::string& table_name)
      : table_name_(table_name),
        start_time_(std::chrono::steady_clock::now()) {
    CompactionManager::GetInstance().UpdateStats(table_name_, 0, 0);
  }

  ~ScopedCompactionTracker() {
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);
    // Update duration in stats
  }

  void RecordFreedMemory(size_t bytes) {
    freed_bytes_ += bytes;
  }

 private:
  std::string table_name_;
  std::chrono::steady_clock::time_point start_time_;
  size_t freed_bytes_{0};
};

}  // namespace engine
}  // namespace vectordb