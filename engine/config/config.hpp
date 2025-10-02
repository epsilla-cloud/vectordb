#pragma once

#include <algorithm>
#include <atomic> // Include for std::atomic
#include <cctype> // Include for ::tolower
#include <cstddef>
#include <cstdlib> // Include for std::getenv and std::atoi
#include <exception>
#include <memory>
#include <set>
#include <string>
#include <thread> // Include for std::thread::hardware_concurrency
#include <vector>
#include <stdexcept> // Include for std::invalid_argument

#include "utils/json.hpp"

namespace vectordb {

struct Config {
  std::atomic<int> IntraQueryThreads;
  std::atomic<int> MasterQueueSize{500};
  std::atomic<int> LocalQueueSize{500};
  std::atomic<int> GlobalSyncInterval{15};
  std::atomic<int> MinimalGraphSize{100};
  std::atomic<int> NumExecutorPerField{16};
  std::atomic<int> RebuildThreads;
  std::atomic<bool> PreFilter{false};
  std::atomic<bool> SoftDelete{true};
  
  // WAL auto-flush configuration
  std::atomic<int> WALFlushInterval{15};  // Default: flush every 15 seconds (was 30)
  std::atomic<bool> WALAutoFlush{true};   // Enable/disable auto flush

  // Compaction configuration (inspired by Qdrant and Milvus best practices)
  std::atomic<bool> AutoCompaction{true};        // Enable automatic compaction (default: true)
  std::atomic<double> CompactionThreshold{0.3};  // Trigger compaction when 30% deleted (default: 0.3)
  std::atomic<int> CompactionInterval{3600};     // Check interval in seconds (default: 1 hour)
  std::atomic<int> MinVectorsForCompaction{1000}; // Minimum vectors to trigger compaction (default: 1000)
  std::atomic<int> CompactionMaxDuration{1800};   // Maximum compaction duration in seconds (default: 30 min)

  // Index rebuild configuration
  std::atomic<int> RebuildInterval{60000};       // Index rebuild interval in milliseconds (default: 60 seconds)
  
  // === Incremental rebuild configuration ===
  std::atomic<bool> EnableIncrementalRebuild{false};        // Enable incremental rebuild (default: false, enable gradually)
  std::atomic<int> IncrementalThreshold{1000};              // New records < threshold use incremental (default: 1000)
  std::atomic<int> FullRebuildInterval{10};                 // Full rebuild every N incremental rebuilds (default: 10)
  std::atomic<bool> FilterDeletedInIncremental{true};       // Filter deleted nodes during incremental rebuild (default: true)
  std::atomic<bool> EnableBidirectionalLinks{true};         // Enable bidirectional links in incremental rebuild (default: true)
  
  // === Deletion and compaction strategy ===
  std::atomic<double> CompactionBeforeRebuildThreshold{0.1}; // Consider compaction if deletion ratio > threshold (default: 0.1 = 10%)
  std::atomic<bool> ForceFullRebuildAfterCompaction{true};   // Force full rebuild after compaction (default: true, required)
  
  // === I/O optimization ===
  std::atomic<int> RebuildSaveInterval{5};                  // Save to disk every N rebuilds (default: 5)
  std::atomic<int> RebuildSaveIntervalSeconds{300};         // Or save every N seconds (default: 300 = 5 minutes)

  // Memory management configuration
  std::atomic<int> InitialTableCapacity{1000};   // Initial table capacity (default: 1000, was 150000)

  // Eager compaction configuration: run Table::Compact() immediately after large soft-deletes
  std::atomic<bool> EagerCompactionOnDelete{true}; // Default: true
  std::atomic<int> MinDeletedVectorsForEagerCompaction{10}; // Minimum deleted vectors to trigger eager compaction (default: 10)
  // std::atomic<bool> EagerCompactionOnDelete{false}; --- IGNORE
  
  // Constructor to initialize thread counts based on hardware
  Config() {
    unsigned int hw_threads = std::thread::hardware_concurrency();
    if (hw_threads == 0) {
      hw_threads = 4; // Fallback if detection fails
    }
    
    // Check environment variable for thread count override
    const char* env_threads = std::getenv("EPSILLA_INTRA_QUERY_THREADS");
    int query_threads = static_cast<int>(hw_threads);
    if (env_threads != nullptr) {
      int env_value = std::atoi(env_threads);
      if (env_value >= 1 && env_value <= 128) {
        query_threads = env_value;
        printf("[Config] Using EPSILLA_INTRA_QUERY_THREADS=%d from environment\n", query_threads);
      }
    }
    
    // Use configured threads for query (optimal for parallel search)
    IntraQueryThreads.store(query_threads, std::memory_order_release);
    
    // Use up to 4 threads for rebuild to avoid overwhelming the system
    RebuildThreads.store(static_cast<int>(std::min(hw_threads, 4u)), std::memory_order_release);
    
    // Check environment variable for soft delete mode
    const char* env_soft_delete = std::getenv("SOFT_DELETE");
    bool soft_delete_enabled = true; // Default to soft delete
    if (env_soft_delete != nullptr) {
      std::string env_value(env_soft_delete);
      // Convert to lowercase for case-insensitive comparison
      std::transform(env_value.begin(), env_value.end(), env_value.begin(), ::tolower);
      soft_delete_enabled = (env_value == "true" || env_value == "1" || env_value == "yes");
      printf("[Config] Using SOFT_DELETE=%s from environment (soft delete %s)\n", 
             env_soft_delete, soft_delete_enabled ? "enabled" : "disabled");
    }
    SoftDelete.store(soft_delete_enabled, std::memory_order_release);
    
    // Check environment variable for WAL flush interval
    const char* env_wal_interval = std::getenv("WAL_FLUSH_INTERVAL");
    if (env_wal_interval != nullptr) {
      int interval = std::atoi(env_wal_interval);
      if (interval >= 5 && interval <= 3600) {  // Between 5 seconds and 1 hour
        WALFlushInterval.store(interval, std::memory_order_release);
        printf("[Config] Using WAL_FLUSH_INTERVAL=%d seconds from environment\n", interval);
      }
    }
    
    // Check environment variable for WAL auto flush
    const char* env_wal_auto = std::getenv("WAL_AUTO_FLUSH");
    if (env_wal_auto != nullptr) {
      std::string env_value(env_wal_auto);
      std::transform(env_value.begin(), env_value.end(), env_value.begin(), ::tolower);
      bool auto_flush = (env_value == "true" || env_value == "1" || env_value == "yes");
      WALAutoFlush.store(auto_flush, std::memory_order_release);
      printf("[Config] Using WAL_AUTO_FLUSH=%s from environment\n", auto_flush ? "true" : "false");
    }

    // Check environment variables for compaction configuration
    const char* env_auto_compact = std::getenv("AUTO_COMPACTION");
    if (env_auto_compact != nullptr) {
      std::string env_value(env_auto_compact);
      std::transform(env_value.begin(), env_value.end(), env_value.begin(), ::tolower);
      bool auto_compact = (env_value == "true" || env_value == "1" || env_value == "yes");
      AutoCompaction.store(auto_compact, std::memory_order_release);
      printf("[Config] Using AUTO_COMPACTION=%s from environment\n", auto_compact ? "true" : "false");
    }

    const char* env_compact_thresh = std::getenv("COMPACTION_THRESHOLD");
    if (env_compact_thresh != nullptr) {
      double threshold = std::atof(env_compact_thresh);
      if (threshold >= 0.05 && threshold <= 0.5) {  // Between 5% and 50%
        CompactionThreshold.store(threshold, std::memory_order_release);
        printf("[Config] Using COMPACTION_THRESHOLD=%.2f from environment\n", threshold);
      }
    }

    const char* env_compact_interval = std::getenv("COMPACTION_INTERVAL");
    if (env_compact_interval != nullptr) {
      int interval = std::atoi(env_compact_interval);
      if (interval >= 60 && interval <= 86400) {  // Between 1 minute and 24 hours
        CompactionInterval.store(interval, std::memory_order_release);
        printf("[Config] Using COMPACTION_INTERVAL=%d seconds from environment\n", interval);
      }
    }

    // Eager compaction toggle after delete (optional)
    const char* env_eager_delete_compact = std::getenv("EAGER_DELETE_COMPACT");
    if (env_eager_delete_compact != nullptr) {
      std::string env_value(env_eager_delete_compact);
      std::transform(env_value.begin(), env_value.end(), env_value.begin(), ::tolower);
      bool eager = (env_value == "true" || env_value == "1" || env_value == "yes");
      EagerCompactionOnDelete.store(eager, std::memory_order_release);
      printf("[Config] Using EAGER_DELETE_COMPACT=%s from environment\n", eager ? "true" : "false");
    }

    // Check environment variable for minimum deleted vectors for eager compaction
    const char* env_min_deleted_vectors = std::getenv("MIN_DELETED_VECTORS_FOR_EAGER_COMPACTION");
    if (env_min_deleted_vectors != nullptr) {
      int min_deleted = std::atoi(env_min_deleted_vectors);
      if (min_deleted >= 1 && min_deleted <= 10000) {  // Reasonable range: 1 to 10,000
        MinDeletedVectorsForEagerCompaction.store(min_deleted, std::memory_order_release);
        printf("[Config] Using MIN_DELETED_VECTORS_FOR_EAGER_COMPACTION=%d from environment\n", min_deleted);
      }
    }

    // Check environment variable for initial table capacity
    const char* env_initial_capacity = std::getenv("INITIAL_TABLE_CAPACITY");
    if (env_initial_capacity != nullptr) {
      int capacity = std::atoi(env_initial_capacity);
      if (capacity >= 10 && capacity <= 10000000) {  // Between 10 and 10M (lowered for testing)
        InitialTableCapacity.store(capacity, std::memory_order_release);
        printf("[Config] Using INITIAL_TABLE_CAPACITY=%d from environment\n", capacity);
      } else {
        printf("[Config] Invalid INITIAL_TABLE_CAPACITY=%s, using default %d\n",
               env_initial_capacity, InitialTableCapacity.load());
      }
    }

    // Check environment variable for rebuild interval
    const char* env_rebuild_interval = std::getenv("REBUILD_INTERVAL");
    if (env_rebuild_interval != nullptr) {
      int interval = std::atoi(env_rebuild_interval);
      if (interval >= 5000 && interval <= 3600000) {  // Between 5 seconds and 1 hour (in milliseconds)
        RebuildInterval.store(interval, std::memory_order_release);
        printf("[Config] Using REBUILD_INTERVAL=%d milliseconds from environment\n", interval);
      } else {
        printf("[Config] Invalid REBUILD_INTERVAL=%s, using default %d ms\n",
               env_rebuild_interval, RebuildInterval.load());
      }
    }
    
    // === Incremental rebuild configuration ===
    const char* env_enable_incremental = std::getenv("ENABLE_INCREMENTAL_REBUILD");
    if (env_enable_incremental != nullptr) {
      std::string env_value(env_enable_incremental);
      std::transform(env_value.begin(), env_value.end(), env_value.begin(), ::tolower);
      bool enable = (env_value == "true" || env_value == "1" || env_value == "yes");
      EnableIncrementalRebuild.store(enable, std::memory_order_release);
      printf("[Config] Using ENABLE_INCREMENTAL_REBUILD=%s from environment\n", enable ? "true" : "false");
    }
    
    const char* env_incremental_threshold = std::getenv("INCREMENTAL_THRESHOLD");
    if (env_incremental_threshold != nullptr) {
      int threshold = std::atoi(env_incremental_threshold);
      if (threshold >= 10 && threshold <= 100000) {
        IncrementalThreshold.store(threshold, std::memory_order_release);
        printf("[Config] Using INCREMENTAL_THRESHOLD=%d from environment\n", threshold);
      }
    }
    
    const char* env_full_rebuild_interval = std::getenv("FULL_REBUILD_INTERVAL");
    if (env_full_rebuild_interval != nullptr) {
      int interval = std::atoi(env_full_rebuild_interval);
      if (interval >= 1 && interval <= 100) {
        FullRebuildInterval.store(interval, std::memory_order_release);
        printf("[Config] Using FULL_REBUILD_INTERVAL=%d from environment\n", interval);
      }
    }
    
    const char* env_filter_deleted = std::getenv("FILTER_DELETED_IN_INCREMENTAL");
    if (env_filter_deleted != nullptr) {
      std::string env_value(env_filter_deleted);
      std::transform(env_value.begin(), env_value.end(), env_value.begin(), ::tolower);
      bool filter = (env_value == "true" || env_value == "1" || env_value == "yes");
      FilterDeletedInIncremental.store(filter, std::memory_order_release);
      printf("[Config] Using FILTER_DELETED_IN_INCREMENTAL=%s from environment\n", filter ? "true" : "false");
    }
    
    const char* env_bidirectional = std::getenv("ENABLE_BIDIRECTIONAL_LINKS");
    if (env_bidirectional != nullptr) {
      std::string env_value(env_bidirectional);
      std::transform(env_value.begin(), env_value.end(), env_value.begin(), ::tolower);
      bool enable = (env_value == "true" || env_value == "1" || env_value == "yes");
      EnableBidirectionalLinks.store(enable, std::memory_order_release);
      printf("[Config] Using ENABLE_BIDIRECTIONAL_LINKS=%s from environment\n", enable ? "true" : "false");
    }
    
    // === Deletion and compaction strategy ===
    const char* env_compact_before_rebuild = std::getenv("COMPACTION_BEFORE_REBUILD_THRESHOLD");
    if (env_compact_before_rebuild != nullptr) {
      double threshold = std::atof(env_compact_before_rebuild);
      if (threshold >= 0.05 && threshold <= 0.5) {
        CompactionBeforeRebuildThreshold.store(threshold, std::memory_order_release);
        printf("[Config] Using COMPACTION_BEFORE_REBUILD_THRESHOLD=%.2f from environment\n", threshold);
      }
    }
    
    const char* env_force_full_after_compact = std::getenv("FORCE_FULL_REBUILD_AFTER_COMPACTION");
    if (env_force_full_after_compact != nullptr) {
      std::string env_value(env_force_full_after_compact);
      std::transform(env_value.begin(), env_value.end(), env_value.begin(), ::tolower);
      bool force = (env_value == "true" || env_value == "1" || env_value == "yes");
      ForceFullRebuildAfterCompaction.store(force, std::memory_order_release);
      printf("[Config] Using FORCE_FULL_REBUILD_AFTER_COMPACTION=%s from environment\n", force ? "true" : "false");
    }
    
    // === I/O optimization ===
    const char* env_rebuild_save_interval = std::getenv("REBUILD_SAVE_INTERVAL");
    if (env_rebuild_save_interval != nullptr) {
      int interval = std::atoi(env_rebuild_save_interval);
      if (interval >= 1 && interval <= 100) {
        RebuildSaveInterval.store(interval, std::memory_order_release);
        printf("[Config] Using REBUILD_SAVE_INTERVAL=%d from environment\n", interval);
      }
    }
    
    const char* env_rebuild_save_seconds = std::getenv("REBUILD_SAVE_INTERVAL_SECONDS");
    if (env_rebuild_save_seconds != nullptr) {
      int seconds = std::atoi(env_rebuild_save_seconds);
      if (seconds >= 60 && seconds <= 3600) {
        RebuildSaveIntervalSeconds.store(seconds, std::memory_order_release);
        printf("[Config] Using REBUILD_SAVE_INTERVAL_SECONDS=%d from environment\n", seconds);
      }
    }

    // Check environment variable for VECTORDB_DISABLE_WAL_SYNC
    const char* env_disable_wal_sync = std::getenv("VECTORDB_DISABLE_WAL_SYNC");
    if (env_disable_wal_sync != nullptr) {
      // Note: This is just for reading the env var, actual WAL sync logic is elsewhere
      printf("[Config] VECTORDB_DISABLE_WAL_SYNC=%s from environment\n", env_disable_wal_sync);
    }

    // Configuration printing moved to ConfigManager::PrintConfiguration()
    // to avoid duplicate output
  }

  // Setter method for IntraQueryThreads
  void setIntraQueryThreads(int value) {
    if (value >= 1 && value <= 128) {
      IntraQueryThreads.store(value, std::memory_order_release);
    } else {
      throw std::invalid_argument("Invalid value for IntraQueryThreads, valid range: [1, 128]");
    }
  }

  // Setter method for SearchQueueSize (modifies both MasterQueueSize and LocalQueueSize atomically)
  void setSearchQueueSize(int value) {
    if (value >= 500 && value <= 10000000) {
      MasterQueueSize.store(value, std::memory_order_release);
      LocalQueueSize.store(value, std::memory_order_release);
    } else {
      throw std::invalid_argument("Invalid value for SearchQueueSize, valid range: [500, 10000000]");
    }
  }

  // Setter method for NumExecutorPerField
  void setNumExecutorPerField(int value) {
    if (value >= 1 && value <= 128) {
      NumExecutorPerField.store(value, std::memory_order_release);
    } else {
      throw std::invalid_argument("Invalid value for NumExecutorPerField, valid range: [1, 128]");
    }
  }

  // Setter method for RebuildThreads
  void setRebuildThreads(int value) {
    if (value >= 1 && value <= 128) {
      RebuildThreads.store(value, std::memory_order_release);
    } else {
      throw std::invalid_argument("Invalid value for RebuildThreads, valid range: [1, 128]");
    }
  }

  // A setter function that takes a JSON config, and loop through the keys and values to set the corresponding fields
  void updateConfig(const vectordb::Json& json, bool& needSwapExecutors) {
    needSwapExecutors = false;
    if (json.HasMember("IntraQueryThreads")) {
      setIntraQueryThreads(json.GetInt("IntraQueryThreads"));
      needSwapExecutors = true;
    }
    if (json.HasMember("ConcurrentWorkersPerIndex")) {
      setNumExecutorPerField(json.GetInt("ConcurrentWorkersPerIndex"));
      needSwapExecutors = true;
    }
    if (json.HasMember("RebuildThreads")) {
      setRebuildThreads(json.GetInt("RebuildThreads"));
    }
    if (json.HasMember("SearchQueueSize")) {
      setSearchQueueSize(json.GetInt("SearchQueueSize"));
      needSwapExecutors = true;
    }
    if (json.HasMember("PreFilter")) {
      PreFilter.store(json.GetBool("PreFilter"));
    }
    if (json.HasMember("SoftDelete")) {
      setSoftDelete(json.GetBool("SoftDelete"));
    }
    if (json.HasMember("InitialTableCapacity")) {
      int capacity = json.GetInt("InitialTableCapacity");
      if (capacity >= 100 && capacity <= 10000000) {
        InitialTableCapacity.store(capacity, std::memory_order_release);
      }
    }
    if (json.HasMember("EagerCompactionOnDelete")) {
      EagerCompactionOnDelete.store(json.GetBool("EagerCompactionOnDelete"), std::memory_order_release);
    }
    if (json.HasMember("RebuildInterval")) {
      int interval = json.GetInt("RebuildInterval");
      if (interval >= 5000 && interval <= 3600000) {  // Between 5 seconds and 1 hour (in milliseconds)
        RebuildInterval.store(interval, std::memory_order_release);
      }
    }
  }
  
  // Setter method for SoftDelete mode
  void setSoftDelete(bool value) {
    SoftDelete.store(value, std::memory_order_release);
  }
  
  // Get current configuration as JSON for debugging
  vectordb::Json getConfigAsJson() const {
    vectordb::Json config;
    config.SetInt("IntraQueryThreads", IntraQueryThreads.load(std::memory_order_acquire));
    config.SetInt("MasterQueueSize", MasterQueueSize.load(std::memory_order_acquire));
    config.SetInt("LocalQueueSize", LocalQueueSize.load(std::memory_order_acquire));
    config.SetInt("GlobalSyncInterval", GlobalSyncInterval.load(std::memory_order_acquire));
    config.SetInt("MinimalGraphSize", MinimalGraphSize.load(std::memory_order_acquire));
    config.SetInt("NumExecutorPerField", NumExecutorPerField.load(std::memory_order_acquire));
    config.SetInt("RebuildThreads", RebuildThreads.load(std::memory_order_acquire));
    config.SetBool("PreFilter", PreFilter.load(std::memory_order_acquire));
    config.SetBool("SoftDelete", SoftDelete.load(std::memory_order_acquire));
    config.SetInt("WALFlushInterval", WALFlushInterval.load(std::memory_order_acquire));
    config.SetBool("WALAutoFlush", WALAutoFlush.load(std::memory_order_acquire));
    config.SetBool("AutoCompaction", AutoCompaction.load(std::memory_order_acquire));
    config.SetDouble("CompactionThreshold", CompactionThreshold.load(std::memory_order_acquire));
    config.SetInt("CompactionInterval", CompactionInterval.load(std::memory_order_acquire));
    config.SetInt("MinVectorsForCompaction", MinVectorsForCompaction.load(std::memory_order_acquire));
    config.SetInt("CompactionMaxDuration", CompactionMaxDuration.load(std::memory_order_acquire));
    config.SetInt("InitialTableCapacity", InitialTableCapacity.load(std::memory_order_acquire));
    config.SetBool("EagerCompactionOnDelete", EagerCompactionOnDelete.load(std::memory_order_acquire));
    config.SetInt("RebuildInterval", RebuildInterval.load(std::memory_order_acquire));
    return config;
  }
};

// Global config instance
inline Config globalConfig;

}  // namespace vectordb
