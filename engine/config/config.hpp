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
  std::atomic<double> CompactionThreshold{0.2};  // Trigger compaction when 20% deleted (default: 0.2)
  std::atomic<int> CompactionInterval{3600};     // Check interval in seconds (default: 1 hour)
  std::atomic<int> MinVectorsForCompaction{1000}; // Minimum vectors to trigger compaction (default: 1000)
  std::atomic<int> CompactionMaxDuration{1800};   // Maximum compaction duration in seconds (default: 30 min)

  // Memory management configuration
  std::atomic<int> InitialTableCapacity{1000};   // Initial table capacity (default: 1000, was 150000)
  
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
    return config;
  }
};

// Global config instance
inline Config globalConfig;

}  // namespace vectordb
