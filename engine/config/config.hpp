#pragma once

#include <algorithm>
#include <atomic> // Include for std::atomic
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
    
    // Log the configuration
    printf("[Config] Initialized with IntraQueryThreads=%d, RebuildThreads=%d (detected %u hardware threads)\n", 
           IntraQueryThreads.load(), RebuildThreads.load(), hw_threads);
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
    return config;
  }
};

// Global config instance
inline Config globalConfig;

}  // namespace vectordb
