#pragma once

#include <algorithm>
#include <atomic> // Include for std::atomic
#include <cctype> // Include for ::tolower
#include <cerrno> // Include for errno
#include <climits> // Include for INT_MAX, INT_MIN
#include <cstddef>
#include <cstdlib> // Include for std::getenv and std::atoi
#include <exception>
#include <fstream> // Include for std::ifstream (cgroup detection)
#include <memory>
#include <set>
#include <string>
#include <thread> // Include for std::thread::hardware_concurrency
#include <vector>
#include <stdexcept> // Include for std::invalid_argument

#include "utils/json.hpp"

namespace vectordb {

// ============================================================================
// Configuration Limits and Defaults
// Centralized management of all configuration constraints
// All limits can be overridden via environment variables with _MIN/_MAX suffix
// ============================================================================

struct ConfigLimits {
  // Initial Table Capacity
  static constexpr int INITIAL_TABLE_CAPACITY_DEFAULT = 1000;
  static constexpr int INITIAL_TABLE_CAPACITY_MIN = 10;
  static constexpr int INITIAL_TABLE_CAPACITY_MAX = 10000000;

  // Eager Compaction
  static constexpr int MIN_DELETED_VECTORS_MIN = 1;
  static constexpr int MIN_DELETED_VECTORS_MAX = 10000;

  // WAL Flush Interval
  static constexpr int WAL_FLUSH_INTERVAL_MIN = 1;
  static constexpr int WAL_FLUSH_INTERVAL_MAX = 3600;

  // Compaction Threshold
  static constexpr double COMPACTION_THRESHOLD_MIN = 0.01;
  static constexpr double COMPACTION_THRESHOLD_MAX = 0.99;

  // Compaction Interval
  static constexpr int COMPACTION_INTERVAL_MIN = 60;
  static constexpr int COMPACTION_INTERVAL_MAX = 86400;

  // Rebuild Interval
  static constexpr int REBUILD_INTERVAL_MIN = 1000;      // 1 second
  static constexpr int REBUILD_INTERVAL_MAX = 3600000;   // 1 hour

  // Worker Threads
  static constexpr int CPU_WORKERS_MIN = 1;
  static constexpr int CPU_WORKERS_MAX = 256;
  static constexpr int IO_WORKERS_MIN = 1;
  static constexpr int IO_WORKERS_MAX = 256;

  // Queue Sizes
  static constexpr int QUEUE_SIZE_MIN = 10;
  static constexpr int QUEUE_SIZE_MAX = 1000000;

  // === P1 High Priority Configuration Limits ===

  // Intra Query Threads
  static constexpr int INTRA_QUERY_THREADS_MIN = 1;
  static constexpr int INTRA_QUERY_THREADS_MAX = 256;

  // Incremental Rebuild
  static constexpr int INCREMENTAL_THRESHOLD_MIN = 10;
  static constexpr int INCREMENTAL_THRESHOLD_MAX = 100000;

  static constexpr int FULL_REBUILD_INTERVAL_MIN = 1;
  static constexpr int FULL_REBUILD_INTERVAL_MAX = 100;

  // Compaction Before Rebuild
  static constexpr double COMPACTION_BEFORE_REBUILD_MIN = 0.01;
  static constexpr double COMPACTION_BEFORE_REBUILD_MAX = 0.99;

  // Rebuild Save Intervals
  static constexpr int REBUILD_SAVE_INTERVAL_MIN = 1;
  static constexpr int REBUILD_SAVE_INTERVAL_MAX = 1000;

  static constexpr int REBUILD_SAVE_INTERVAL_SECONDS_MIN = 60;
  static constexpr int REBUILD_SAVE_INTERVAL_SECONDS_MAX = 86400;  // 24 hours

  // === P2 NSG Index Parameters ===

  static constexpr int NSG_SEARCH_LENGTH_MIN = 0;   // 0 = auto-adaptive
  static constexpr int NSG_SEARCH_LENGTH_MAX = 256;

  static constexpr int NSG_OUT_DEGREE_MIN = 10;
  static constexpr int NSG_OUT_DEGREE_MAX = 200;

  static constexpr int NSG_CANDIDATE_POOL_MIN = 50;
  static constexpr int NSG_CANDIDATE_POOL_MAX = 2000;

  // Helper function to get limit from environment or use default
  static int GetEnvInt(const char* env_name, int default_value, int min_value, int max_value) {
    const char* env_value = std::getenv(env_name);
    if (env_value != nullptr) {
      // CRITICAL BUG FIX (BUG-CFG-002): Use strtol with error checking instead of atoi
      // atoi() doesn't handle overflow - values like "999999999999" wrap around silently
      char* end;
      errno = 0;
      long value_long = std::strtol(env_value, &end, 10);

      // Check for conversion errors
      if (errno == ERANGE || value_long > INT_MAX || value_long < INT_MIN) {
        printf("[ConfigLimits] Warning: %s=%s out of range (overflow), using default %d\n",
               env_name, env_value, default_value);
        return default_value;
      }

      if (end == env_value || *end != '\0') {
        printf("[ConfigLimits] Warning: %s=%s invalid integer, using default %d\n",
               env_name, env_value, default_value);
        return default_value;
      }

      int value = static_cast<int>(value_long);
      if (value >= min_value && value <= max_value) {
        return value;
      }
      printf("[ConfigLimits] Warning: %s=%s out of range [%d, %d], using default %d\n",
             env_name, env_value, min_value, max_value, default_value);
    }
    return default_value;
  }

  static double GetEnvDouble(const char* env_name, double default_value, double min_value, double max_value) {
    const char* env_value = std::getenv(env_name);
    if (env_value != nullptr) {
      double value = std::atof(env_value);
      if (value >= min_value && value <= max_value) {
        return value;
      }
      printf("[ConfigLimits] Warning: %s=%s out of range [%.2f, %.2f], using default %.2f\n",
             env_name, env_value, min_value, max_value, default_value);
    }
    return default_value;
  }

  /**
   * @brief Detect CPU quota from cgroup (K8s-aware CPU detection)
   *
   * In Kubernetes environments, containers may have CPU limits set via cgroups
   * that are lower than the physical CPU count. This function detects those limits.
   *
   * Priority:
   * 1. Check cgroup v1 (K8s < 1.25): /sys/fs/cgroup/cpu/cpu.cfs_quota_us
   * 2. Check cgroup v2 (K8s >= 1.25): /sys/fs/cgroup/cpu.max
   * 3. Fallback to hardware_concurrency()
   *
   * @return Effective CPU cores available (minimum 1)
   */
  static size_t DetectCgroupCpuQuota() {
    size_t hardware_cores = std::thread::hardware_concurrency();
    if (hardware_cores == 0) {
      hardware_cores = 4; // Fallback
    }

#ifdef __linux__
    // Try cgroup v1 (used by most K8s versions)
    try {
      std::ifstream quota_file("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
      std::ifstream period_file("/sys/fs/cgroup/cpu/cpu.cfs_period_us");

      if (quota_file && period_file) {
        int64_t quota = -1, period = -1;
        quota_file >> quota;
        period_file >> period;

        // quota = -1 means no limit set
        if (quota > 0 && period > 0) {
          size_t cgroup_cores = static_cast<size_t>(quota / period);
          // Handle fractional cores (e.g., 0.5 CPU = 500m in K8s)
          if (cgroup_cores == 0 && quota < period) {
            cgroup_cores = 1; // Round up fractional cores to 1
          }
          if (cgroup_cores > 0 && cgroup_cores < hardware_cores) {
            printf("[Config] Detected cgroup v1 CPU quota: %zu cores (quota=%ld, period=%ld, hw=%zu)\n",
                   cgroup_cores, quota, period, hardware_cores);
            return cgroup_cores;
          }
        }
      }
    } catch (...) {
      // Silently continue to next detection method
    }

    // Try cgroup v2 (newer K8s 1.25+)
    try {
      std::ifstream cgroup_v2_file("/sys/fs/cgroup/cpu.max");
      if (cgroup_v2_file) {
        std::string quota_str, period_str;
        cgroup_v2_file >> quota_str >> period_str;

        // "max" means no limit
        if (quota_str != "max" && !period_str.empty()) {
          int64_t quota = std::stoll(quota_str);
          int64_t period = std::stoll(period_str);

          if (quota > 0 && period > 0) {
            size_t cgroup_cores = static_cast<size_t>(quota / period);
            if (cgroup_cores == 0 && quota < period) {
              cgroup_cores = 1;
            }
            if (cgroup_cores > 0 && cgroup_cores < hardware_cores) {
              printf("[Config] Detected cgroup v2 CPU quota: %zu cores (quota=%ld, period=%ld, hw=%zu)\n",
                     cgroup_cores, quota, period, hardware_cores);
              return cgroup_cores;
            }
          }
        }
      }
    } catch (...) {
      // Silently continue to fallback
    }
#endif

    // No cgroup limit detected or not on Linux - use hardware count
    return hardware_cores;
  }

  // Allow runtime override of limits via environment variables
  static void InitializeLimits() {
    // Currently using compile-time constants
    // Future: could load from CONFIG_LIMITS_FILE or environment variables like:
    // INITIAL_TABLE_CAPACITY_MIN, INITIAL_TABLE_CAPACITY_MAX, etc.
  }
};

// ============================================================================
// Main Configuration Structure
// ============================================================================

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
  // Initial table capacity - tables grow automatically from this starting point
  // Default: 1000 (sufficient for most use cases with adaptive growth)
  // Configure via INITIAL_TABLE_CAPACITY environment variable for performance-sensitive scenarios:
  // - If you know you'll insert millions of vectors, set higher (e.g., 100000) to avoid resize overhead
  // - For memory-constrained environments, keep at 1000 to minimize initial footprint
  // Note: API parameter "vectorScale" is deprecated - use this environment variable instead
  std::atomic<int> InitialTableCapacity{1000};

  // Eager compaction configuration: run Table::Compact() immediately after large soft-deletes
  std::atomic<bool> EagerCompactionOnDelete{true}; // Default: true
  std::atomic<int> MinDeletedVectorsForEagerCompaction{10}; // Minimum deleted vectors to trigger eager compaction (default: 10)
  // std::atomic<bool> EagerCompactionOnDelete{false}; --- IGNORE

  // NSG Index parameters
  std::atomic<int> NSGSearchLength{0};        // 0 = auto-adaptive based on data size
  std::atomic<int> NSGOutDegree{50};
  std::atomic<int> NSGCandidatePoolSize{300};
  std::atomic<int> NSGKnng{100};

  // Worker pool configuration for CPU/IO task separation
  std::atomic<int> CpuWorkerThreads{0};        // 0 = auto-detect (75% of hardware threads)
  std::atomic<int> IoWorkerThreads{0};         // 0 = auto-detect (25% of hardware threads)
  std::atomic<int> WorkerPoolMaxQueueSize{10000};
  std::atomic<bool> EnableCpuAffinity{false};  // Bind CPU workers to specific cores

  // === Full-Text Search Integration ===
  std::atomic<bool> EnableFullText{false};     // Enable full-text search integration
  std::string FullTextEngine;                  // Full-text engine type (quickwit, elasticsearch, meilisearch)
  std::string FullTextBinaryPath;              // Path to engine binary (for Quickwit)
  std::string FullTextConfigPath;              // Path to engine config file
  std::string FullTextDataDir;                 // Data directory (default: ./fulltext_data)
  std::atomic<int> FullTextPort{7280};         // HTTP port (default: 7280)
  std::string FullTextEndpoint;                // HTTP endpoint (for Elasticsearch/Meilisearch)
  std::string FullTextApiKey;                  // API key (optional)

  // Backward compatibility (deprecated - use FullText* instead)
  std::atomic<bool> EnableQuickwit{false};
  std::string QuickwitBinaryPath;
  std::string QuickwitConfigPath;
  std::string QuickwitDataDir;
  std::atomic<int> QuickwitPort{7280};

  // === Vector CRUD API Statistics (in-memory only, for monitoring) ===
  // Request counters for each operation type
  std::atomic<uint64_t> VectorInsertRequestCount{0};
  std::atomic<uint64_t> VectorQueryRequestCount{0};
  std::atomic<uint64_t> VectorDeleteRequestCount{0};
  std::atomic<uint64_t> VectorUpdateRequestCount{0};

  // Last request timestamps (Unix timestamp in seconds)
  std::atomic<int64_t> VectorInsertLastRequestTime{0};
  std::atomic<int64_t> VectorQueryLastRequestTime{0};
  std::atomic<int64_t> VectorDeleteLastRequestTime{0};
  std::atomic<int64_t> VectorUpdateLastRequestTime{0};

  // Request latency statistics (in microseconds for precision)
  // Histogram buckets: <1ms, <5ms, <10ms, <50ms, <100ms, <500ms, <1s, <5s, >=5s
  std::atomic<uint64_t> VectorQueryLatencyBucket_1ms{0};     // < 1ms
  std::atomic<uint64_t> VectorQueryLatencyBucket_5ms{0};     // < 5ms
  std::atomic<uint64_t> VectorQueryLatencyBucket_10ms{0};    // < 10ms
  std::atomic<uint64_t> VectorQueryLatencyBucket_50ms{0};    // < 50ms
  std::atomic<uint64_t> VectorQueryLatencyBucket_100ms{0};   // < 100ms
  std::atomic<uint64_t> VectorQueryLatencyBucket_500ms{0};   // < 500ms
  std::atomic<uint64_t> VectorQueryLatencyBucket_1s{0};      // < 1s
  std::atomic<uint64_t> VectorQueryLatencyBucket_5s{0};      // < 5s
  std::atomic<uint64_t> VectorQueryLatencyBucket_inf{0};     // >= 5s

  // Total query latency sum for calculating average (in microseconds)
  std::atomic<uint64_t> VectorQueryLatencySum{0};

  // Memory and resource statistics
  std::atomic<uint64_t> MemoryUsedBytes{0};      // Current memory usage
  std::atomic<uint64_t> MemoryPeakBytes{0};      // Peak memory usage

  // Database/Table statistics
  std::atomic<uint64_t> DatabaseCount{0};        // Number of databases
  std::atomic<uint64_t> TableCount{0};           // Number of tables
  std::atomic<uint64_t> TotalVectorCount{0};     // Total vectors across all tables

  // Constructor to initialize thread counts based on hardware
  Config() {
    // CRITICAL FIX: Use cgroup-aware CPU detection for K8s compatibility
    // In K8s, hardware_concurrency() returns the host's CPU count, not the container's limit
    // This caused severe over-subscription (e.g., 4 threads fighting for 1 CPU core)
    size_t effective_cores = ConfigLimits::DetectCgroupCpuQuota();

    // Check environment variable for thread count override
    int query_threads = ConfigLimits::GetEnvInt(
      "EPSILLA_INTRA_QUERY_THREADS",
      static_cast<int>(effective_cores),
      ConfigLimits::INTRA_QUERY_THREADS_MIN,
      ConfigLimits::INTRA_QUERY_THREADS_MAX
    );
    if (query_threads != static_cast<int>(effective_cores)) {
      printf("[Config] Using EPSILLA_INTRA_QUERY_THREADS=%d from environment (detected cores=%zu)\n",
             query_threads, effective_cores);
    }

    // Use configured threads for query (optimal for parallel search)
    IntraQueryThreads.store(query_threads, std::memory_order_release);

    // Use up to 4 threads for rebuild to avoid overwhelming the system
    // In K8s with 1-2 cores, this will automatically limit to 1-2 threads
    RebuildThreads.store(static_cast<int>(std::min(effective_cores, size_t(4))), std::memory_order_release);

    // Log the final configuration for debugging K8s deployments
    printf("[Config] Thread configuration: IntraQuery=%d, Rebuild=%d (effective_cores=%zu)\n",
           IntraQueryThreads.load(), RebuildThreads.load(), effective_cores);
    
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
    int wal_interval = ConfigLimits::GetEnvInt(
      "WAL_FLUSH_INTERVAL",
      WALFlushInterval.load(),
      ConfigLimits::WAL_FLUSH_INTERVAL_MIN,
      ConfigLimits::WAL_FLUSH_INTERVAL_MAX
    );
    if (wal_interval != WALFlushInterval.load()) {
      WALFlushInterval.store(wal_interval, std::memory_order_release);
      printf("[Config] Using WAL_FLUSH_INTERVAL=%d seconds from environment\n", wal_interval);
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

    // CRITICAL FIX: Use ConfigLimits range (was hardcoded 0.05-0.5, should be 0.01-0.99)
    double compact_threshold = ConfigLimits::GetEnvDouble(
      "COMPACTION_THRESHOLD",
      CompactionThreshold.load(),
      ConfigLimits::COMPACTION_THRESHOLD_MIN,
      ConfigLimits::COMPACTION_THRESHOLD_MAX
    );
    if (compact_threshold != CompactionThreshold.load()) {
      CompactionThreshold.store(compact_threshold, std::memory_order_release);
      printf("[Config] Using COMPACTION_THRESHOLD=%.2f from environment\n", compact_threshold);
    }

    int compact_interval = ConfigLimits::GetEnvInt(
      "COMPACTION_INTERVAL",
      CompactionInterval.load(),
      ConfigLimits::COMPACTION_INTERVAL_MIN,
      ConfigLimits::COMPACTION_INTERVAL_MAX
    );
    if (compact_interval != CompactionInterval.load()) {
      CompactionInterval.store(compact_interval, std::memory_order_release);
      printf("[Config] Using COMPACTION_INTERVAL=%d seconds from environment\n", compact_interval);
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
    int min_deleted = ConfigLimits::GetEnvInt(
      "MIN_DELETED_VECTORS_FOR_EAGER_COMPACTION",
      MinDeletedVectorsForEagerCompaction.load(),
      ConfigLimits::MIN_DELETED_VECTORS_MIN,
      ConfigLimits::MIN_DELETED_VECTORS_MAX
    );
    if (min_deleted != MinDeletedVectorsForEagerCompaction.load()) {
      MinDeletedVectorsForEagerCompaction.store(min_deleted, std::memory_order_release);
      printf("[Config] Using MIN_DELETED_VECTORS_FOR_EAGER_COMPACTION=%d from environment\n", min_deleted);
    }

    // Check environment variable for initial table capacity
    // This is the recommended way to configure initial capacity (API parameter "vectorScale" is deprecated)
    // Use cases:
    //   - Performance-sensitive: Set higher (e.g., 100000) if you know you'll insert millions of vectors
    //   - Memory-constrained: Keep at default 1000 - tables grow automatically as needed
    int capacity = ConfigLimits::GetEnvInt(
      "INITIAL_TABLE_CAPACITY",
      InitialTableCapacity.load(),
      ConfigLimits::INITIAL_TABLE_CAPACITY_MIN,
      ConfigLimits::INITIAL_TABLE_CAPACITY_MAX
    );
    if (capacity != InitialTableCapacity.load()) {
      InitialTableCapacity.store(capacity, std::memory_order_release);
      printf("[Config] Using INITIAL_TABLE_CAPACITY=%d from environment\n", capacity);
    }

    // Check environment variable for rebuild interval
    int rebuild_interval = ConfigLimits::GetEnvInt(
      "REBUILD_INTERVAL",
      RebuildInterval.load(),
      ConfigLimits::REBUILD_INTERVAL_MIN,
      ConfigLimits::REBUILD_INTERVAL_MAX
    );
    if (rebuild_interval != RebuildInterval.load()) {
      RebuildInterval.store(rebuild_interval, std::memory_order_release);
      printf("[Config] Using REBUILD_INTERVAL=%d milliseconds from environment\n", rebuild_interval);
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
    
    int incremental_threshold = ConfigLimits::GetEnvInt(
      "INCREMENTAL_THRESHOLD",
      IncrementalThreshold.load(),
      ConfigLimits::INCREMENTAL_THRESHOLD_MIN,
      ConfigLimits::INCREMENTAL_THRESHOLD_MAX
    );
    if (incremental_threshold != IncrementalThreshold.load()) {
      IncrementalThreshold.store(incremental_threshold, std::memory_order_release);
      printf("[Config] Using INCREMENTAL_THRESHOLD=%d from environment\n", incremental_threshold);
    }
    
    int full_rebuild_interval = ConfigLimits::GetEnvInt(
      "FULL_REBUILD_INTERVAL",
      FullRebuildInterval.load(),
      ConfigLimits::FULL_REBUILD_INTERVAL_MIN,
      ConfigLimits::FULL_REBUILD_INTERVAL_MAX
    );
    if (full_rebuild_interval != FullRebuildInterval.load()) {
      FullRebuildInterval.store(full_rebuild_interval, std::memory_order_release);
      printf("[Config] Using FULL_REBUILD_INTERVAL=%d from environment\n", full_rebuild_interval);
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
    double compact_before_rebuild = ConfigLimits::GetEnvDouble(
      "COMPACTION_BEFORE_REBUILD_THRESHOLD",
      CompactionBeforeRebuildThreshold.load(),
      ConfigLimits::COMPACTION_BEFORE_REBUILD_MIN,
      ConfigLimits::COMPACTION_BEFORE_REBUILD_MAX
    );
    if (compact_before_rebuild != CompactionBeforeRebuildThreshold.load()) {
      CompactionBeforeRebuildThreshold.store(compact_before_rebuild, std::memory_order_release);
      printf("[Config] Using COMPACTION_BEFORE_REBUILD_THRESHOLD=%.2f from environment\n", compact_before_rebuild);
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
    int rebuild_save_interval = ConfigLimits::GetEnvInt(
      "REBUILD_SAVE_INTERVAL",
      RebuildSaveInterval.load(),
      ConfigLimits::REBUILD_SAVE_INTERVAL_MIN,
      ConfigLimits::REBUILD_SAVE_INTERVAL_MAX
    );
    if (rebuild_save_interval != RebuildSaveInterval.load()) {
      RebuildSaveInterval.store(rebuild_save_interval, std::memory_order_release);
      printf("[Config] Using REBUILD_SAVE_INTERVAL=%d from environment\n", rebuild_save_interval);
    }

    int rebuild_save_seconds = ConfigLimits::GetEnvInt(
      "REBUILD_SAVE_INTERVAL_SECONDS",
      RebuildSaveIntervalSeconds.load(),
      ConfigLimits::REBUILD_SAVE_INTERVAL_SECONDS_MIN,
      ConfigLimits::REBUILD_SAVE_INTERVAL_SECONDS_MAX
    );
    if (rebuild_save_seconds != RebuildSaveIntervalSeconds.load()) {
      RebuildSaveIntervalSeconds.store(rebuild_save_seconds, std::memory_order_release);
      printf("[Config] Using REBUILD_SAVE_INTERVAL_SECONDS=%d from environment\n", rebuild_save_seconds);
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

  // Setter method for NSGSearchLength
  void setNSGSearchLength(int value) {
    if (value >= ConfigLimits::NSG_SEARCH_LENGTH_MIN && value <= ConfigLimits::NSG_SEARCH_LENGTH_MAX) {
      NSGSearchLength.store(value, std::memory_order_release);
    } else {
      throw std::invalid_argument("Invalid value for NSGSearchLength, valid range: [" +
                                  std::to_string(ConfigLimits::NSG_SEARCH_LENGTH_MIN) + ", " +
                                  std::to_string(ConfigLimits::NSG_SEARCH_LENGTH_MAX) + "], 0 = auto");
    }
  }

  // Setter method for NSGOutDegree
  void setNSGOutDegree(int value) {
    if (value >= ConfigLimits::NSG_OUT_DEGREE_MIN && value <= ConfigLimits::NSG_OUT_DEGREE_MAX) {
      NSGOutDegree.store(value, std::memory_order_release);
    } else {
      throw std::invalid_argument("Invalid value for NSGOutDegree, valid range: [" +
                                  std::to_string(ConfigLimits::NSG_OUT_DEGREE_MIN) + ", " +
                                  std::to_string(ConfigLimits::NSG_OUT_DEGREE_MAX) + "]");
    }
  }

  // Setter method for NSGCandidatePoolSize
  void setNSGCandidatePoolSize(int value) {
    if (value >= ConfigLimits::NSG_CANDIDATE_POOL_MIN && value <= ConfigLimits::NSG_CANDIDATE_POOL_MAX) {
      NSGCandidatePoolSize.store(value, std::memory_order_release);
    } else {
      throw std::invalid_argument("Invalid value for NSGCandidatePoolSize, valid range: [" +
                                  std::to_string(ConfigLimits::NSG_CANDIDATE_POOL_MIN) + ", " +
                                  std::to_string(ConfigLimits::NSG_CANDIDATE_POOL_MAX) + "]");
    }
  }

  // Calculate adaptive search_length based on dataset size
  int getAdaptiveSearchLength(size_t dataset_size) const {
    int configured = NSGSearchLength.load(std::memory_order_acquire);

    // If manually configured (non-zero), use configured value
    if (configured > 0) {
      return configured;
    }

    // Auto-adaptive algorithm based on dataset size
    if (dataset_size < 10000) {
      return 40;
    } else if (dataset_size < 50000) {
      return 60;
    } else if (dataset_size < 100000) {
      return 100;
    } else if (dataset_size < 500000) {
      return 150;
    } else if (dataset_size < 1000000) {
      return 200;
    } else {
      return 250;
    }
  }

  // Load full-text search config from environment variables (called at startup)
  void loadFullTextConfigFromEnv() {
    // Check new environment variables first: VECTORDB_FULLTEXT_SEARCH_ENABLE
    const char* enable_env = std::getenv("VECTORDB_FULLTEXT_SEARCH_ENABLE");
    if (enable_env != nullptr) {
      std::string val_str(enable_env);
      std::transform(val_str.begin(), val_str.end(), val_str.begin(), ::tolower);
      if (val_str == "true" || val_str == "1" || val_str == "yes") {
        EnableFullText.store(true, std::memory_order_release);
        printf("[Config] Full-text search integration enabled from VECTORDB_FULLTEXT_SEARCH_ENABLE\n");
      }
    } else {
      // Backward compatibility: check VECTORDB_ENABLE_FULLTEXT
      const char* old_enable_env = std::getenv("VECTORDB_ENABLE_FULLTEXT");
      if (old_enable_env != nullptr) {
        std::string val_str(old_enable_env);
        std::transform(val_str.begin(), val_str.end(), val_str.begin(), ::tolower);
        if (val_str == "true" || val_str == "1" || val_str == "yes") {
          EnableFullText.store(true, std::memory_order_release);
          printf("[Config] Full-text search enabled via deprecated VECTORDB_ENABLE_FULLTEXT\n");
        }
      } else {
        // Backward compatibility: check old VECTORDB_ENABLE_QUICKWIT
        const char* quickwit_enable_env = std::getenv("VECTORDB_ENABLE_QUICKWIT");
        if (quickwit_enable_env != nullptr) {
          std::string val_str(quickwit_enable_env);
          std::transform(val_str.begin(), val_str.end(), val_str.begin(), ::tolower);
          if (val_str == "true" || val_str == "1" || val_str == "yes") {
            EnableFullText.store(true, std::memory_order_release);
            EnableQuickwit.store(true, std::memory_order_release);
            printf("[Config] Full-text search enabled via deprecated VECTORDB_ENABLE_QUICKWIT\n");
          }
        }
      }
    }

    // Engine type: VECTORDB_FULLTEXT_SEARCH_PROVIDER (preferred) or VECTORDB_FULLTEXT_ENGINE (deprecated)
    const char* provider_env = std::getenv("VECTORDB_FULLTEXT_SEARCH_PROVIDER");
    if (provider_env != nullptr) {
      FullTextEngine = std::string(provider_env);
      printf("[Config] Full-text engine set to %s from VECTORDB_FULLTEXT_SEARCH_PROVIDER\n", provider_env);
    } else {
      const char* engine_env = std::getenv("VECTORDB_FULLTEXT_ENGINE");
      if (engine_env != nullptr) {
        FullTextEngine = std::string(engine_env);
        printf("[Config] Full-text engine set to %s from deprecated VECTORDB_FULLTEXT_ENGINE\n", engine_env);
      } else {
        FullTextEngine = "quickwit";  // Default to Quickwit
      }
    }

    // Binary path (for Quickwit)
    const char* binary_env = std::getenv("VECTORDB_FULLTEXT_BINARY");
    if (binary_env != nullptr) {
      FullTextBinaryPath = std::string(binary_env);
      printf("[Config] Full-text binary path set to %s from environment\n", binary_env);
    } else {
      const char* old_binary_env = std::getenv("VECTORDB_QUICKWIT_BINARY");
      if (old_binary_env != nullptr) {
        FullTextBinaryPath = std::string(old_binary_env);
        QuickwitBinaryPath = FullTextBinaryPath;
      } else {
        FullTextBinaryPath = "/usr/local/bin/quickwit";
      }
    }

    // Config path
    const char* config_env = std::getenv("VECTORDB_FULLTEXT_CONFIG");
    if (config_env != nullptr) {
      FullTextConfigPath = std::string(config_env);
      printf("[Config] Full-text config path set to %s from environment\n", config_env);
    } else {
      const char* old_config_env = std::getenv("VECTORDB_QUICKWIT_CONFIG");
      if (old_config_env != nullptr) {
        FullTextConfigPath = std::string(old_config_env);
        QuickwitConfigPath = FullTextConfigPath;
      } else {
        FullTextConfigPath = "";
      }
    }

    // Data directory
    // Priority: QW_DATA_DIR > VECTORDB_FULLTEXT_DATA_DIR > VECTORDB_QUICKWIT_DATA_DIR > default
    const char* qw_data_env = std::getenv("QW_DATA_DIR");
    if (qw_data_env != nullptr) {
      FullTextDataDir = std::string(qw_data_env);
      QuickwitDataDir = FullTextDataDir;
      printf("[Config] Full-text data dir set to %s from QW_DATA_DIR\n", qw_data_env);
    } else {
      const char* data_env = std::getenv("VECTORDB_FULLTEXT_DATA_DIR");
      if (data_env != nullptr) {
        FullTextDataDir = std::string(data_env);
        printf("[Config] Full-text data dir set to %s from VECTORDB_FULLTEXT_DATA_DIR\n", data_env);
      } else {
        const char* old_data_env = std::getenv("VECTORDB_QUICKWIT_DATA_DIR");
        if (old_data_env != nullptr) {
          FullTextDataDir = std::string(old_data_env);
          QuickwitDataDir = FullTextDataDir;
        } else {
          FullTextDataDir = "/tmp/qwdata";  // Updated default path
        }
      }
    }

    // Port
    const char* port_env = std::getenv("VECTORDB_FULLTEXT_PORT");
    if (port_env != nullptr) {
      try {
        int val = std::stoi(port_env);
        if (val > 0 && val < 65536) {
          FullTextPort.store(val, std::memory_order_release);
          printf("[Config] Full-text port set to %d from environment\n", val);
        }
      } catch (...) {}
    } else {
      const char* old_port_env = std::getenv("VECTORDB_QUICKWIT_PORT");
      if (old_port_env != nullptr) {
        try {
          int val = std::stoi(old_port_env);
          if (val > 0 && val < 65536) {
            FullTextPort.store(val, std::memory_order_release);
            QuickwitPort.store(val, std::memory_order_release);
          }
        } catch (...) {}
      }
    }

    // Endpoint (for Elasticsearch/Meilisearch)
    const char* endpoint_env = std::getenv("VECTORDB_FULLTEXT_ENDPOINT");
    if (endpoint_env != nullptr) {
      FullTextEndpoint = std::string(endpoint_env);
      printf("[Config] Full-text endpoint set to %s from environment\n", endpoint_env);
    }

    // API key
    const char* api_key_env = std::getenv("VECTORDB_FULLTEXT_API_KEY");
    if (api_key_env != nullptr) {
      FullTextApiKey = std::string(api_key_env);
      printf("[Config] Full-text API key set from environment\n");
    }
  }

  // Deprecated: use loadFullTextConfigFromEnv() instead
  void loadQuickwitConfigFromEnv() {
    loadFullTextConfigFromEnv();
  }

  // Load NSG config from environment variables (called at startup)
  void loadNSGConfigFromEnv() {
    const char* search_len_env = std::getenv("EPSILLA_NSG_SEARCH_LENGTH");
    if (search_len_env != nullptr) {
      try {
        int val = std::stoi(search_len_env);
        setNSGSearchLength(val);
        // Also update query queue sizes for better recall
        if (val > 0) {
          MasterQueueSize.store(std::max(val, 500), std::memory_order_release);
          LocalQueueSize.store(std::max(val, 500), std::memory_order_release);
          printf("[Config] NSG search_length set to %d from environment (queue sizes updated)\n", val);
        }
      } catch (...) {
        // Ignore invalid env var
      }
    }

    const char* out_degree_env = std::getenv("EPSILLA_NSG_OUT_DEGREE");
    if (out_degree_env != nullptr) {
      try {
        int val = std::stoi(out_degree_env);
        setNSGOutDegree(val);
        printf("[Config] NSG out_degree set to %d from environment\n", val);
      } catch (...) {
        // Ignore invalid env var
      }
    }

    const char* candidate_pool_env = std::getenv("EPSILLA_NSG_CANDIDATE_POOL_SIZE");
    if (candidate_pool_env != nullptr) {
      try {
        int val = std::stoi(candidate_pool_env);
        setNSGCandidatePoolSize(val);
        printf("[Config] NSG candidate_pool_size set to %d from environment\n", val);
      } catch (...) {
        // Ignore invalid env var
      }
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
    // Initial table capacity configuration (also available via INITIAL_TABLE_CAPACITY env var)
    // Recommended for performance-sensitive scenarios with known large datasets
    if (json.HasMember("InitialTableCapacity")) {
      int capacity = json.GetInt("InitialTableCapacity");
      if (capacity >= ConfigLimits::INITIAL_TABLE_CAPACITY_MIN &&
          capacity <= ConfigLimits::INITIAL_TABLE_CAPACITY_MAX) {
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
