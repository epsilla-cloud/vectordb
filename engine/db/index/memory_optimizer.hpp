#pragma once

#include "db/index/base_index.hpp"
#include "logger/logger.hpp"
#include <memory>
#include <chrono>
#include <atomic>

namespace vectordb {
namespace engine {
namespace index {

/**
 * @brief Memory optimization strategy
 */
enum class MemoryOptimizationStrategy {
  CONSERVATIVE = 1,    // Minimize memory footprint
  BALANCED = 2,        // Balance memory vs performance
  AGGRESSIVE = 3,      // Maximum memory efficiency
  ADAPTIVE = 4         // Adapt based on system conditions
};

/**
 * @brief Memory usage profile
 */
struct MemoryProfile {
  // Current usage
  size_t total_memory_bytes = 0;
  size_t resident_memory_bytes = 0;
  size_t virtual_memory_bytes = 0;
  size_t peak_memory_bytes = 0;
  
  // Index-specific breakdown  
  size_t index_data_bytes = 0;
  size_t metadata_bytes = 0;
  size_t cache_bytes = 0;
  size_t temporary_bytes = 0;
  
  // Memory efficiency metrics
  double memory_efficiency = 0.0;  // useful_data / total_memory
  double compression_ratio = 0.0;  // original_size / compressed_size
  
  // Allocation patterns
  size_t allocation_count = 0;
  size_t deallocation_count = 0;
  size_t fragmentation_score = 0;  // 0-100, higher is more fragmented
  
  std::chrono::system_clock::time_point last_updated;
};

/**
 * @brief Memory optimization recommendation
 */
struct MemoryOptimization {
  std::string category;           // "compression", "caching", "allocation", etc.
  std::string description;        // Human-readable description
  double estimated_savings_mb;    // Expected memory savings
  double performance_impact;      // -1.0 to 1.0 (negative = slower, positive = faster)
  int priority;                  // 1-10, higher = more important
  bool requires_rebuild;         // Whether optimization requires index rebuild
};

/**
 * @brief Memory-mapped file manager for indexes
 */
class MemoryMappedStorage {
private:
  std::string file_path_;
  void* mapped_memory_;
  size_t mapped_size_;
  int file_descriptor_;
  mutable Logger logger_;

public:
  explicit MemoryMappedStorage(const std::string& file_path);
  ~MemoryMappedStorage();

  /**
   * @brief Create and map a file of specified size
   */
  bool CreateMapping(size_t size);

  /**
   * @brief Map an existing file
   */
  bool MapExistingFile();

  /**
   * @brief Unmap the file
   */
  void Unmap();

  /**
   * @brief Get mapped memory pointer
   */
  void* GetMappedMemory() const { return mapped_memory_; }

  /**
   * @brief Get mapped size
   */
  size_t GetMappedSize() const { return mapped_size_; }

  /**
   * @brief Sync changes to disk
   */
  bool Sync() const;

  /**
   * @brief Prefault pages to improve access patterns
   */
  void Prefault(size_t offset = 0, size_t size = 0) const;

  /**
   * @brief Advise kernel about access patterns
   */
  void AdviseAccess(int advice) const;  // MADV_SEQUENTIAL, MADV_RANDOM, etc.
};

/**
 * @brief Index memory optimizer
 * 
 * Analyzes and optimizes memory usage for vector indexes
 */
class IndexMemoryOptimizer {
private:
  MemoryOptimizationStrategy strategy_;
  mutable Logger logger_;

  // Memory monitoring
  std::atomic<bool> monitoring_enabled_{false};
  mutable std::mutex profile_mutex_;
  std::unordered_map<BaseIndex*, MemoryProfile> index_profiles_;

public:
  explicit IndexMemoryOptimizer(MemoryOptimizationStrategy strategy = 
                               MemoryOptimizationStrategy::BALANCED);

  /**
   * @brief Optimize memory usage for an index
   */
  std::vector<MemoryOptimization> OptimizeIndex(BaseIndex* index);

  /**
   * @brief Get memory profile for an index
   */
  MemoryProfile GetMemoryProfile(const BaseIndex* index) const;

  /**
   * @brief Monitor memory usage continuously
   */
  void StartMemoryMonitoring();
  void StopMemoryMonitoring();

  /**
   * @brief Apply memory optimizations
   */
  bool ApplyOptimizations(BaseIndex* index, 
                         const std::vector<MemoryOptimization>& optimizations);

  /**
   * @brief Set optimization strategy
   */
  void SetStrategy(MemoryOptimizationStrategy strategy) { 
    strategy_ = strategy; 
  }

  /**
   * @brief Get current system memory status
   */
  static MemoryProfile GetSystemMemoryProfile();

private:
  /**
   * @brief Analyze memory usage patterns
   */
  MemoryProfile AnalyzeMemoryUsage(const BaseIndex* index) const;

  /**
   * @brief Generate compression recommendations
   */
  std::vector<MemoryOptimization> GenerateCompressionOptimizations(
    const BaseIndex* index, 
    const MemoryProfile& profile) const;

  /**
   * @brief Generate caching optimizations
   */
  std::vector<MemoryOptimization> GenerateCachingOptimizations(
    const BaseIndex* index,
    const MemoryProfile& profile) const;

  /**
   * @brief Generate allocation optimizations
   */
  std::vector<MemoryOptimization> GenerateAllocationOptimizations(
    const BaseIndex* index,
    const MemoryProfile& profile) const;

  /**
   * @brief Check if memory-mapped storage would be beneficial
   */
  bool ShouldUseMemoryMapping(const BaseIndex* index,
                             const MemoryProfile& profile) const;

  /**
   * @brief Estimate memory savings from optimization
   */
  double EstimateMemorySavings(const MemoryOptimization& optimization,
                              const MemoryProfile& profile) const;

  /**
   * @brief Monitor memory usage in background thread
   */
  void MemoryMonitorLoop();
};

/**
 * @brief Smart memory allocator for indexes
 * 
 * Custom allocator that optimizes memory layout and reduces fragmentation
 */
template<typename T>
class IndexMemoryAllocator {
private:
  // Memory pool for reducing allocation overhead
  struct MemoryPool {
    void* pool_start;
    size_t pool_size;
    size_t used_size;
    std::mutex pool_mutex;
  };
  
  static thread_local std::unique_ptr<MemoryPool> local_pool_;
  static constexpr size_t POOL_SIZE = 64 * 1024 * 1024;  // 64MB per thread

public:
  using value_type = T;

  /**
   * @brief Allocate memory with optimization
   */
  T* allocate(size_t n);

  /**
   * @brief Deallocate memory
   */
  void deallocate(T* p, size_t n);

  /**
   * @brief Allocate aligned memory
   */
  T* allocate_aligned(size_t n, size_t alignment = 32);

  /**
   * @brief Bulk allocate for better cache locality
   */
  std::vector<T*> bulk_allocate(const std::vector<size_t>& sizes);

private:
  /**
   * @brief Initialize thread-local memory pool
   */
  void InitializePool();

  /**
   * @brief Try to allocate from pool
   */
  T* TryPoolAllocate(size_t n);

  /**
   * @brief Fallback to system allocation
   */
  T* SystemAllocate(size_t n);
};

/**
 * @brief Memory compression utilities for indexes
 */
class IndexMemoryCompressor {
public:
  /**
   * @brief Compress vector data using quantization
   */
  static std::vector<uint8_t> CompressVectors(
    const float* vectors,
    size_t count,
    size_t dimension,
    double quality = 0.95);

  /**
   * @brief Decompress vector data
   */
  static std::vector<float> DecompressVectors(
    const std::vector<uint8_t>& compressed_data,
    size_t count,
    size_t dimension);

  /**
   * @brief Compress graph adjacency lists
   */
  static std::vector<uint8_t> CompressGraph(
    const std::vector<std::vector<int64_t>>& adjacency_lists);

  /**
   * @brief Decompress graph data
   */
  static std::vector<std::vector<int64_t>> DecompressGraph(
    const std::vector<uint8_t>& compressed_data);

  /**
   * @brief Estimate compression ratio for data
   */
  static double EstimateCompressionRatio(
    const void* data,
    size_t size,
    const std::string& algorithm = "lz4");
};

} // namespace index
} // namespace engine
} // namespace vectordb