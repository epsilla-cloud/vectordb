#pragma once

#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>
#include <unordered_map>
#include <vector>
#include <list>
#include <algorithm>

#include "logger/logger.hpp"

namespace vectordb {
namespace utils {

/**
 * @brief Memory pool configuration
 */
struct MemoryPoolConfig {
    // Initial pool size in bytes
    size_t initial_size = 64 * 1024 * 1024;  // 64MB
    
    // Maximum pool size in bytes
    size_t max_size = 1024 * 1024 * 1024;  // 1GB
    
    // Chunk sizes to pre-allocate
    std::vector<size_t> chunk_sizes = {
        64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536
    };
    
    // Number of chunks per size to pre-allocate
    size_t chunks_per_size = 100;
    
    // Enable thread-local caching
    bool enable_thread_cache = true;
    
    // Thread cache size per thread
    size_t thread_cache_size = 1024 * 1024;  // 1MB per thread
    
    // Enable memory alignment
    bool enable_alignment = true;
    
    // Alignment boundary
    size_t alignment = 64;  // Cache line size
    
    // Enable statistics tracking
    bool enable_stats = true;
    
    // Enable memory zeroing on allocation
    bool zero_memory = false;
    
    // Enable overflow detection
    bool enable_overflow_detection = true;
};

/**
 * @brief Memory pool statistics
 */
struct MemoryPoolStats {
    std::atomic<size_t> total_allocated{0};
    std::atomic<size_t> total_freed{0};
    std::atomic<size_t> current_usage{0};
    std::atomic<size_t> peak_usage{0};
    std::atomic<size_t> allocation_count{0};
    std::atomic<size_t> free_count{0};
    std::atomic<size_t> cache_hits{0};
    std::atomic<size_t> cache_misses{0};
    
    // Delete copy constructor and assignment operator
    MemoryPoolStats() = default;
    MemoryPoolStats(const MemoryPoolStats&) = delete;
    MemoryPoolStats& operator=(const MemoryPoolStats&) = delete;
    
    double GetCacheHitRate() const {
        size_t total = cache_hits + cache_misses;
        return total > 0 ? static_cast<double>(cache_hits) / total : 0.0;
    }
    
    double GetFragmentation() const {
        size_t allocated = total_allocated;
        size_t usage = current_usage;
        return allocated > 0 ? 1.0 - (static_cast<double>(usage) / allocated) : 0.0;
    }
};

/**
 * @brief Memory block header for tracking
 */
struct BlockHeader {
    size_t size;
    size_t actual_size;  // Including padding
    bool in_use;
    uint32_t magic;  // For overflow detection
    
    static constexpr uint32_t MAGIC_VALUE = 0xDEADBEEF;
};

/**
 * @brief Memory chunk for fixed-size allocations
 */
class MemoryChunk {
public:
    MemoryChunk(size_t block_size, size_t num_blocks, bool zero_memory = false)
        : block_size_(block_size), 
          num_blocks_(num_blocks),
          zero_memory_(zero_memory) {
        
        // Allocate contiguous memory for all blocks
        size_t total_size = block_size_ * num_blocks_;
        memory_ = std::aligned_alloc(64, total_size);
        
        if (!memory_) {
            throw std::bad_alloc();
        }
        
        if (zero_memory_) {
            std::memset(memory_, 0, total_size);
        }
        
        // Initialize free list
        free_blocks_.reserve(num_blocks_);
        char* ptr = static_cast<char*>(memory_);
        
        for (size_t i = 0; i < num_blocks_; ++i) {
            free_blocks_.push_back(ptr + i * block_size_);
        }
    }
    
    ~MemoryChunk() {
        if (memory_) {
            std::free(memory_);
        }
    }
    
    void* Allocate() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (free_blocks_.empty()) {
            return nullptr;
        }
        
        void* block = free_blocks_.back();
        free_blocks_.pop_back();
        used_blocks_++;
        
        if (zero_memory_) {
            std::memset(block, 0, block_size_);
        }
        
        return block;
    }
    
    bool Deallocate(void* ptr) {
        if (!Contains(ptr)) {
            return false;
        }
        
        std::lock_guard<std::mutex> lock(mutex_);
        free_blocks_.push_back(ptr);
        used_blocks_--;
        return true;
    }
    
    bool Contains(void* ptr) const {
        char* p = static_cast<char*>(ptr);
        char* start = static_cast<char*>(memory_);
        char* end = start + block_size_ * num_blocks_;
        return p >= start && p < end;
    }
    
    size_t GetBlockSize() const { return block_size_; }
    size_t GetUsedBlocks() const { return used_blocks_; }
    size_t GetFreeBlocks() const { return free_blocks_.size(); }
    
private:
    void* memory_;
    size_t block_size_;
    size_t num_blocks_;
    bool zero_memory_;
    
    std::vector<void*> free_blocks_;
    std::atomic<size_t> used_blocks_{0};
    mutable std::mutex mutex_;
};

/**
 * @brief Thread-local memory cache
 */
class ThreadCache {
public:
    ThreadCache(size_t max_size) : max_size_(max_size), current_size_(0) {}
    
    void* Get(size_t size) {
        auto it = cache_.find(size);
        if (it != cache_.end() && !it->second.empty()) {
            void* ptr = it->second.back();
            it->second.pop_back();
            current_size_ -= size;
            return ptr;
        }
        return nullptr;
    }
    
    bool Put(void* ptr, size_t size) {
        if (current_size_ + size > max_size_) {
            return false;  // Cache full
        }
        
        cache_[size].push_back(ptr);
        current_size_ += size;
        return true;
    }
    
    void Clear() {
        cache_.clear();
        current_size_ = 0;
    }
    
private:
    std::unordered_map<size_t, std::vector<void*>> cache_;
    size_t max_size_;
    size_t current_size_;
};

/**
 * @brief High-performance memory pool
 * 
 * Features:
 * - Fixed-size chunk allocation for common sizes
 * - Thread-local caching to reduce contention
 * - Memory alignment support
 * - Overflow detection
 * - Statistics tracking
 */
class MemoryPool {
public:
    static MemoryPool& GetInstance() {
        static MemoryPool instance;
        return instance;
    }
    
    /**
     * @brief Initialize the memory pool
     */
    void Initialize(const MemoryPoolConfig& config = MemoryPoolConfig()) {
        std::lock_guard<std::mutex> lock(init_mutex_);
        
        if (initialized_) {
            return;
        }
        
        config_ = config;
        
        // Pre-allocate chunks for common sizes
        for (size_t size : config_.chunk_sizes) {
            size_t actual_size = size;
            if (config_.enable_alignment) {
                actual_size = AlignSize(size);
            }
            
            auto chunk = std::make_unique<MemoryChunk>(
                actual_size, config_.chunks_per_size, config_.zero_memory);
            
            chunk_pools_[size] = std::move(chunk);
        }
        
        initialized_ = true;
        logger_.Info("Memory pool initialized with " + 
                    std::to_string(config_.chunk_sizes.size()) + " chunk sizes");
    }
    
    /**
     * @brief Allocate memory from the pool
     */
    void* Allocate(size_t size) {
        if (!initialized_) {
            Initialize();
        }
        
        if (size == 0) {
            return nullptr;
        }
        
        // Add space for header if overflow detection is enabled
        size_t actual_size = size;
        if (config_.enable_overflow_detection) {
            actual_size += sizeof(BlockHeader) + sizeof(uint32_t);  // Header + footer
        }
        
        void* ptr = nullptr;
        
        // Try thread-local cache first
        if (config_.enable_thread_cache) {
            auto& cache = GetThreadCache();
            ptr = cache.Get(actual_size);
            if (ptr) {
                stats_.cache_hits++;
            } else {
                stats_.cache_misses++;
            }
        }
        
        // Try fixed-size pools
        if (!ptr) {
            ptr = AllocateFromChunks(actual_size);
        }
        
        // Fall back to system allocator for large or unusual sizes
        if (!ptr) {
            ptr = AllocateFromSystem(actual_size);
        }
        
        if (!ptr) {
            return nullptr;
        }
        
        // Setup header if overflow detection is enabled
        if (config_.enable_overflow_detection) {
            SetupBlockHeader(ptr, size, actual_size);
            ptr = static_cast<char*>(ptr) + sizeof(BlockHeader);
        }
        
        // Update statistics
        stats_.allocation_count++;
        stats_.total_allocated += actual_size;
        stats_.current_usage += actual_size;
        
        size_t current = stats_.current_usage.load();
        size_t peak = stats_.peak_usage.load();
        while (current > peak && !stats_.peak_usage.compare_exchange_weak(peak, current)) {
            // Keep trying until we update peak
        }
        
        return ptr;
    }
    
    /**
     * @brief Deallocate memory back to the pool
     */
    void Deallocate(void* ptr) {
        if (!ptr) {
            return;
        }
        
        void* actual_ptr = ptr;
        size_t size = 0;
        
        // Check overflow detection
        if (config_.enable_overflow_detection) {
            actual_ptr = static_cast<char*>(ptr) - sizeof(BlockHeader);
            if (!ValidateBlock(actual_ptr, size)) {
                logger_.Error("Memory corruption detected!");
                // In production, might want to abort or throw
                return;
            }
        }
        
        // Try to return to thread cache
        if (config_.enable_thread_cache && size > 0 && size <= 65536) {
            auto& cache = GetThreadCache();
            if (cache.Put(actual_ptr, size)) {
                return;
            }
        }
        
        // Try to return to chunk pools
        if (DeallocateToChunks(actual_ptr)) {
            stats_.free_count++;
            stats_.total_freed += size;
            stats_.current_usage -= size;
            return;
        }
        
        // Return to system
        DeallocateToSystem(actual_ptr);
        
        stats_.free_count++;
        stats_.total_freed += size;
        stats_.current_usage -= size;
    }
    
    /**
     * @brief Reallocate memory
     */
    void* Reallocate(void* ptr, size_t new_size) {
        if (!ptr) {
            return Allocate(new_size);
        }
        
        if (new_size == 0) {
            Deallocate(ptr);
            return nullptr;
        }
        
        // Get old size from header
        size_t old_size = 0;
        if (config_.enable_overflow_detection) {
            void* actual_ptr = static_cast<char*>(ptr) - sizeof(BlockHeader);
            BlockHeader* header = static_cast<BlockHeader*>(actual_ptr);
            old_size = header->size;
        }
        
        // Allocate new block
        void* new_ptr = Allocate(new_size);
        if (!new_ptr) {
            return nullptr;
        }
        
        // Copy old data
        if (old_size > 0) {
            std::memcpy(new_ptr, ptr, std::min(old_size, new_size));
        }
        
        // Free old block
        Deallocate(ptr);
        
        return new_ptr;
    }
    
    /**
     * @brief Get pool statistics (returns a snapshot copy)
     */
    void GetStats(MemoryPoolStats& out_stats) const {
        out_stats.total_allocated.store(stats_.total_allocated.load());
        out_stats.total_freed.store(stats_.total_freed.load());
        out_stats.current_usage.store(stats_.current_usage.load());
        out_stats.peak_usage.store(stats_.peak_usage.load());
        out_stats.allocation_count.store(stats_.allocation_count.load());
        out_stats.free_count.store(stats_.free_count.load());
        out_stats.cache_hits.store(stats_.cache_hits.load());
        out_stats.cache_misses.store(stats_.cache_misses.load());
    }
    
    /**
     * @brief Reset the pool (free all memory)
     */
    void Reset() {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        
        // Clear thread caches
        thread_caches_.clear();
        
        // Clear chunk pools
        chunk_pools_.clear();
        
        // Clear large allocations
        large_allocations_.clear();
        
        // Reset stats (manually reset each atomic)
        stats_.total_allocated.store(0);
        stats_.total_freed.store(0);
        stats_.current_usage.store(0);
        stats_.peak_usage.store(0);
        stats_.allocation_count.store(0);
        stats_.free_count.store(0);
        stats_.cache_hits.store(0);
        stats_.cache_misses.store(0);
        
        // Reinitialize if needed
        if (initialized_) {
            initialized_ = false;
            Initialize(config_);
        }
    }
    
    /**
     * @brief Destructor
     */
    ~MemoryPool() {
        Reset();
    }
    
private:
    MemoryPool() = default;
    
    MemoryPool(const MemoryPool&) = delete;
    MemoryPool& operator=(const MemoryPool&) = delete;
    
    /**
     * @brief Align size to configured boundary
     */
    size_t AlignSize(size_t size) const {
        if (config_.alignment == 0) {
            return size;
        }
        return (size + config_.alignment - 1) & ~(config_.alignment - 1);
    }
    
    /**
     * @brief Get thread-local cache
     */
    ThreadCache& GetThreadCache() {
        static thread_local ThreadCache cache(config_.thread_cache_size);
        return cache;
    }
    
    /**
     * @brief Allocate from fixed-size chunks
     */
    void* AllocateFromChunks(size_t size) {
        // Find the smallest chunk size that fits
        std::lock_guard<std::mutex> lock(pool_mutex_);
        
        for (const auto& chunk_size : config_.chunk_sizes) {
            if (chunk_size >= size) {
                auto it = chunk_pools_.find(chunk_size);
                if (it != chunk_pools_.end()) {
                    void* ptr = it->second->Allocate();
                    if (ptr) {
                        return ptr;
                    }
                }
            }
        }
        
        return nullptr;
    }
    
    /**
     * @brief Deallocate to fixed-size chunks
     */
    bool DeallocateToChunks(void* ptr) {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        
        for (auto& [size, chunk] : chunk_pools_) {
            if (chunk->Contains(ptr)) {
                return chunk->Deallocate(ptr);
            }
        }
        
        return false;
    }
    
    /**
     * @brief Allocate from system
     */
    void* AllocateFromSystem(size_t size) {
        void* ptr = nullptr;
        
        if (config_.enable_alignment && config_.alignment > 0) {
            ptr = std::aligned_alloc(config_.alignment, AlignSize(size));
        } else {
            ptr = std::malloc(size);
        }
        
        if (ptr && config_.zero_memory) {
            std::memset(ptr, 0, size);
        }
        
        if (ptr) {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            large_allocations_[ptr] = size;
        }
        
        return ptr;
    }
    
    /**
     * @brief Deallocate to system
     */
    void DeallocateToSystem(void* ptr) {
        {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            large_allocations_.erase(ptr);
        }
        
        std::free(ptr);
    }
    
    /**
     * @brief Setup block header for overflow detection
     */
    void SetupBlockHeader(void* ptr, size_t size, size_t actual_size) {
        BlockHeader* header = static_cast<BlockHeader*>(ptr);
        header->size = size;
        header->actual_size = actual_size;
        header->in_use = true;
        header->magic = BlockHeader::MAGIC_VALUE;
        
        // Add footer magic
        uint32_t* footer = reinterpret_cast<uint32_t*>(
            static_cast<char*>(ptr) + actual_size - sizeof(uint32_t));
        *footer = BlockHeader::MAGIC_VALUE;
    }
    
    /**
     * @brief Validate block for overflow detection
     */
    bool ValidateBlock(void* ptr, size_t& size) {
        BlockHeader* header = static_cast<BlockHeader*>(ptr);
        
        // Check header magic
        if (header->magic != BlockHeader::MAGIC_VALUE) {
            return false;
        }
        
        // Check footer magic
        uint32_t* footer = reinterpret_cast<uint32_t*>(
            static_cast<char*>(ptr) + header->actual_size - sizeof(uint32_t));
        if (*footer != BlockHeader::MAGIC_VALUE) {
            return false;
        }
        
        size = header->actual_size;
        return true;
    }
    
    MemoryPoolConfig config_;
    bool initialized_ = false;
    std::mutex init_mutex_;
    
    mutable std::mutex pool_mutex_;
    std::unordered_map<size_t, std::unique_ptr<MemoryChunk>> chunk_pools_;
    std::unordered_map<void*, size_t> large_allocations_;
    
    static thread_local std::unique_ptr<ThreadCache> thread_cache_;
    std::unordered_map<std::thread::id, std::unique_ptr<ThreadCache>> thread_caches_;
    
    MemoryPoolStats stats_;
    engine::Logger logger_;
};

/**
 * @brief Allocator adapter for STL containers
 */
template<typename T>
class PoolAllocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    
    template<typename U>
    struct rebind {
        using other = PoolAllocator<U>;
    };
    
    PoolAllocator() noexcept = default;
    
    template<typename U>
    PoolAllocator(const PoolAllocator<U>&) noexcept {}
    
    pointer allocate(size_type n) {
        void* ptr = MemoryPool::GetInstance().Allocate(n * sizeof(T));
        if (!ptr) {
            throw std::bad_alloc();
        }
        return static_cast<pointer>(ptr);
    }
    
    void deallocate(pointer p, size_type) noexcept {
        MemoryPool::GetInstance().Deallocate(p);
    }
    
    template<typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        new(p) U(std::forward<Args>(args)...);
    }
    
    template<typename U>
    void destroy(U* p) {
        p->~U();
    }
};

template<typename T, typename U>
bool operator==(const PoolAllocator<T>&, const PoolAllocator<U>&) noexcept {
    return true;
}

template<typename T, typename U>
bool operator!=(const PoolAllocator<T>&, const PoolAllocator<U>&) noexcept {
    return false;
}

} // namespace utils
} // namespace vectordb