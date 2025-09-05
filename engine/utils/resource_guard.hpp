#pragma once

#include <fstream>
#include <memory>
#include <functional>
#include <mutex>
#include <unordered_map>
#include "logger/logger.hpp"

namespace vectordb {
namespace utils {

/**
 * @brief RAII guard for file handles to prevent leaks
 */
class FileGuard {
public:
    explicit FileGuard(const std::string& path, std::ios::openmode mode = std::ios::in)
        : path_(path) {
        file_.open(path, mode);
        if (!file_.is_open()) {
            throw std::runtime_error("Failed to open file: " + path);
        }
    }
    
    ~FileGuard() {
        if (file_.is_open()) {
            file_.close();
        }
    }
    
    // Delete copy operations
    FileGuard(const FileGuard&) = delete;
    FileGuard& operator=(const FileGuard&) = delete;
    
    // Allow move operations
    FileGuard(FileGuard&& other) noexcept 
        : file_(std::move(other.file_)), path_(std::move(other.path_)) {}
    
    FileGuard& operator=(FileGuard&& other) noexcept {
        if (this != &other) {
            if (file_.is_open()) {
                file_.close();
            }
            file_ = std::move(other.file_);
            path_ = std::move(other.path_);
        }
        return *this;
    }
    
    std::fstream& get() { return file_; }
    const std::fstream& get() const { return file_; }
    
    bool is_open() const { return file_.is_open(); }
    const std::string& path() const { return path_; }
    
private:
    std::fstream file_;
    std::string path_;
};

/**
 * @brief RAII guard for any resource with custom deleter
 */
template<typename T>
class ResourceGuard {
public:
    using Deleter = std::function<void(T*)>;
    
    ResourceGuard(T* resource, Deleter deleter)
        : resource_(resource), deleter_(deleter) {}
    
    ~ResourceGuard() {
        if (resource_ && deleter_) {
            deleter_(resource_);
        }
    }
    
    // Delete copy operations
    ResourceGuard(const ResourceGuard&) = delete;
    ResourceGuard& operator=(const ResourceGuard&) = delete;
    
    // Allow move operations
    ResourceGuard(ResourceGuard&& other) noexcept
        : resource_(other.resource_), deleter_(std::move(other.deleter_)) {
        other.resource_ = nullptr;
    }
    
    ResourceGuard& operator=(ResourceGuard&& other) noexcept {
        if (this != &other) {
            if (resource_ && deleter_) {
                deleter_(resource_);
            }
            resource_ = other.resource_;
            deleter_ = std::move(other.deleter_);
            other.resource_ = nullptr;
        }
        return *this;
    }
    
    T* get() { return resource_; }
    const T* get() const { return resource_; }
    
    T* release() {
        T* temp = resource_;
        resource_ = nullptr;
        return temp;
    }
    
    void reset(T* new_resource = nullptr) {
        if (resource_ && deleter_) {
            deleter_(resource_);
        }
        resource_ = new_resource;
    }
    
private:
    T* resource_;
    Deleter deleter_;
};

/**
 * @brief Memory pool with leak detection
 */
class LeakDetectingMemoryPool {
public:
    LeakDetectingMemoryPool(size_t block_size = 4096, size_t max_blocks = 1000)
        : block_size_(block_size), max_blocks_(max_blocks), allocated_blocks_(0) {
        logger_.Info("Memory pool initialized: block_size=" + std::to_string(block_size) +
                    ", max_blocks=" + std::to_string(max_blocks));
    }
    
    ~LeakDetectingMemoryPool() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Check for leaks
        if (!allocations_.empty()) {
            logger_.Warning("Memory leak detected: " + 
                          std::to_string(allocations_.size()) + " blocks not freed");
            
            // Clean up leaked memory
            for (auto& pair : allocations_) {
                std::free(pair.first);
            }
        }
        
        // Free pool memory
        for (void* block : free_blocks_) {
            std::free(block);
        }
    }
    
    void* allocate(size_t size) {
        if (size > block_size_) {
            throw std::bad_alloc();  // Request too large
        }
        
        std::lock_guard<std::mutex> lock(mutex_);
        
        void* ptr = nullptr;
        
        if (!free_blocks_.empty()) {
            // Reuse from pool
            ptr = free_blocks_.back();
            free_blocks_.pop_back();
        } else {
            // Allocate new block
            if (allocated_blocks_ >= max_blocks_) {
                throw std::bad_alloc();  // Pool exhausted
            }
            
            ptr = std::aligned_alloc(64, block_size_);  // 64-byte aligned
            if (!ptr) {
                throw std::bad_alloc();
            }
            
            allocated_blocks_++;
        }
        
        // Track allocation
        allocations_[ptr] = size;
        
        return ptr;
    }
    
    void deallocate(void* ptr) {
        if (!ptr) return;
        
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = allocations_.find(ptr);
        if (it == allocations_.end()) {
            logger_.Error("Attempted to free untracked pointer");
            return;
        }
        
        // Remove from tracking
        allocations_.erase(it);
        
        // Return to pool
        if (free_blocks_.size() < max_blocks_ / 2) {
            std::memset(ptr, 0, block_size_);  // Clear memory
            free_blocks_.push_back(ptr);
        } else {
            // Pool has enough free blocks, actually free this one
            std::free(ptr);
            allocated_blocks_--;
        }
    }
    
    size_t allocated_count() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return allocations_.size();
    }
    
    size_t pool_size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return free_blocks_.size();
    }
    
    void check_leaks() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!allocations_.empty()) {
            logger_.Warning("Active allocations: " + 
                          std::to_string(allocations_.size()));
            for (const auto& pair : allocations_) {
                logger_.Debug("  Leaked block: " + 
                            std::to_string(reinterpret_cast<uintptr_t>(pair.first)) +
                            ", size: " + std::to_string(pair.second));
            }
        }
    }
    
private:
    const size_t block_size_;
    const size_t max_blocks_;
    std::atomic<size_t> allocated_blocks_;
    
    mutable std::mutex mutex_;
    std::unordered_map<void*, size_t> allocations_;  // Track active allocations
    std::vector<void*> free_blocks_;                  // Pool of free blocks
    
    Logger logger_;
};

/**
 * @brief Scoped lock guard with timeout
 */
template<typename Mutex>
class TimedLockGuard {
public:
    TimedLockGuard(Mutex& mutex, std::chrono::milliseconds timeout)
        : mutex_(mutex), owns_lock_(false) {
        
        if constexpr (std::is_same_v<Mutex, std::timed_mutex> || 
                     std::is_same_v<Mutex, std::recursive_timed_mutex>) {
            owns_lock_ = mutex_.try_lock_for(timeout);
            if (!owns_lock_) {
                throw std::runtime_error("Failed to acquire lock within timeout");
            }
        } else {
            mutex_.lock();
            owns_lock_ = true;
        }
    }
    
    ~TimedLockGuard() {
        if (owns_lock_) {
            mutex_.unlock();
        }
    }
    
    // Delete copy operations
    TimedLockGuard(const TimedLockGuard&) = delete;
    TimedLockGuard& operator=(const TimedLockGuard&) = delete;
    
    bool owns_lock() const { return owns_lock_; }
    
private:
    Mutex& mutex_;
    bool owns_lock_;
};

} // namespace utils
} // namespace vectordb