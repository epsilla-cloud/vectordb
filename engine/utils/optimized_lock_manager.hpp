#pragma once

#include <shared_mutex>
#include <mutex>
#include <atomic>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <vector>
#include "logger/logger.hpp"

namespace vectordb {
namespace utils {

/**
 * @brief Optimistic reader-writer lock with minimal contention
 * 
 * Uses version counter for optimistic reads
 */
class OptimisticRWLock {
public:
    OptimisticRWLock() : version_(0) {}
    
    /**
     * @brief Begin optimistic read - no lock needed
     */
    uint64_t beginRead() const {
        return version_.load(std::memory_order_acquire);
    }
    
    /**
     * @brief Validate read - check if version changed
     */
    bool validateRead(uint64_t expected_version) const {
        return version_.load(std::memory_order_acquire) == expected_version;
    }
    
    /**
     * @brief Pessimistic read lock - fallback when optimistic fails
     */
    void lockRead() {
        mutex_.lock_shared();
    }
    
    void unlockRead() {
        mutex_.unlock_shared();
    }
    
    /**
     * @brief Write lock with version increment
     */
    void lockWrite() {
        mutex_.lock();
        version_.fetch_add(1, std::memory_order_release);
    }
    
    void unlockWrite() {
        version_.fetch_add(1, std::memory_order_release);
        mutex_.unlock();
    }
    
private:
    mutable std::shared_mutex mutex_;
    mutable std::atomic<uint64_t> version_;
};

/**
 * @brief Sharded lock manager to reduce contention
 * 
 * Splits a single lock into multiple shards based on key hash
 */
template<typename Key>
class ShardedLockManager {
public:
    explicit ShardedLockManager(size_t num_shards = 16) 
        : num_shards_(num_shards), locks_(num_shards) {
        logger_.Info("Initialized sharded lock manager with " + 
                    std::to_string(num_shards) + " shards");
    }
    
    /**
     * @brief Get the lock for a specific key
     */
    std::shared_mutex& getLock(const Key& key) {
        size_t shard = std::hash<Key>{}(key) % num_shards_;
        return locks_[shard];
    }
    
    /**
     * @brief Lock for reading with key
     */
    void lockShared(const Key& key) {
        getLock(key).lock_shared();
    }
    
    void unlockShared(const Key& key) {
        getLock(key).unlock_shared();
    }
    
    /**
     * @brief Lock for writing with key
     */
    void lock(const Key& key) {
        getLock(key).lock();
    }
    
    void unlock(const Key& key) {
        getLock(key).unlock();
    }
    
    /**
     * @brief RAII shared lock guard for a key
     */
    class SharedLockGuard {
    public:
        SharedLockGuard(ShardedLockManager& manager, const Key& key)
            : manager_(manager), key_(key) {
            manager_.lockShared(key_);
        }
        
        ~SharedLockGuard() {
            manager_.unlockShared(key_);
        }
        
    private:
        ShardedLockManager& manager_;
        Key key_;
    };
    
    /**
     * @brief RAII unique lock guard for a key
     */
    class UniqueLockGuard {
    public:
        UniqueLockGuard(ShardedLockManager& manager, const Key& key)
            : manager_(manager), key_(key) {
            manager_.lock(key_);
        }
        
        ~UniqueLockGuard() {
            manager_.unlock(key_);
        }
        
    private:
        ShardedLockManager& manager_;
        Key key_;
    };
    
private:
    const size_t num_shards_;
    std::vector<std::shared_mutex> locks_;
    Logger logger_;
};

/**
 * @brief Spin lock for very short critical sections
 * 
 * More efficient than mutex for short operations
 */
class SpinLock {
public:
    SpinLock() : locked_(false) {}
    
    void lock() {
        // Exponential backoff
        size_t backoff = 1;
        while (locked_.exchange(true, std::memory_order_acquire)) {
            for (size_t i = 0; i < backoff; ++i) {
                std::this_thread::yield();
            }
            backoff = std::min(backoff * 2, size_t(32));
        }
    }
    
    void unlock() {
        locked_.store(false, std::memory_order_release);
    }
    
    bool try_lock() {
        return !locked_.exchange(true, std::memory_order_acquire);
    }
    
private:
    std::atomic<bool> locked_;
};

/**
 * @brief Read-Copy-Update (RCU) for lock-free reads
 * 
 * Readers never block, writers update a copy
 */
template<typename T>
class RCUProtected {
public:
    RCUProtected() : data_(std::make_shared<T>()) {}
    
    explicit RCUProtected(T initial_value) 
        : data_(std::make_shared<T>(std::move(initial_value))) {}
    
    /**
     * @brief Read data without locking
     */
    std::shared_ptr<const T> read() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return data_;
    }
    
    /**
     * @brief Update data (creates a copy)
     */
    template<typename Updater>
    void update(Updater updater) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        
        // Create a copy
        auto new_data = std::make_shared<T>(*data_);
        
        // Update the copy
        updater(*new_data);
        
        // Atomically replace
        data_ = new_data;
    }
    
    /**
     * @brief Replace data entirely
     */
    void replace(T new_value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_ = std::make_shared<T>(std::move(new_value));
    }
    
private:
    mutable std::shared_mutex mutex_;
    std::shared_ptr<T> data_;
};

/**
 * @brief Hierarchical lock to prevent deadlocks
 * 
 * Enforces lock ordering by hierarchy level
 */
class HierarchicalMutex {
public:
    explicit HierarchicalMutex(unsigned level)
        : hierarchy_level_(level), previous_level_(0) {}
    
    void lock() {
        checkForHierarchyViolation();
        internal_mutex_.lock();
        updateHierarchyLevel();
    }
    
    void unlock() {
        if (thread_hierarchy_level_ != hierarchy_level_) {
            throw std::logic_error("Mutex hierarchy violated on unlock");
        }
        thread_hierarchy_level_ = previous_level_;
        internal_mutex_.unlock();
    }
    
    bool try_lock() {
        checkForHierarchyViolation();
        if (!internal_mutex_.try_lock()) {
            return false;
        }
        updateHierarchyLevel();
        return true;
    }
    
private:
    void checkForHierarchyViolation() {
        if (thread_hierarchy_level_ <= hierarchy_level_) {
            throw std::logic_error("Mutex hierarchy violated: tried to lock level " +
                                  std::to_string(hierarchy_level_) + 
                                  " while holding level " +
                                  std::to_string(thread_hierarchy_level_));
        }
    }
    
    void updateHierarchyLevel() {
        previous_level_ = thread_hierarchy_level_;
        thread_hierarchy_level_ = hierarchy_level_;
    }
    
    std::mutex internal_mutex_;
    const unsigned hierarchy_level_;
    unsigned previous_level_;
    
    static thread_local unsigned thread_hierarchy_level_;
};

// Initialize thread-local storage
template<>
thread_local unsigned HierarchicalMutex::thread_hierarchy_level_ = UINT_MAX;

/**
 * @brief Lock-free statistics counter
 */
class LockFreeCounter {
public:
    LockFreeCounter() : value_(0) {}
    
    void increment() {
        value_.fetch_add(1, std::memory_order_relaxed);
    }
    
    void add(int64_t delta) {
        value_.fetch_add(delta, std::memory_order_relaxed);
    }
    
    int64_t get() const {
        return value_.load(std::memory_order_relaxed);
    }
    
    void reset() {
        value_.store(0, std::memory_order_relaxed);
    }
    
private:
    std::atomic<int64_t> value_;
};

} // namespace utils
} // namespace vectordb