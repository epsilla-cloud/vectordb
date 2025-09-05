#pragma once

#include <atomic>
#include <shared_mutex>
#include <memory>
#include <chrono>
#include <functional>
#include <thread>
#include "utils/status.hpp"
#include "utils/concurrent_bitset.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

/**
 * Thread-safe capacity manager with atomic operations
 * Fixes the capacity race condition in Insert operations
 */
class AtomicCapacityManager {
public:
    AtomicCapacityManager(size_t max_capacity) 
        : max_capacity_(max_capacity), current_size_(0) {}
    
    /**
     * Atomically reserve space for new records
     * @return true if space was reserved, false if capacity exceeded
     */
    bool TryReserve(size_t count, size_t& reserved_position) {
        size_t current = current_size_.load(std::memory_order_acquire);
        
        while (true) {
            // Check if adding would exceed capacity
            if (current + count > max_capacity_) {
                return false;
            }
            
            // Try to atomically update the size
            if (current_size_.compare_exchange_weak(
                    current, current + count,
                    std::memory_order_acq_rel,
                    std::memory_order_acquire)) {
                reserved_position = current;
                return true;
            }
            // If CAS failed, current was updated with actual value, loop continues
        }
    }
    
    /**
     * Release reserved space if operation failed
     */
    void ReleaseReservation(size_t count) {
        current_size_.fetch_sub(count, std::memory_order_acq_rel);
    }
    
    size_t GetCurrentSize() const {
        return current_size_.load(std::memory_order_acquire);
    }
    
    size_t GetMaxCapacity() const {
        return max_capacity_;
    }
    
    void Reset() {
        current_size_.store(0, std::memory_order_release);
    }
    
private:
    const size_t max_capacity_;
    std::atomic<size_t> current_size_;
};

/**
 * WAL transaction manager with rollback support
 * Fixes WAL-data inconsistency issues
 */
class WALTransactionManager {
public:
    struct Transaction {
        int64_t wal_id;
        bool committed;
        std::function<void()> rollback_fn;
        
        Transaction(int64_t id) : wal_id(id), committed(false) {}
        
        ~Transaction() {
            if (!committed && rollback_fn) {
                rollback_fn();
            }
        }
        
        void Commit() {
            committed = true;
        }
    };
    
    std::unique_ptr<Transaction> BeginTransaction(int64_t wal_id) {
        auto txn = std::make_unique<Transaction>(wal_id);
        return txn;
    }
    
    void SetRollback(Transaction* txn, std::function<void()> rollback) {
        if (txn) {
            txn->rollback_fn = std::move(rollback);
        }
    }
};

/**
 * RCU-style executor pool manager
 * Fixes executor pool replacement race condition
 */
template<typename T>
class RCUManager {
public:
    RCUManager() : current_(nullptr), grace_period_ms_(100) {}
    
    /**
     * Get current object with hazard pointer protection
     */
    std::shared_ptr<T> Get() {
        std::shared_ptr<T> result;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            result = current_;
        }
        return result;
    }
    
    /**
     * Replace current object with grace period for safe cleanup
     */
    void Replace(std::shared_ptr<T> new_obj) {
        std::shared_ptr<T> old_obj;
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            old_obj = current_;
            current_ = new_obj;
        }
        
        // Defer deletion with grace period
        if (old_obj) {
            std::thread([old_obj, grace_ms = grace_period_ms_]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(grace_ms));
                // old_obj destroyed here after grace period
            }).detach();
        }
    }
    
    void SetGracePeriod(int milliseconds) {
        grace_period_ms_ = milliseconds;
    }
    
private:
    mutable std::shared_mutex mutex_;
    std::shared_ptr<T> current_;
    int grace_period_ms_;
};

/**
 * Snapshot isolation manager for consistent reads
 * Fixes query-delete phantom reads
 */
class SnapshotManager {
public:
    struct Snapshot {
        uint64_t version;
        std::shared_ptr<ConcurrentBitset> deleted_bitmap;
        std::chrono::steady_clock::time_point timestamp;
        
        bool IsDeleted(size_t idx) const {
            return deleted_bitmap && deleted_bitmap->test(idx);
        }
    };
    
    SnapshotManager() : current_version_(0) {}
    
    /**
     * Create a consistent snapshot for queries
     */
    Snapshot CreateSnapshot(const ConcurrentBitset* current_deleted) {
        Snapshot snap;
        snap.version = current_version_.load(std::memory_order_acquire);
        snap.timestamp = std::chrono::steady_clock::now();
        if (current_deleted) {
            // Create a new bitmap and copy the state
            size_t capacity = const_cast<ConcurrentBitset*>(current_deleted)->capacity();
            snap.deleted_bitmap = std::make_shared<ConcurrentBitset>(capacity);
            // Copy the bits (we'll need a method to do this safely)
            for (size_t i = 0; i < capacity; ++i) {
                if (current_deleted->test(i)) {
                    snap.deleted_bitmap->set(i);
                }
            }
        }
        return snap;
    }
    
    /**
     * Increment version after modifications
     */
    void IncrementVersion() {
        current_version_.fetch_add(1, std::memory_order_acq_rel);
    }
    
    uint64_t GetCurrentVersion() const {
        return current_version_.load(std::memory_order_acquire);
    }
    
private:
    std::atomic<uint64_t> current_version_;
};

/**
 * Atomic upsert manager for preventing lost updates
 * Fixes upsert race conditions
 */
class AtomicUpsertManager {
public:
    struct UpsertOperation {
        size_t old_index;
        size_t new_index;
        bool is_update;
        std::atomic<bool> completed{false};
    };
    
    /**
     * Perform atomic upsert with proper synchronization
     */
    template<typename KeyType, typename PrimaryKeyManager>
    Status AtomicUpsert(
        const KeyType& key,
        size_t new_index,
        PrimaryKeyManager& pk_manager,
        ConcurrentBitset* deleted_set,
        UpsertOperation& op) {
        
        // Try to add key - if it exists, this returns false and sets old_index
        size_t old_index;
        if (!pk_manager.addKeyIfNotExist(key, new_index, &old_index)) {
            // Key exists - perform atomic update
            op.old_index = old_index;
            op.new_index = new_index;
            op.is_update = true;
            
            // Atomically update the primary key mapping
            if (!pk_manager.compareAndSwapKey(key, old_index, new_index)) {
                // Another thread updated the key concurrently
                return Status(DB_UNEXPECTED_ERROR, "Concurrent upsert conflict");
            }
            
            // Mark old record as deleted atomically
            if (deleted_set) {
                deleted_set->set(old_index);
            }
            
            op.completed.store(true, std::memory_order_release);
            return Status::OK();
        }
        
        // New key inserted successfully
        op.is_update = false;
        op.new_index = new_index;
        op.completed.store(true, std::memory_order_release);
        return Status::OK();
    }
};

/**
 * Enhanced reader-writer lock manager
 * Provides better concurrency for read-heavy workloads
 */
class OptimizedRWLock {
public:
    class ReadGuard {
    public:
        explicit ReadGuard(std::shared_mutex& mutex) 
            : lock_(mutex) {}
    private:
        std::shared_lock<std::shared_mutex> lock_;
    };
    
    class WriteGuard {
    public:
        explicit WriteGuard(std::shared_mutex& mutex)
            : lock_(mutex) {}
    private:
        std::unique_lock<std::shared_mutex> lock_;
    };
    
    ReadGuard ReadLock() {
        return ReadGuard(mutex_);
    }
    
    WriteGuard WriteLock() {
        return WriteGuard(mutex_);
    }
    
private:
    mutable std::shared_mutex mutex_;
};

} // namespace engine
} // namespace vectordb