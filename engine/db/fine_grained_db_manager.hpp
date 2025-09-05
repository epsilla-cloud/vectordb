#pragma once

#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>
#include <string>
#include <atomic>
#include "db/db_mvp.hpp"
#include "utils/concurrent_unordered_map.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

/**
 * @brief Fine-grained database manager with per-database locking
 * 
 * This class replaces the global database lock with per-database locks,
 * significantly reducing lock contention for multi-database operations.
 */
class FineGrainedDBManager {
public:
    struct DBEntry {
        std::shared_ptr<DBMVP> db;
        mutable std::shared_mutex mutex;  // Per-database lock
        std::atomic<bool> is_active{true};
        std::atomic<int64_t> access_count{0};
        
        DBEntry(std::shared_ptr<DBMVP> database) : db(std::move(database)) {}
    };
    
    FineGrainedDBManager() : next_db_id_(0) {}
    
    /**
     * @brief Add a new database with fine-grained locking
     */
    Status AddDatabase(const std::string& db_name, std::shared_ptr<DBMVP> db) {
        // First check if database already exists (read lock on map)
        {
            std::shared_lock<std::shared_mutex> map_lock(map_mutex_);
            if (db_entries_.find(db_name) != db_entries_.end()) {
                return Status(DB_ALREADY_EXIST, "Database " + db_name + " already exists");
            }
        }
        
        // Add new database (write lock on map, but brief)
        {
            std::unique_lock<std::shared_mutex> map_lock(map_mutex_);
            // Double-check after acquiring write lock
            if (db_entries_.find(db_name) != db_entries_.end()) {
                return Status(DB_ALREADY_EXIST, "Database " + db_name + " already exists");
            }
            
            auto entry = std::make_shared<DBEntry>(db);
            db_entries_[db_name] = entry;
            db_name_to_id_[db_name] = next_db_id_++;
            
            logger_.Info("Added database: " + db_name);
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Get database with fine-grained read lock
     */
    std::shared_ptr<DBMVP> GetDatabase(const std::string& db_name) {
        std::shared_ptr<DBEntry> entry;
        
        // Get entry with minimal lock on map
        {
            std::shared_lock<std::shared_mutex> map_lock(map_mutex_);
            auto it = db_entries_.find(db_name);
            if (it == db_entries_.end()) {
                return nullptr;
            }
            entry = it->second;
        }
        
        // Lock only the specific database
        std::shared_lock<std::shared_mutex> db_lock(entry->mutex);
        if (!entry->is_active) {
            return nullptr;
        }
        
        entry->access_count.fetch_add(1, std::memory_order_relaxed);
        return entry->db;
    }
    
    /**
     * @brief Execute operation on database with appropriate locking
     */
    template<typename Func>
    Status ExecuteOnDatabase(const std::string& db_name, bool exclusive, Func&& func) {
        std::shared_ptr<DBEntry> entry;
        
        // Get entry with minimal lock
        {
            std::shared_lock<std::shared_mutex> map_lock(map_mutex_);
            auto it = db_entries_.find(db_name);
            if (it == db_entries_.end()) {
                return Status(DB_NOT_FOUND, "Database " + db_name + " not found");
            }
            entry = it->second;
        }
        
        // Execute with appropriate lock on specific database
        if (exclusive) {
            std::unique_lock<std::shared_mutex> db_lock(entry->mutex);
            if (!entry->is_active) {
                return Status(DB_NOT_FOUND, "Database " + db_name + " is inactive");
            }
            return func(entry->db);
        } else {
            std::shared_lock<std::shared_mutex> db_lock(entry->mutex);
            if (!entry->is_active) {
                return Status(DB_NOT_FOUND, "Database " + db_name + " is inactive");
            }
            entry->access_count.fetch_add(1, std::memory_order_relaxed);
            return func(entry->db);
        }
    }
    
    /**
     * @brief Remove database with fine-grained locking
     */
    Status RemoveDatabase(const std::string& db_name) {
        std::shared_ptr<DBEntry> entry;
        
        // Remove from map first
        {
            std::unique_lock<std::shared_mutex> map_lock(map_mutex_);
            auto it = db_entries_.find(db_name);
            if (it == db_entries_.end()) {
                return Status(DB_NOT_FOUND, "Database " + db_name + " not found");
            }
            entry = it->second;
            db_entries_.erase(it);
            db_name_to_id_.erase(db_name);
        }
        
        // Mark as inactive and wait for ongoing operations
        {
            std::unique_lock<std::shared_mutex> db_lock(entry->mutex);
            entry->is_active = false;
        }
        
        // Wait for access count to reach zero
        while (entry->access_count.load(std::memory_order_acquire) > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        logger_.Info("Removed database: " + db_name);
        return Status::OK();
    }
    
    /**
     * @brief Get all database names (snapshot)
     */
    std::vector<std::string> GetAllDatabaseNames() {
        std::shared_lock<std::shared_mutex> map_lock(map_mutex_);
        std::vector<std::string> names;
        names.reserve(db_entries_.size());
        for (const auto& pair : db_entries_) {
            names.push_back(pair.first);
        }
        return names;
    }
    
    /**
     * @brief Execute operation on all databases
     */
    template<typename Func>
    void ExecuteOnAllDatabases(bool exclusive, Func&& func) {
        std::vector<std::pair<std::string, std::shared_ptr<DBEntry>>> snapshot;
        
        // Take snapshot of entries
        {
            std::shared_lock<std::shared_mutex> map_lock(map_mutex_);
            for (const auto& pair : db_entries_) {
                snapshot.push_back(pair);
            }
        }
        
        // Execute on each database with individual locks
        for (const auto& [name, entry] : snapshot) {
            if (exclusive) {
                std::unique_lock<std::shared_mutex> db_lock(entry->mutex);
                if (entry->is_active) {
                    func(name, entry->db);
                }
            } else {
                std::shared_lock<std::shared_mutex> db_lock(entry->mutex);
                if (entry->is_active) {
                    func(name, entry->db);
                }
            }
        }
    }
    
    /**
     * @brief Get statistics
     */
    struct Statistics {
        size_t total_databases;
        size_t active_databases;
        std::unordered_map<std::string, int64_t> access_counts;
    };
    
    Statistics GetStatistics() {
        Statistics stats;
        std::shared_lock<std::shared_mutex> map_lock(map_mutex_);
        
        stats.total_databases = db_entries_.size();
        stats.active_databases = 0;
        
        for (const auto& [name, entry] : db_entries_) {
            if (entry->is_active) {
                stats.active_databases++;
            }
            stats.access_counts[name] = entry->access_count.load(std::memory_order_relaxed);
        }
        
        return stats;
    }
    
private:
    // Map-level mutex (only for structural changes)
    mutable std::shared_mutex map_mutex_;
    
    // Database entries with per-database locks
    std::unordered_map<std::string, std::shared_ptr<DBEntry>> db_entries_;
    
    // Name to ID mapping (for compatibility)
    std::unordered_map<std::string, size_t> db_name_to_id_;
    
    std::atomic<size_t> next_db_id_;
    Logger logger_;
};

} // namespace engine
} // namespace vectordb