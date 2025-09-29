#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

// Forward declarations
class WriteAheadLog;
class CompactionManager;

/**
 * WALCompactionCoordinator - Coordinates WAL operations with compaction
 *
 * This class ensures that:
 * 1. WAL is flushed before compaction starts
 * 2. WAL entries for deleted records are cleaned up after compaction
 * 3. Compaction and WAL operations don't interfere with each other
 *
 * Based on best practices from:
 * - RocksDB: WAL truncation after compaction
 * - Cassandra: Commit log cleanup with compaction
 * - PostgreSQL: WAL archival coordination
 */
class WALCompactionCoordinator {
public:
    static WALCompactionCoordinator& GetInstance() {
        static WALCompactionCoordinator instance;
        return instance;
    }

    // Delete copy constructor and assignment
    WALCompactionCoordinator(const WALCompactionCoordinator&) = delete;
    WALCompactionCoordinator& operator=(const WALCompactionCoordinator&) = delete;

    /**
     * Prepare for compaction by ensuring WAL is in a consistent state
     * @param table_name The table being compacted
     * @return Status indicating success or failure
     */
    Status PrepareForCompaction(const std::string& table_name);

    /**
     * Clean up WAL after successful compaction
     * @param table_name The table that was compacted
     * @param last_compacted_wal_id The last WAL ID that was included in compaction
     * @return Status indicating success or failure
     */
    Status CleanupAfterCompaction(const std::string& table_name,
                                 int64_t last_compacted_wal_id);

    /**
     * Check if WAL needs to be flushed before compaction
     * @param table_name The table to check
     * @return true if WAL flush is needed
     */
    bool NeedsWALFlush(const std::string& table_name) const;

    /**
     * Force flush WAL for a specific table
     * @param table_name The table to flush WAL for
     * @return Status indicating success or failure
     */
    Status FlushWAL(const std::string& table_name);

    /**
     * Get the size of WAL that would be freed by compaction
     * @param table_name The table to check
     * @return Size in bytes
     */
    size_t GetCompactableWALSize(const std::string& table_name) const;

    /**
     * Set WAL retention policy for compaction
     * @param min_wal_retention_seconds Minimum time to retain WAL after compaction
     */
    void SetWALRetentionPolicy(int64_t min_wal_retention_seconds);

    /**
     * Enable/disable WAL coordination
     * @param enabled Whether to coordinate WAL with compaction
     */
    void SetEnabled(bool enabled) { enabled_ = enabled; }

    /**
     * Check if WAL coordination is enabled
     * @return true if enabled
     */
    bool IsEnabled() const { return enabled_.load(); }

private:
    WALCompactionCoordinator();
    ~WALCompactionCoordinator() = default;

    // Configuration
    std::atomic<bool> enabled_{true};
    std::atomic<int64_t> min_wal_retention_seconds_{3600};  // 1 hour default

    // Synchronization
    mutable std::mutex coordination_mutex_;

    // Logging
    Logger logger_;
};

/**
 * ScopedWALCompactionGuard - RAII guard for WAL operations during compaction
 *
 * Ensures WAL is properly flushed before compaction and cleaned up after
 */
class ScopedWALCompactionGuard {
public:
    explicit ScopedWALCompactionGuard(const std::string& table_name)
        : table_name_(table_name),
          coordinator_(WALCompactionCoordinator::GetInstance()),
          success_(false) {

        // Prepare WAL for compaction
        if (coordinator_.IsEnabled()) {
            auto status = coordinator_.PrepareForCompaction(table_name_);
            if (!status.ok()) {
                throw std::runtime_error("Failed to prepare WAL for compaction: " +
                                       status.message());
            }
        }
    }

    ~ScopedWALCompactionGuard() {
        // Cleanup only if compaction was successful
        if (success_ && coordinator_.IsEnabled()) {
            coordinator_.CleanupAfterCompaction(table_name_, last_wal_id_);
        }
    }

    // Mark compaction as successful
    void SetSuccess(int64_t last_wal_id) {
        success_ = true;
        last_wal_id_ = last_wal_id;
    }

private:
    std::string table_name_;
    WALCompactionCoordinator& coordinator_;
    bool success_;
    int64_t last_wal_id_{-1};
};

}  // namespace engine
}  // namespace vectordb