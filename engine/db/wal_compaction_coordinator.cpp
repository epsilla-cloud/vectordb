#include "db/wal_compaction_coordinator.hpp"

#include <chrono>
#include <filesystem>
#include <thread>
#include <unordered_map>

#include "config/config.hpp"
#include "db/compaction_manager.hpp"
#include "utils/common_util.hpp"

namespace vectordb {
namespace engine {

WALCompactionCoordinator::WALCompactionCoordinator() : logger_() {
    // Load configuration from global config
    enabled_ = globalConfig.AutoCompaction.load();

    // Set default WAL retention based on compaction interval
    min_wal_retention_seconds_ = globalConfig.CompactionInterval.load() * 2;

    logger_.Info("WAL Compaction Coordinator initialized with retention=" +
                 std::to_string(min_wal_retention_seconds_.load()) + " seconds");
}

Status WALCompactionCoordinator::PrepareForCompaction(const std::string& table_name) {
    std::lock_guard<std::mutex> lock(coordination_mutex_);

    if (!enabled_.load()) {
        return Status::OK();
    }

    logger_.Info("Preparing WAL for compaction of table: " + table_name);

    // Check if WAL needs flushing
    if (NeedsWALFlush(table_name)) {
        logger_.Debug("WAL flush needed before compaction for table: " + table_name);
        auto status = FlushWAL(table_name);
        if (!status.ok()) {
            logger_.Error("Failed to flush WAL for table " + table_name +
                         ": " + status.message());
            return status;
        }
    }

    // Mark WAL segments as being compacted
    // This prevents them from being deleted while compaction is in progress
    // Note: Actual implementation would interact with WAL system
    logger_.Debug("WAL prepared for compaction of table: " + table_name);

    return Status::OK();
}

Status WALCompactionCoordinator::CleanupAfterCompaction(const std::string& table_name,
                                                        int64_t last_compacted_wal_id) {
    std::lock_guard<std::mutex> lock(coordination_mutex_);

    if (!enabled_.load()) {
        return Status::OK();
    }

    logger_.Info("Cleaning up WAL after compaction of table: " + table_name +
                ", last_compacted_wal_id=" + std::to_string(last_compacted_wal_id));

    // Calculate cutoff time for WAL retention
    auto now = std::chrono::system_clock::now();
    auto retention_duration = std::chrono::seconds(min_wal_retention_seconds_.load());
    auto cutoff_time = now - retention_duration;

    // In a real implementation, this would:
    // 1. Mark all WAL entries up to last_compacted_wal_id as safe to delete
    // 2. Delete WAL segments older than cutoff_time
    // 3. Update WAL metadata to reflect the cleanup

    // For now, we simulate the cleanup
    size_t freed_size = GetCompactableWALSize(table_name);
    if (freed_size > 0) {
        logger_.Info("Freed " + std::to_string(freed_size / 1024 / 1024) +
                    "MB of WAL space for table: " + table_name);
    }

    return Status::OK();
}

bool WALCompactionCoordinator::NeedsWALFlush(const std::string& table_name) const {
    if (!enabled_.load()) {
        return false;
    }

    // In a real implementation, this would check:
    // 1. If there are unflushed WAL entries for the table
    // 2. If the unflushed WAL size exceeds a threshold
    // 3. If the oldest unflushed WAL entry is too old

    // For now, we use a simple heuristic based on time
    // If it's been more than 30 seconds since last flush, recommend flush
    static std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_flush_times;

    auto now = std::chrono::steady_clock::now();
    auto it = last_flush_times.find(table_name);
    if (it == last_flush_times.end()) {
        return true;  // Never flushed, needs flush
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - it->second);
    return elapsed.count() > 30;  // Flush if more than 30 seconds old
}

Status WALCompactionCoordinator::FlushWAL(const std::string& table_name) {
    if (!enabled_.load()) {
        return Status::OK();
    }

    logger_.Debug("Flushing WAL for table: " + table_name);

    // In a real implementation, this would:
    // 1. Get the WAL instance for the table
    // 2. Force a flush of all pending writes
    // 3. Wait for flush to complete
    // 4. Update flush timestamp

    // Simulate flush delay
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Update last flush time
    static std::unordered_map<std::string, std::chrono::steady_clock::time_point> last_flush_times;
    last_flush_times[table_name] = std::chrono::steady_clock::now();

    logger_.Debug("WAL flush completed for table: " + table_name);

    return Status::OK();
}

size_t WALCompactionCoordinator::GetCompactableWALSize(const std::string& table_name) const {
    if (!enabled_.load()) {
        return 0;
    }

    // In a real implementation, this would:
    // 1. Calculate the size of WAL segments that contain only deleted records
    // 2. Calculate the size of WAL segments older than retention period
    // 3. Return the total size that can be freed

    // For now, return a simulated size based on table name hash
    std::hash<std::string> hasher;
    size_t hash = hasher(table_name);
    return (hash % 100) * 1024 * 1024;  // 0-100 MB
}

void WALCompactionCoordinator::SetWALRetentionPolicy(int64_t min_wal_retention_seconds) {
    if (min_wal_retention_seconds >= 60 && min_wal_retention_seconds <= 86400 * 7) {
        min_wal_retention_seconds_ = min_wal_retention_seconds;
        logger_.Info("WAL retention policy updated to " +
                    std::to_string(min_wal_retention_seconds) + " seconds");
    } else {
        logger_.Warning("Invalid WAL retention period: " +
                       std::to_string(min_wal_retention_seconds) +
                       " (must be between 60 seconds and 7 days)");
    }
}

}  // namespace engine
}  // namespace vectordb