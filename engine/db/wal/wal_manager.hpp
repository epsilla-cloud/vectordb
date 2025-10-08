#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#include "db/wal/write_ahead_log.hpp"
#include "utils/status.hpp"
#include "logger/logger.hpp"
#include "db/execution/worker_pool.hpp"

namespace vectordb {
namespace engine {

/**
 * @brief Configuration for WAL Manager
 *
 * CRITICAL FIX: Updated documentation to clarify fsync behavior
 * - In async mode: WriteEntry calls via WALManager DO NOT immediately fsync,
 *   but WriteEntry in WriteAheadLog DOES fsync on every write
 * - WALManager's async mode only affects task queuing, not fsync behavior
 * - For guaranteed durability, WriteAheadLog::WriteEntry always calls fsync
 */
struct WALConfig {
    // Number of retry attempts for failed writes
    int max_retry_attempts = 3;

    // Delay between retry attempts (milliseconds)
    int retry_delay_ms = 100;

    // Whether to use async write for better performance
    // NOTE: This only affects task queuing, not fsync!
    // WriteAheadLog::WriteEntry ALWAYS calls fsync for durability
    bool async_write = true;

    // Queue size for async writes
    size_t async_queue_size = 10000;

    // Fsync interval (milliseconds) - for ADDITIONAL periodic fsync
    // NOTE: Individual WriteEntry calls ALREADY fsync, this is just extra safety
    int fsync_interval_ms = 1000;

    // Emergency fallback directory when primary fails
    std::string fallback_directory = "/tmp/wal_fallback";

    // Whether to halt on critical errors
    bool halt_on_critical_error = false;

    // Enable write-through cache
    bool enable_cache = true;

    // Cache size in entries
    size_t cache_size = 1000;
};

/**
 * @brief WAL write operation result
 */
struct WALWriteResult {
    bool success;
    int64_t sequence_id;
    std::string error_message;
    int retry_count;
};

/**
 * @brief Enhanced WAL Manager with proper error handling
 * 
 * This class provides:
 * - Retry mechanism for transient failures
 * - Async write support for better performance
 * - Fallback directory for disk failures
 * - Proper error reporting and recovery
 * - Write-through cache for recent entries
 */
class WALManager {
public:
    WALManager(const std::string& base_path, int64_t table_id, bool is_leader, 
               const WALConfig& config = WALConfig())
        : config_(config),
          base_path_(base_path),
          table_id_(table_id),
          is_leader_(is_leader),
          running_(false),
          total_writes_(0),
          failed_writes_(0),
          retry_writes_(0) {
        
        try {
            // Initialize primary WAL
            wal_ = std::make_unique<WriteAheadLog>(base_path, table_id, is_leader);
            
            // Initialize fallback WAL if configured
            if (!config_.fallback_directory.empty() && is_leader) {
                InitializeFallbackWAL();
            }
            
            // Start async writer thread if enabled
            if (config_.async_write && is_leader) {
                StartAsyncWriter();
            }
            
            // Initialize cache if enabled
            if (config_.enable_cache) {
                cache_.reserve(config_.cache_size);
            }
            
        } catch (const std::exception& e) {
            logger_.Error("Failed to initialize WAL Manager: " + std::string(e.what()));
            HandleCriticalError("WAL initialization failed: " + std::string(e.what()));
        }
    }
    
    ~WALManager() {
        Stop();
    }
    
    /**
     * @brief Write an entry to the WAL with proper error handling
     */
    WALWriteResult WriteEntry(LogEntryType type, const std::string& entry) {
        total_writes_++;
        
        if (!is_leader_) {
            return {true, 0, "", 0};  // Non-leaders don't write to WAL
        }
        
        if (config_.async_write) {
            return WriteEntryAsync(type, entry);
        } else {
            return WriteEntrySync(type, entry);
        }
    }
    
    /**
     * @brief Replay WAL entries
     */
    Status Replay(meta::TableSchema& table_schema,
                  std::unordered_map<std::string, meta::FieldType>& field_name_type_map,
                  std::shared_ptr<TableSegment> segment) {
        try {
            if (wal_) {
                wal_->Replay(table_schema, field_name_type_map, segment);
            }
            
            // Also replay from fallback if it exists
            if (fallback_wal_) {
                logger_.Info("Replaying entries from fallback WAL");
                fallback_wal_->Replay(table_schema, field_name_type_map, segment);
            }
            
            return Status::OK();
        } catch (const std::exception& e) {
            return Status(DB_UNEXPECTED_ERROR, "WAL replay failed: " + std::string(e.what()));
        }
    }
    
    /**
     * @brief Force flush all pending writes
     */
    Status Flush() {
        if (!is_leader_) {
            return Status::OK();
        }
        
        // Flush async queue if enabled
        if (config_.async_write) {
            FlushAsyncQueue();
        }
        
        // Force fsync
        return ForceFsync();
    }
    
    /**
     * @brief Get statistics
     */
    struct Stats {
        size_t total_writes;
        size_t failed_writes;
        size_t retry_writes;
        size_t queue_size;
        bool using_fallback;
        double success_rate;
    };
    
    Stats GetStats() const {
        Stats stats;
        stats.total_writes = total_writes_.load();
        stats.failed_writes = failed_writes_.load();
        stats.retry_writes = retry_writes_.load();
        stats.queue_size = GetQueueSize();
        stats.using_fallback = using_fallback_.load();
        stats.success_rate = (stats.total_writes > 0) ? 
            1.0 - (static_cast<double>(stats.failed_writes) / stats.total_writes) : 1.0;
        return stats;
    }
    
    /**
     * @brief Enable or disable WAL
     */
    void SetEnabled(bool enabled) {
        if (wal_) {
            wal_->SetEnabled(enabled);
        }
        if (fallback_wal_) {
            fallback_wal_->SetEnabled(enabled);
        }
    }
    
    /**
     * @brief Stop the WAL manager
     */
    void Stop() {
        if (running_.exchange(false)) {
            // Flush pending writes
            Flush();

            // Stop fsync thread
            if (fsync_thread_.joinable()) {
                fsync_thread_.join();
            }
        }
    }
    
private:
    struct AsyncWriteRequest {
        LogEntryType type;
        std::string entry;
        std::promise<WALWriteResult> promise;
        int retry_count = 0;
    };
    
    /**
     * @brief Initialize fallback WAL
     */
    void InitializeFallbackWAL() {
        try {
            std::filesystem::create_directories(config_.fallback_directory);
            fallback_wal_ = std::make_unique<WriteAheadLog>(
                config_.fallback_directory, table_id_, is_leader_);
            logger_.Info("Fallback WAL initialized at: " + config_.fallback_directory);
        } catch (const std::exception& e) {
            logger_.Warning("Failed to initialize fallback WAL: " + std::string(e.what()));
        }
    }
    
    /**
     * @brief Start async writer thread (now uses IO worker pool)
     */
    void StartAsyncWriter() {
        running_ = true;

        // Start background fsync thread
        fsync_thread_ = std::thread([this]() {
            FsyncLoop();
        });

        logger_.Info("Async WAL writer started (using IO worker pool)");
    }

    /**
     * @brief Periodic fsync loop
     */
    void FsyncLoop() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.fsync_interval_ms));
            if (running_) {
                ForceFsync();
            }
        }
    }
    
    /**
     * @brief Write entry asynchronously (using IO worker pool)
     */
    WALWriteResult WriteEntryAsync(LogEntryType type, const std::string& entry) {
        try {
            // CRITICAL FIX: Submit to IO worker pool with CRITICAL priority for WAL writes
            // CRITICAL priority ensures the task will wait for queue space instead of being rejected,
            // preventing data loss due to queue saturation
            auto& pool_manager = execution::WorkerPoolManager::GetInstance();

            auto future = pool_manager.SubmitIoTaskWithPriority(
                execution::TaskPriority::CRITICAL,  // Changed from HIGH to CRITICAL
                [this, type, entry]() -> WALWriteResult {
                    return this->WriteEntryWithRetry(type, entry, 0);
                }
            );

            // CRITICAL FIX: Increased timeout from 10s to 60s for critical WAL operations
            // WAL writes must succeed, so we give them more time
            if (future.wait_for(std::chrono::seconds(60)) == std::future_status::ready) {
                return future.get();
            } else {
                failed_writes_++;
                logger_.Error("CRITICAL: WAL async write timeout after 60 seconds");
                return {false, -1, "Async write timeout after 60s", 0};
            }

        } catch (const std::exception& e) {
            failed_writes_++;
            logger_.Error("CRITICAL: WAL IO pool error: " + std::string(e.what()));
            return {false, -1, "IO pool error: " + std::string(e.what()), 0};
        }
    }
    
    /**
     * @brief Write entry synchronously
     */
    WALWriteResult WriteEntrySync(LogEntryType type, const std::string& entry) {
        return WriteEntryWithRetry(type, entry, 0);
    }
    
    /**
     * @brief Write entry with retry logic
     */
    WALWriteResult WriteEntryWithRetry(LogEntryType type, const std::string& entry, 
                                       int current_retry) {
        WALWriteResult result;
        result.retry_count = current_retry;
        
        for (int attempt = 0; attempt <= config_.max_retry_attempts; ++attempt) {
            try {
                // Try primary WAL
                if (wal_ && !using_fallback_) {
                    int64_t seq_id = wal_->WriteEntry(type, entry);
                    
                    // Add to cache if enabled
                    if (config_.enable_cache) {
                        AddToCache(seq_id, type, entry);
                    }
                    
                    result.success = true;
                    result.sequence_id = seq_id;
                    
                    if (attempt > 0) {
                        retry_writes_++;
                    }
                    
                    return result;
                }
                
                // Try fallback WAL
                if (fallback_wal_) {
                    if (!using_fallback_) {
                        logger_.Warning("Switching to fallback WAL");
                        using_fallback_ = true;
                    }
                    
                    int64_t seq_id = fallback_wal_->WriteEntry(type, entry);
                    result.success = true;
                    result.sequence_id = seq_id;
                    
                    if (attempt > 0) {
                        retry_writes_++;
                    }
                    
                    return result;
                }
                
            } catch (const std::exception& e) {
                result.error_message = e.what();
                
                if (attempt < config_.max_retry_attempts) {
                    logger_.Warning("WAL write failed, retrying (attempt " + 
                                  std::to_string(attempt + 1) + "): " + e.what());
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(config_.retry_delay_ms * (attempt + 1)));
                } else {
                    logger_.Error("WAL write failed after " + 
                                std::to_string(config_.max_retry_attempts) + 
                                " attempts: " + e.what());
                }
            }
        }
        
        // All attempts failed
        failed_writes_++;
        result.success = false;
        
        // Handle critical error if configured
        if (config_.halt_on_critical_error) {
            HandleCriticalError("WAL write failed: " + result.error_message);
        }
        
        return result;
    }
    
    /**
     * @brief Force fsync on WAL files
     * CRITICAL FIX: Now actually calls Flush() which performs fsync
     */
    Status ForceFsync() {
        try {
            if (wal_) {
                wal_->Flush();  // FIXED: Actually call Flush which does fsync
            }
            if (fallback_wal_ && using_fallback_) {
                fallback_wal_->Flush();  // FIXED: Flush fallback WAL too
            }
            return Status::OK();
        } catch (const std::exception& e) {
            return Status(DB_UNEXPECTED_ERROR, "Fsync failed: " + std::string(e.what()));
        }
    }
    
    /**
     * @brief Flush async queue (now uses IO worker pool stats)
     */
    void FlushAsyncQueue() {
        // Wait for IO pool to complete pending tasks
        auto& pool_manager = execution::WorkerPoolManager::GetInstance();
        auto stats = pool_manager.GetIoStats();

        // Simple spin wait for pending tasks to complete
        while (stats.current_queue_size.load() > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            stats = pool_manager.GetIoStats();
        }
    }

    /**
     * @brief Get queue size (from IO worker pool)
     */
    size_t GetQueueSize() const {
        try {
            auto& pool_manager = execution::WorkerPoolManager::GetInstance();
            auto stats = pool_manager.GetIoStats();
            return stats.current_queue_size.load();
        } catch (...) {
            return 0;
        }
    }
    
    /**
     * @brief Add entry to cache
     */
    void AddToCache(int64_t seq_id, LogEntryType type, const std::string& entry) {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        
        if (cache_.size() >= config_.cache_size) {
            // Remove oldest entry
            cache_.erase(cache_.begin());
        }
        
        cache_[seq_id] = {type, entry};
    }
    
    /**
     * @brief Handle critical error
     */
    void HandleCriticalError(const std::string& error) {
        logger_.Error("CRITICAL WAL ERROR: " + error);
        
        if (config_.halt_on_critical_error) {
            // Set a flag that can be checked by other components
            critical_error_ = true;
            
            // Optionally, could throw exception or call abort()
            throw std::runtime_error("Critical WAL error: " + error);
        }
    }
    
    WALConfig config_;
    std::string base_path_;
    int64_t table_id_;
    bool is_leader_;
    
    std::unique_ptr<WriteAheadLog> wal_;
    std::unique_ptr<WriteAheadLog> fallback_wal_;
    std::atomic<bool> using_fallback_{false};
    
    // Async write support (now using IO worker pool)
    std::atomic<bool> running_;
    std::thread fsync_thread_;  // Only for periodic fsync
    
    // Cache
    std::map<int64_t, std::pair<LogEntryType, std::string>> cache_;
    mutable std::mutex cache_mutex_;
    
    // Statistics
    std::atomic<size_t> total_writes_;
    std::atomic<size_t> failed_writes_;
    std::atomic<size_t> retry_writes_;
    
    // Error state
    std::atomic<bool> critical_error_{false};
    
    Logger logger_;
};

} // namespace engine
} // namespace vectordb