#pragma once

#include <string>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <thread>
#include "logger/logger.hpp"
#include "config/config.hpp"

namespace vectordb {
namespace engine {
namespace db {

/**
 * Configuration Manager for VectorDB
 * Manages all environment variables and default values
 */
class ConfigManager {
public:
    // Configuration structure
    struct Config {
        // Performance settings
        int initial_table_capacity;
        int wal_flush_interval;
        bool soft_delete;
        bool vectordb_disable_wal_sync;

        // Query settings
        int intra_query_threads;

        // Debug settings
        bool log_executor;
        bool batch_optimization;

        // Other settings
        bool wal_auto_flush;
    };

private:
    static Config config_;
    static bool initialized_;
    static Logger logger_;

    // Helper to get environment variable with default
    static int GetEnvInt(const char* name, int default_value) {
        const char* env_value = std::getenv(name);
        if (env_value) {
            try {
                int value = std::stoi(env_value);
                logger_.Info("Config: " + std::string(name) + " = " + std::to_string(value) + " (from environment)");
                return value;
            } catch (...) {
                logger_.Warning("Invalid value for " + std::string(name) + ", using default: " + std::to_string(default_value));
            }
        }
        return default_value;
    }

    static bool GetEnvBool(const char* name, bool default_value) {
        const char* env_value = std::getenv(name);
        if (env_value) {
            std::string str_value(env_value);
            bool value = (str_value == "1" || str_value == "true" || str_value == "TRUE" || str_value == "yes" || str_value == "YES");
            logger_.Info("Config: " + std::string(name) + " = " + (value ? "true" : "false") + " (from environment)");
            return value;
        }
        return default_value;
    }

public:
    static void Initialize() {
        if (initialized_) return;

        logger_.Info("================================================================================");
        logger_.Info("                     VectorDB Configuration Initialization                      ");
        logger_.Info("================================================================================");

        // Set default values as requested
        config_.initial_table_capacity = GetEnvInt("INITIAL_TABLE_CAPACITY", 1000);  // Default: 1000
        config_.wal_flush_interval = GetEnvInt("WAL_FLUSH_INTERVAL", 15);           // Default: 15 seconds
        config_.soft_delete = GetEnvBool("SOFT_DELETE", true);                      // Default: 1 (true)
        config_.vectordb_disable_wal_sync = GetEnvBool("VECTORDB_DISABLE_WAL_SYNC", false); // Default: 0 (false)

        // Other configurations
        config_.intra_query_threads = GetEnvInt("EPSILLA_INTRA_QUERY_THREADS",
                                                std::thread::hardware_concurrency());
        config_.log_executor = GetEnvBool("VECTORDB_LOG_EXECUTOR", false);
        config_.batch_optimization = GetEnvBool("VECTORDB_BATCH_OPTIMIZATION", false);
        config_.wal_auto_flush = GetEnvBool("WAL_AUTO_FLUSH", true);

        // Load NSG configuration from environment variables
        vectordb::globalConfig.loadNSGConfigFromEnv();

        // Load Quickwit configuration from environment variables
        vectordb::globalConfig.loadQuickwitConfigFromEnv();

        initialized_ = true;

        PrintConfiguration();
    }

    static void PrintConfiguration() {
        // Get hardware info
        unsigned int hw_threads = std::thread::hardware_concurrency();
        if (hw_threads == 0) hw_threads = 4;

        // Get config from globalConfig (from config.hpp)
        const char* env_threads = std::getenv("EPSILLA_INTRA_QUERY_THREADS");
        const char* env_soft_delete = std::getenv("SOFT_DELETE");
        const char* env_wal_interval = std::getenv("WAL_FLUSH_INTERVAL");
        const char* env_wal_auto = std::getenv("WAL_AUTO_FLUSH");
        const char* env_initial_capacity = std::getenv("INITIAL_TABLE_CAPACITY");
        const char* env_disable_wal_sync = std::getenv("VECTORDB_DISABLE_WAL_SYNC");
        const char* env_auto_compact = std::getenv("AUTO_COMPACTION");
        const char* env_compact_thresh = std::getenv("COMPACTION_THRESHOLD");
        const char* env_compact_interval = std::getenv("COMPACTION_INTERVAL");
        const char* env_eager_delete_compact = std::getenv("EAGER_DELETE_COMPACT");
        const char* env_batch_opt = std::getenv("VECTORDB_BATCH_OPTIMIZATION");
        const char* env_log_exec = std::getenv("VECTORDB_LOG_EXECUTOR");

        std::cout << "\n";
        std::cout << "================================================================================\n";
        std::cout << "                        VectorDB Configuration Summary                         \n";
        std::cout << "================================================================================\n";

        std::cout << " Thread Configuration:\n";
        std::cout << "   - Hardware Threads Detected : " << hw_threads << "\n";
        std::cout << "   - Intra-Query Threads       : " << config_.intra_query_threads
                  << (env_threads ? " (from env)" : " (default)") << "\n";
        std::cout << "   - Rebuild Threads           : 4 (default)\n";
        std::cout << "   - Executors Per Field       : 16 (default)\n";
        std::cout << "\n";

        std::cout << " Queue Configuration:\n";
        std::cout << "   - Master Queue Size         : 500 (default)\n";
        std::cout << "   - Local Queue Size          : 500 (default)\n";
        std::cout << "\n";

        std::cout << " Deletion Configuration:\n";
        std::cout << "   - Soft Delete Mode          : " << (config_.soft_delete ? "ENABLED" : "DISABLED");
        if (config_.soft_delete) {
            std::cout << " (⚠️ Monitor memory in K8s)";
        } else {
            std::cout << " (✓ Safe for K8s)";
        }
        std::cout << (env_soft_delete ? " [from env]" : " [default]") << "\n";
        std::cout << "\n";

        std::cout << " WAL Configuration:\n";
        std::cout << "   - WAL Flush Interval        : " << config_.wal_flush_interval << " seconds"
                  << (env_wal_interval ? " (from env)" : " (default)") << "\n";
        std::cout << "   - WAL Auto Flush            : " << (config_.wal_auto_flush ? "ENABLED" : "DISABLED")
                  << (env_wal_auto ? " (from env)" : " (default)") << "\n";
        std::cout << "   - WAL Sync Mode             : " << (config_.vectordb_disable_wal_sync ? "ASYNC" : "SYNC")
                  << (env_disable_wal_sync ? " (from env)" : " (default)") << "\n";
        std::cout << "\n";

        std::cout << " Compaction Configuration:\n";
        std::cout << "   - Auto Compaction           : ENABLED"
                  << (env_auto_compact ? " (from env)" : " (default)") << "\n";
        std::cout << "   - Compaction Threshold      : 20.0% deleted"
                  << (env_compact_thresh ? " (from env)" : " (default)") << "\n";
        std::cout << "   - Compaction Interval       : 3600 seconds"
                  << (env_compact_interval ? " (from env)" : " (default)") << "\n";
        std::cout << "   - Min Vectors               : 1000 (default)\n";
        std::cout << "   - Eager Compaction On Delete: " << (vectordb::globalConfig.EagerCompactionOnDelete.load() ? "ENABLED" : "DISABLED")
                  << (env_eager_delete_compact ? " (from env)" : " (default)") << "\n";
        std::cout << "\n";

        std::cout << " Memory Management:\n";
        std::cout << "   - Initial Table Capacity    : " << config_.initial_table_capacity
                  << (env_initial_capacity ? " (from env)" : " (default)") << "\n";
        std::cout << "   - Batch Optimization        : " << (config_.batch_optimization ? "ENABLED" : "DISABLED")
                  << (env_batch_opt ? " (from env)" : " (default)") << "\n";
        std::cout << "\n";

        std::cout << " Other Settings:\n";
        std::cout << "   - Pre-Filter                : DISABLED (default)\n";
        std::cout << "   - Global Sync Interval      : 15 (default)\n";
        std::cout << "   - Minimal Graph Size        : 100 (default)\n";
        std::cout << "   - Log Executor              : " << (config_.log_executor ? "ENABLED" : "DISABLED")
                  << (env_log_exec ? " (from env)" : " (default)") << "\n";
        std::cout << "\n";

        // Full-Text Search Configuration
        const char* env_ft_enable = std::getenv("VECTORDB_FULLTEXT_SEARCH_ENABLE");
        const char* env_ft_provider = std::getenv("VECTORDB_FULLTEXT_SEARCH_PROVIDER");
        const char* env_quickwit_binary = std::getenv("VECTORDB_QUICKWIT_BINARY");
        const char* env_ft_port = std::getenv("VECTORDB_FULLTEXT_PORT");
        const char* env_qw_data_dir = std::getenv("QW_DATA_DIR");

        bool ft_enabled = vectordb::globalConfig.EnableFullText.load();
        std::string ft_engine = vectordb::globalConfig.FullTextEngine;
        std::string ft_binary = vectordb::globalConfig.FullTextBinaryPath;
        std::string ft_data_dir = vectordb::globalConfig.FullTextDataDir;
        int ft_port = vectordb::globalConfig.FullTextPort.load();

        std::cout << " Full-Text Search Configuration:\n";
        std::cout << "   - Full-Text Search          : " << (ft_enabled ? "ENABLED" : "DISABLED");
        if (ft_enabled) {
            std::cout << " ✓";
        }
        std::cout << (env_ft_enable ? " (from env)" : " (default)") << "\n";

        if (ft_enabled) {
            std::cout << "   - Search Provider           : " << ft_engine
                      << (env_ft_provider ? " (from env)" : " (default)") << "\n";
            std::cout << "   - Quickwit Binary           : " << ft_binary
                      << (env_quickwit_binary ? " (from env)" : " (default)") << "\n";
            std::cout << "   - Data Directory            : " << ft_data_dir
                      << (env_qw_data_dir ? " (from env)" : " (default)") << "\n";
            std::cout << "   - Port                      : " << ft_port
                      << (env_ft_port ? " (from env)" : " (default)") << "\n";
        }
        std::cout << "\n";

        std::cout << " Environment Variables Set:\n";
        int env_count = 0;
        if (env_initial_capacity) { std::cout << "   - INITIAL_TABLE_CAPACITY=" << env_initial_capacity << "\n"; env_count++; }
        if (env_wal_interval) { std::cout << "   - WAL_FLUSH_INTERVAL=" << env_wal_interval << "\n"; env_count++; }
        if (env_soft_delete) { std::cout << "   - SOFT_DELETE=" << env_soft_delete << "\n"; env_count++; }
        if (env_disable_wal_sync) { std::cout << "   - VECTORDB_DISABLE_WAL_SYNC=" << env_disable_wal_sync << "\n"; env_count++; }
        if (env_wal_auto) { std::cout << "   - WAL_AUTO_FLUSH=" << env_wal_auto << "\n"; env_count++; }
        if (env_threads) { std::cout << "   - EPSILLA_INTRA_QUERY_THREADS=" << env_threads << "\n"; env_count++; }
        if (env_batch_opt) { std::cout << "   - VECTORDB_BATCH_OPTIMIZATION=" << env_batch_opt << "\n"; env_count++; }
        if (env_log_exec) { std::cout << "   - VECTORDB_LOG_EXECUTOR=" << env_log_exec << "\n"; env_count++; }
        if (env_auto_compact) { std::cout << "   - AUTO_COMPACTION=" << env_auto_compact << "\n"; env_count++; }
        if (env_compact_thresh) { std::cout << "   - COMPACTION_THRESHOLD=" << env_compact_thresh << "\n"; env_count++; }
        if (env_compact_interval) { std::cout << "   - COMPACTION_INTERVAL=" << env_compact_interval << "\n"; env_count++; }
        if (env_eager_delete_compact) { std::cout << "   - EAGER_DELETE_COMPACT=" << env_eager_delete_compact << "\n"; env_count++; }
        if (env_ft_enable) { std::cout << "   - VECTORDB_FULLTEXT_SEARCH_ENABLE=" << env_ft_enable << "\n"; env_count++; }
        if (env_ft_provider) { std::cout << "   - VECTORDB_FULLTEXT_SEARCH_PROVIDER=" << env_ft_provider << "\n"; env_count++; }
        if (env_quickwit_binary) { std::cout << "   - VECTORDB_QUICKWIT_BINARY=" << env_quickwit_binary << "\n"; env_count++; }
        if (env_qw_data_dir) { std::cout << "   - QW_DATA_DIR=" << env_qw_data_dir << "\n"; env_count++; }
        if (env_ft_port) { std::cout << "   - VECTORDB_FULLTEXT_PORT=" << env_ft_port << "\n"; env_count++; }
        if (env_count == 0) {
            std::cout << "   (none - using all defaults)\n";
        }
        std::cout << "\n";

        // Environment Variable Documentation
        std::cout << " Environment Variable Guide:\n";
        std::cout << "   Data Directory (Priority: highest to lowest):\n";
        std::cout << "     • QW_DATA_DIR                      - Short alias (recommended)\n";
        std::cout << "     • VECTORDB_FULLTEXT_DATA_DIR       - Full name\n";
        std::cout << "     • VECTORDB_QUICKWIT_DATA_DIR       - Legacy name\n";
        std::cout << "\n";
        std::cout << "   Full-Text Search:\n";
        std::cout << "     • VECTORDB_FULLTEXT_SEARCH_ENABLE  - Enable/disable (true/false)\n";
        std::cout << "     • VECTORDB_FULLTEXT_SEARCH_PROVIDER- Provider (quickwit/elasticsearch)\n";
        std::cout << "     • VECTORDB_QUICKWIT_BINARY         - Path to Quickwit binary\n";
        std::cout << "     • VECTORDB_FULLTEXT_PORT           - Port for full-text service\n";
        std::cout << "\n";
        std::cout << "   Health Check:\n";
        std::cout << "     • curl http://localhost:8888/health - Check service status\n";

        std::cout << "\n Performance Impact Summary:\n";
        if (config_.initial_table_capacity >= 1000) {
            std::cout << "   ✓ Large initial capacity: Optimized for batch insertion\n";
        } else {
            std::cout << "   ⚠ Small initial capacity: May cause frequent reallocation\n";
        }

        if (config_.wal_flush_interval <= 15) {
            std::cout << "   ✓ Short WAL flush interval: Better data durability\n";
        } else {
            std::cout << "   ⚠ Long WAL flush interval: Risk of data loss on crash\n";
        }

        if (config_.vectordb_disable_wal_sync) {
            std::cout << "   ⚠ WAL sync disabled: Higher performance but risk of data loss\n";
        } else {
            std::cout << "   ✓ WAL sync enabled: Data durability guaranteed\n";
        }

        if (!config_.soft_delete) {
            std::cout << "   ✓ Hard delete enabled: Memory freed immediately (good for K8s)\n";
        } else {
            std::cout << "   ⚠ Soft delete enabled: Deleted data retained until compaction\n";
        }

        if (config_.batch_optimization) {
            std::cout << "   ✓ Batch optimization enabled: Better bulk insertion performance\n";
        }

        std::cout << "================================================================================\n\n";
    }

    // Getters
    static const Config& GetConfig() {
        if (!initialized_) {
            Initialize();
        }
        return config_;
    }

    static int GetInitialTableCapacity() {
        return GetConfig().initial_table_capacity;
    }

    static int GetWalFlushInterval() {
        return GetConfig().wal_flush_interval;
    }

    static bool IsSoftDeleteEnabled() {
        return GetConfig().soft_delete;
    }

    static bool IsWalSyncDisabled() {
        return GetConfig().vectordb_disable_wal_sync;
    }

    static int GetIntraQueryThreads() {
        return GetConfig().intra_query_threads;
    }

    static bool IsLogExecutorEnabled() {
        return GetConfig().log_executor;
    }

    static bool IsBatchOptimizationEnabled() {
        return GetConfig().batch_optimization;
    }

    static bool IsWalAutoFlushEnabled() {
        return GetConfig().wal_auto_flush;
    }
};

// Static member definitions
ConfigManager::Config ConfigManager::config_;
bool ConfigManager::initialized_ = false;
Logger ConfigManager::logger_;

} // namespace db
} // namespace engine
} // namespace vectordb