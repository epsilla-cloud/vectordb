#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstring>
#include <random>

namespace vectordb {

/**
 * Production-friendly logging system that balances performance with observability
 *
 * Features:
 * 1. Runtime configurable log levels via environment variables
 * 2. Sampling for high-frequency logs
 * 3. Async logging for non-critical paths
 * 4. Structured logging with minimal overhead
 * 5. Critical error always logged
 */

enum class LogLevel : uint8_t {
    OFF = 0,
    ERROR = 1,
    WARNING = 2,
    INFO = 3,
    DEBUG = 4,
    TRACE = 5
};

// Runtime configuration from environment
class LogConfig {
public:
    static LogConfig& Instance() {
        static LogConfig instance;
        return instance;
    }

    LogLevel GetLevel() const {
        return level_.load(std::memory_order_relaxed);
    }

    void SetLevel(LogLevel level) {
        level_.store(level, std::memory_order_relaxed);
    }

    bool ShouldSample() const {
        return sample_rate_ > 0 && (rand() % 100) < sample_rate_;
    }

    uint32_t GetSampleRate() const { return sample_rate_; }

private:
    LogConfig() {
        // Read from environment variable
        const char* env_level = std::getenv("VECTORDB_LOG_LEVEL");
        if (env_level) {
            if (strcmp(env_level, "DEBUG") == 0) level_ = LogLevel::DEBUG;
            else if (strcmp(env_level, "INFO") == 0) level_ = LogLevel::INFO;
            else if (strcmp(env_level, "WARNING") == 0) level_ = LogLevel::WARNING;
            else if (strcmp(env_level, "ERROR") == 0) level_ = LogLevel::ERROR;
            else if (strcmp(env_level, "OFF") == 0) level_ = LogLevel::OFF;
        } else {
            // Default: INFO in production, DEBUG in debug build
            #ifdef DEBUG
                level_ = LogLevel::DEBUG;
            #else
                level_ = LogLevel::INFO;
            #endif
        }

        // Sampling rate for debug logs (0-100%)
        const char* sample_rate = std::getenv("VECTORDB_LOG_SAMPLE_RATE");
        if (sample_rate) {
            sample_rate_ = std::min(100u, (uint32_t)std::stoul(sample_rate));
        }
    }

    std::atomic<LogLevel> level_{LogLevel::INFO};
    uint32_t sample_rate_{1};  // Default 1% sampling for debug logs
};

/**
 * High-performance logger for production use
 */
class ProductionLogger {
public:
    ProductionLogger(const std::string& name) : name_(name) {}

    // Always log errors - critical for production debugging
    template<typename... Args>
    void Error(const char* fmt, Args&&... args) {
        LogFormatted(LogLevel::ERROR, fmt, std::forward<Args>(args)...);
    }

    // Log warnings - important but not critical
    template<typename... Args>
    void Warning(const char* fmt, Args&&... args) {
        if (ShouldLog(LogLevel::WARNING)) {
            LogFormatted(LogLevel::WARNING, fmt, std::forward<Args>(args)...);
        }
    }

    // Info logs - normal operations
    template<typename... Args>
    void Info(const char* fmt, Args&&... args) {
        if (ShouldLog(LogLevel::INFO)) {
            LogFormatted(LogLevel::INFO, fmt, std::forward<Args>(args)...);
        }
    }

    // Debug logs - with sampling in production
    template<typename... Args>
    void Debug(const char* fmt, Args&&... args) {
        if (ShouldLog(LogLevel::DEBUG)) {
            // Apply sampling for debug logs in production
            #ifndef DEBUG
                if (!LogConfig::Instance().ShouldSample()) return;
            #endif
            LogFormatted(LogLevel::DEBUG, fmt, std::forward<Args>(args)...);
        }
    }

    // Trace logs - very detailed, usually disabled
    template<typename... Args>
    void Trace(const char* fmt, Args&&... args) {
        if (ShouldLog(LogLevel::TRACE)) {
            LogFormatted(LogLevel::TRACE, fmt, std::forward<Args>(args)...);
        }
    }

    // Structured logging for better parsing
    class Event {
    public:
        Event& Add(const char* key, int64_t value) {
            // Add key-value pair
            return *this;
        }
        Event& Add(const char* key, double value) {
            return *this;
        }
        Event& Add(const char* key, const std::string& value) {
            return *this;
        }
        void Log(LogLevel level);
    };

    // Log only every N occurrences (for high-frequency events)
    template<typename... Args>
    void DebugEveryN(uint32_t n, const char* fmt, Args&&... args) {
        static std::atomic<uint32_t> counter{0};
        if (counter.fetch_add(1) % n == 0) {
            Debug(fmt, std::forward<Args>(args)...);
        }
    }

    // Log with rate limiting (max N per second)
    template<typename... Args>
    void DebugRateLimited(uint32_t max_per_sec, const char* fmt, Args&&... args) {
        static std::chrono::steady_clock::time_point last_log;
        static std::atomic<uint32_t> count{0};

        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - last_log);

        if (elapsed.count() >= 1) {
            last_log = now;
            count = 0;
        }

        if (count.fetch_add(1) < max_per_sec) {
            Debug(fmt, std::forward<Args>(args)...);
        }
    }

private:
    inline bool ShouldLog(LogLevel level) const {
        return level <= LogConfig::Instance().GetLevel();
    }

    template<typename... Args>
    void LogFormatted(LogLevel level, const char* fmt, Args&&... args) {
        // Use stack buffer for small logs
        char buffer[256];
        int ret = snprintf(buffer, sizeof(buffer), fmt, std::forward<Args>(args)...);

        if (ret < sizeof(buffer)) {
            // Fast path - fits in stack buffer
            WriteLog(level, buffer);
        } else {
            // Slow path - needs heap allocation
            std::vector<char> large_buffer(ret + 1);
            snprintf(large_buffer.data(), large_buffer.size(), fmt, std::forward<Args>(args)...);
            WriteLog(level, large_buffer.data());
        }
    }

    void WriteLog(LogLevel level, const char* msg);

    std::string name_;
};

/**
 * Async logger for non-critical paths
 */
class AsyncLogger : public ProductionLogger {
public:
    AsyncLogger(const std::string& name) : ProductionLogger(name) {
        // Start background logging thread
        logging_thread_ = std::thread(&AsyncLogger::ProcessLogs, this);
    }

    ~AsyncLogger() {
        shutdown_ = true;
        if (logging_thread_.joinable()) {
            logging_thread_.join();
        }
    }

private:
    void ProcessLogs();

    std::thread logging_thread_;
    std::atomic<bool> shutdown_{false};
};

/**
 * Specialized loggers for hot paths
 */

// For the most critical hot path - completely disabled in production by default
#ifdef DEBUG
    #define HOT_PATH_DEBUG(logger, ...) logger.Debug(__VA_ARGS__)
#else
    // Can be enabled via environment variable in production if needed
    #define HOT_PATH_DEBUG(logger, ...) \
        do { \
            static bool enabled = std::getenv("VECTORDB_ENABLE_HOT_PATH_LOG") != nullptr; \
            if (__builtin_expect(enabled, 0)) { \
                logger.DebugRateLimited(10, __VA_ARGS__); /* Max 10 per second */ \
            } \
        } while(0)
#endif

// For query execution - sample 0.1% in production
#define QUERY_TRACE(logger, ...) \
    do { \
        static thread_local std::mt19937 gen(std::random_device{}()); \
        static thread_local std::uniform_int_distribution<> dis(1, 1000); \
        if (dis(gen) == 1) { /* 0.1% sampling */ \
            logger.Debug("[SAMPLED] " __VA_ARGS__); \
        } \
    } while(0)

// For batch operations - log summary instead of individual items
class BatchLogger {
public:
    void StartBatch(const std::string& operation) {
        operation_ = operation;
        start_time_ = std::chrono::steady_clock::now();
        count_ = 0;
    }

    void RecordItem() {
        count_++;
    }

    void EndBatch(ProductionLogger& logger) {
        auto elapsed = std::chrono::steady_clock::now() - start_time_;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

        if (count_ > 0) {
            logger.Info("Batch %s: processed %zu items in %ldms (%.2f items/sec)",
                       operation_.c_str(), count_, ms,
                       (count_ * 1000.0) / std::max(1L, ms));
        }
    }

private:
    std::string operation_;
    std::chrono::steady_clock::time_point start_time_;
    size_t count_{0};
};

/**
 * Metrics collection (separate from logging)
 */
class Metrics {
public:
    static Metrics& Instance() {
        static Metrics instance;
        return instance;
    }

    void RecordQueryLatency(double ms) {
        query_latencies_.fetch_add(1);
        total_query_time_.fetch_add(static_cast<uint64_t>(ms * 1000));
    }

    void RecordCacheHit() { cache_hits_.fetch_add(1); }
    void RecordCacheMiss() { cache_misses_.fetch_add(1); }

    // Expose metrics for monitoring systems (Prometheus, etc.)
    std::string GetMetricsJson() const;

private:
    std::atomic<uint64_t> query_latencies_{0};
    std::atomic<uint64_t> total_query_time_{0};  // in microseconds
    std::atomic<uint64_t> cache_hits_{0};
    std::atomic<uint64_t> cache_misses_{0};
};

} // namespace vectordb