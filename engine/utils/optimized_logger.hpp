#pragma once

#include <string>
#include <sstream>
#include <atomic>
#include <cstring>

namespace vectordb {

// Performance-optimized logging macros with zero overhead in production
// These macros completely eliminate logging code in release builds

#ifdef DEBUG
  #define VECTORDB_DEBUG_LOG(logger, msg) logger.Debug(msg)
  #define VECTORDB_INFO_LOG(logger, msg) logger.Info(msg)
#else
  #define VECTORDB_DEBUG_LOG(logger, msg) ((void)0)
  #define VECTORDB_INFO_LOG(logger, msg) ((void)0)
#endif

// For critical performance paths, use compile-time log level
enum class LogLevel {
  NONE = 0,
  ERROR = 1,
  WARNING = 2,
  INFO = 3,
  DEBUG = 4
};

// Compile-time log level configuration
#ifdef DEBUG
  constexpr LogLevel COMPILE_LOG_LEVEL = LogLevel::DEBUG;
#else
  constexpr LogLevel COMPILE_LOG_LEVEL = LogLevel::WARNING;
#endif

// Zero-cost logging with compile-time elimination
template<LogLevel level>
class OptimizedLogger {
public:
  template<typename... Args>
  inline void Log(Args&&... args) {
    if constexpr (level <= COMPILE_LOG_LEVEL) {
      // Only compile this code if log level is enabled
      LogImpl(std::forward<Args>(args)...);
    }
    // Otherwise, entire function call is optimized away
  }

private:
  template<typename... Args>
  void LogImpl(Args&&... args);
};

// Lazy string formatting - only format when actually logging
class LazyLogMessage {
private:
  mutable std::string cached_msg_;
  mutable bool formatted_ = false;

  // Store format string and arguments without formatting
  const char* format_;

  // Use a small buffer optimization for common cases
  static constexpr size_t SMALL_BUFFER_SIZE = 256;
  mutable char small_buffer_[SMALL_BUFFER_SIZE];

public:
  template<typename... Args>
  LazyLogMessage(const char* fmt, Args&&... args) : format_(fmt) {
    // Don't format yet - wait until actually needed
  }

  const std::string& str() const {
    if (!formatted_) {
      // Format only when accessed
      FormatMessage();
      formatted_ = true;
    }
    return cached_msg_;
  }

private:
  void FormatMessage() const;
};

// Structured logging with zero allocation for disabled levels
class StructuredLogger {
private:
  std::atomic<LogLevel> runtime_level_{LogLevel::INFO};

public:
  // Check log level without any allocation
  inline bool ShouldLog(LogLevel level) const {
    return level <= runtime_level_.load(std::memory_order_relaxed);
  }

  // Log with minimal overhead when disabled
  template<typename Func>
  inline void LogIf(LogLevel level, Func&& log_func) {
    if (ShouldLog(level)) {
      log_func();
    }
    // No function call overhead when logging is disabled
  }

  // Batched logging for hot paths
  class BatchLogger {
  private:
    static constexpr size_t BATCH_SIZE = 100;
    struct LogEntry {
      LogLevel level;
      uint64_t timestamp;
      char message[256];
    };

    LogEntry entries_[BATCH_SIZE];
    std::atomic<size_t> current_{0};

  public:
    void AddEntry(LogLevel level, const char* msg) {
      size_t idx = current_.fetch_add(1, std::memory_order_relaxed);
      if (idx < BATCH_SIZE) {
        entries_[idx].level = level;
        entries_[idx].timestamp = GetTimestamp();
        strncpy(entries_[idx].message, msg, 255);
      }
    }

    void Flush();

  private:
    uint64_t GetTimestamp();
  };
};

// Example usage macros for hot paths
#define FAST_LOG_DEBUG(logger, ...) \
  do { \
    if (logger.ShouldLog(LogLevel::DEBUG)) { \
      logger.LogIf(LogLevel::DEBUG, [&]() { \
        /* Only execute this lambda if logging is enabled */ \
        logger.LogFormatted(__VA_ARGS__); \
      }); \
    } \
  } while(0)

// For the most critical paths, use static branch prediction
#define HOT_PATH_LOG(logger, level, msg) \
  do { \
    if (__builtin_expect(logger.ShouldLog(level), 0)) { \
      logger.Log(level, msg); \
    } \
  } while(0)

} // namespace vectordb