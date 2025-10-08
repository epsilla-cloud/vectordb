/**
 * @file standalone_worker_pool.hpp
 * @brief Worker pool with embedded logger for standalone compilation
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <cstring>
#include <fstream>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <ctime>
#include <iomanip>
#include <sstream>

// Platform-specific headers for CPU affinity
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
#include <pthread.h>
#include <mach/thread_policy.h>
#include <mach/thread_act.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

namespace vectordb {
namespace engine {

/**
 * @brief Simple logger for standalone builds
 */
class StandaloneLogger {
public:
    void Info(const std::string& message) {
        Log("INFO", message);
    }

    void Debug(const std::string& message) {
        #ifdef DEBUG_LOG
        Log("DEBUG", message);
        #endif
    }

    void Warning(const std::string& message) {
        Log("WARN", message);
    }

    void Error(const std::string& message) {
        Log("ERROR", message);
    }

private:
    void Log(const std::string& level, const std::string& message) {
        auto now = std::time(nullptr);
        auto tm = *std::localtime(&now);

        std::ostringstream oss;
        oss << "[" << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << "] "
            << "[" << level << "] " << message;

        if (level == "ERROR" || level == "WARN") {
            std::cerr << oss.str() << std::endl;
        } else {
            std::cout << oss.str() << std::endl;
        }
    }
};

// Now include the worker pool implementation with the logger defined
namespace execution {

// Forward declare logger type for worker pool
using Logger = StandaloneLogger;

// Include worker pool types and implementation
#include "worker_pool_impl.hpp"

} // namespace execution
} // namespace engine
} // namespace vectordb
