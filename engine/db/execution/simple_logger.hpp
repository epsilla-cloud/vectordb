/**
 * @file simple_logger.hpp
 * @brief Simple logger implementation for standalone worker pool
 */

#pragma once

#include <iostream>
#include <string>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace vectordb {
namespace engine {

/**
 * @brief Simple logger for standalone compilation
 */
class SimpleLogger {
public:
    void Info(const std::string& message) {
        Log("INFO", message);
    }

    void Debug(const std::string& message) {
        // Suppress debug messages by default
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

// For standalone builds, use SimpleLogger as StandaloneLogger
// Don't create alias to avoid conflicts with main logger
#ifndef VECTORDB_FULL_BUILD
using StandaloneLogger = SimpleLogger;
#endif

} // namespace engine
} // namespace vectordb
