#include <iostream>

#include "logger.hpp"

namespace vectordb {
namespace engine {

enum class LogLevel {
  DEBUG,
  INFO,
  WARNING,
  ERROR
};

void log(LogLevel level, const std::string& message) {
  std::time_t now = std::time(nullptr);
  char timestamp[20];
  std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", std::localtime(&now));

  switch (level) {
    case LogLevel::DEBUG:
      std::cout << "[" << timestamp << "] [DEBUG] " << message << std::endl;
      break;
    case LogLevel::INFO:
      std::cout << "[" << timestamp << "] [INFO] " << message << std::endl;
      break;
    case LogLevel::WARNING:
      std::cout << "[" << timestamp << "] [WARNING] " << message << std::endl;
      break;
    case LogLevel::ERROR:
      std::cout << "[" << timestamp << "] [ERROR] " << message << std::endl;
      break;
    default:
      break;
  }
};

void Logger::info(const std::string& message) {
  log(LogLevel::INFO, message);
};

} // namespace engine
} // namespace vectordb