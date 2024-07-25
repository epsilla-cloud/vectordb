#include "logger.hpp"

#include <ctime>
#include <iostream>

namespace vectordb {
namespace engine {

enum class LogLevel {
  DEBUG,
  INFO,
  WARNING,
  ERROR
};

void Log(LogLevel level, const std::string& message) {
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

void Logger::Error(const std::string& message) {
  Log(LogLevel::ERROR, message);
}

void Logger::Info(const std::string& message) {
  Log(LogLevel::INFO, message);
};

void Logger::Warning(const std::string& message) {
  Log(LogLevel::WARNING, message);
}

void Logger::Debug(const std::string& message) {
  Log(LogLevel::DEBUG, message);
}

}  // namespace engine
}  // namespace vectordb