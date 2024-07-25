#pragma once

#include <string>

namespace vectordb {
namespace engine {

class Logger {
 public:
  void Info(const std::string& message);
  void Warning(const std::string& message);
  void Error(const std::string& message);
  void Debug(const std::string& message);
};

}  // namespace engine
}  // namespace vectordb
