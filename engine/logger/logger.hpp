#include <string>

namespace vectordb {
namespace engine {

class Logger {
 public:
  void Error(const std::string& message);
  void Info(const std::string& message);
  void Warning(const std::string& message);
};

}  // namespace engine
}  // namespace vectordb
