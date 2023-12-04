#include "server/web_server/utils/util.hpp"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/catalog/meta.hpp"

namespace vectordb {
namespace server {
namespace web {

std::string WebUtil::JoinStrs(const std::vector<std::string>& strings, const std::string& delimiter) {
  std::ostringstream oss;
  bool first = true;

  for (const auto& str : strings) {
    if (!first) {
      oss << delimiter;
    }
    oss << str;
    first = false;
  }

  return oss.str();
}
}  // namespace web
}  // namespace server
}  // namespace vectordb
