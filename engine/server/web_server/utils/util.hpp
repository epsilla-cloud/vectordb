#pragma once

#include "db/catalog/meta.hpp"

namespace vectordb {
namespace server {
namespace web {

class WebUtil {
 public:
  static std::string JoinStrs(const std::vector<std::string>& strings, const std::string& delimiter);
};

}  // namespace web
}  // namespace server
}  // namespace vectordb
