#pragma once

#include <string>

#include "expr_types.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace query {
namespace expr {

  class Expr {
    public:
      Expr() = default;

      Status ParseNodeFromStr(std::string expression, std::vector<ExprNodePtr>& nodes);

      Status DumpToJson(ExprNodePtr& node, Json& json);
  };

  using ExprPtr = std::shared_ptr<Expr>;

} // namespace expr
} // namespace query
} // namespace vectordb