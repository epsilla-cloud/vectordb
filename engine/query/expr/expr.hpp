#pragma once

#include <string>

#include "expr_types.hpp"

namespace vectordb {
namespace query {
namespace expr {

  class Expr {
    public:
      Expr() = default;

      ExprNodePtr ParseFromStr(std::string expression);
  };

  using ExprPtr = std::shared_ptr<Expr>;

} // namespace expr
} // namespace query
} // namespace vectordb