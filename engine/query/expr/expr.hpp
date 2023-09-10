#pragma once

#include <string>
#include <unordered_map>

#include "expr_types.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace query {
namespace expr {

class Expr {
  public:
    static Status ParseNodeFromStr(
      std::string expression,
      std::vector<ExprNodePtr>& nodes,
      std::unordered_map<std::string, engine::meta::FieldType>& field_map
    );

    static Status DumpToJson(ExprNodePtr& node, Json& json);
};

} // namespace expr
} // namespace query
} // namespace vectordb