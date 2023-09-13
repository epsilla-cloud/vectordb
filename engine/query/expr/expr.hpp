#include <string>
#include <unordered_map>

#include "db/catalog/meta_types.hpp"
#include "expr_types.hpp"
#include "utils/json.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace query {
namespace expr {

class Expr {
 public:
  static Status ParseNodeFromStr(
      std::string expression,
      std::vector<ExprNodePtr>& nodes,
      std::unordered_map<std::string, vectordb::engine::meta::FieldType>& field_map);
  static Status DumpToJson(ExprNodePtr& node, Json& json);
};

}  // namespace expr
}  // namespace query
}  // namespace vectordb