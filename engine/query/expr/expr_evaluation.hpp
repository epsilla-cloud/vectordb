#include <any>
#include <unordered_map>
#include <vector>

#include "expr_types.hpp"

namespace vectordb {
namespace query {
namespace expr {

class ExprEvaluation {
  public:
    explicit ExprEvaluation(
      std::vector<ExprNodePtr>& nodes,
      std::unordered_map<std::string, std::any>& field_value_map);

    ~ExprEvaluation();

    bool LogicalEvaluate(const int& node_index);

  private:
    std::string StrEvaluate(const int& node_index);
    double NumEvaluate(const int& node_index);

  public:
    std::vector<ExprNodePtr> nodes_;
    std::unordered_map<std::string, std::any> field_value_map_;
};

} // namespace expr
} // namespace query
} // namespace vector
