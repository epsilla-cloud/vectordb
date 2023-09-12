#include <any>
#include <unordered_map>
#include <vector>

#include "expr_types.hpp"

namespace vectordb {
namespace query {
namespace expr {

class ExprEvaluator {
  public:
    explicit ExprEvaluator(std::vector<ExprNodePtr>& nodes);

    ~ExprEvaluator();

    bool LogicalEvaluate(const int& node_index, std::unordered_map<std::string, std::any>& field_value_map);

  private:
    std::string StrEvaluate(const int& node_index, std::unordered_map<std::string, std::any>& field_value_map);
    double NumEvaluate(const int& node_index, std::unordered_map<std::string, std::any>& field_value_map);

  public:
    std::vector<ExprNodePtr> nodes_;
};

} // namespace expr
} // namespace query
} // namespace vector
