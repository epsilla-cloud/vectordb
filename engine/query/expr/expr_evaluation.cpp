#include "expr_evaluation.hpp"

namespace vectordb {
namespace query {
namespace expr {

ExprEvaluation::ExprEvaluation(
    std::vector<ExprNodePtr>& nodes,
    std::unordered_map<std::string, std::any>& field_value_map)
    : nodes_(nodes), field_value_map_(field_value_map) {
}

std::string ExprEvaluation::StrEvaluate(const int& node_index) {
  ExprNodePtr root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::StringConst) {
    return root->str_value;
  } else if (node_type == NodeType::StringAttr) {
    auto name = root->field_name;
    if (field_value_map_.find(name) != field_value_map_.end()) {
      return std::any_cast<std::string>(field_value_map_.at(name));
    } else {
      return "";
    }
  } else if (node_type == NodeType::Add) {
    auto left = StrEvaluate(root->left);
    auto right = StrEvaluate(root->right);
    return left + right;
  }
  return "";
}

double ExprEvaluation::NumEvaluate(const int& node_index) {
  ExprNodePtr root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::IntConst) {
    return static_cast<double>(root->int_value);
  } else if (node_type == NodeType::DoubleConst) {
    return root->double_value;
  } else if (node_type == NodeType::IntAttr || node_type == NodeType::DoubleAttr) {
    auto name = root->field_name;
    if (field_value_map_.find(name) != field_value_map_.end()) {
      return std::any_cast<double>(field_value_map_.at(name));
    } else {
      return 0.0;
    }
  } else if (root->left != -1 && root->right != -1) {
    auto left = NumEvaluate(root->left);
    auto right = NumEvaluate(root->right);
    switch (node_type) {
      case NodeType::Add:
        return left + right;
      case NodeType::Subtract:
        return left - right;
      case NodeType::Multiply:
        return left * right;
      case NodeType::Divide:
        return left / right;
      case NodeType::Module:
        return left % right;
    }
  }
  return 0.0;
}

bool ExprEvaluation::LogicalEvaluate(const int& node_index) {
  if (node_index < 0) {
    return false;
  }

  ExprNodePtr root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::BoolConst) {
    return root->bool_value;
  } else if (node_type == NodeType::BoolAttr) {
    auto name = root->field_name;
    if (field_value_map_.find(name) != field_value_map_.end()) {
      return std::any_cast<bool>(field_value_map_.at(name));
    } else {
      return false;
    }
  } else if (node_type == NodeType::NOT) {
    auto child_index = root->left;
    auto left = LogicalEvaluate(child_index);
    return !left;
  } else if (root->left != -1 && root->right != -1) {
    auto left_index = root->left;
    auto right_index = root->right;

    auto value_type = root->value_type;
    if (node_type == NodeType::EQ || node_type == NodeType::NE) {
      auto child_value_type = nodes_[left_index]->value_type;
      if (child_value_type == ValueType::STRING) {
        auto left = StrEvaluate(left_index);
        auto right = StrEvaluate(right_index);
        return node_type == NodeType::EQ ? left == right : left != right;
      } else if (child_value_type == ValueType::BOOL) {
        auto left = LogicalEvaluate(left_index);
        auto right = LogicalEvaluate(right_index);
        return node_type == NodeType::EQ ? left == right : left != right;
      } else {
        auto left = NumEvaluate(left_index);
        auto right = NumEvaluate(right_index);
        return node_type == NodeType::EQ ? left == right : left != right;
      }
    } else if (node_type == NodeType::AND || node_type == NodeType::OR) {
      auto left = LogicalEvaluate(left_index);
      auto right = LogicalEvaluate(right_index);
      return node_type == NodeType::AND ? left && right : left || right;
    } else {
      auto left = NumEvaluate(left_index);
      auto right = NumEvaluate(right_index);
      switch (node_type) {
        case NodeType::GT:
          return left > right;
        case NodeType::GTE:
          return left >= right;
        case NodeType::LT:
          return left < right;
        case NodeType::LTE:
          return left <= right;
      }
    }
  }
  return false;
}

ExprEvaluation::~ExprEvaluation() { }

} // namespace expr
} // namespace query
} // namespace vector