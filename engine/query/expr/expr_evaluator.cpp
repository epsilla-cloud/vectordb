#include <cmath>

#include "expr_evaluator.hpp"

namespace vectordb {
namespace query {
namespace expr {

ExprEvaluator::ExprEvaluator(std::vector<ExprNodePtr>& nodes)
    : nodes_(nodes) {
}

std::string ExprEvaluator::StrEvaluate(
  const int& node_index,
  std::unordered_map<std::string, std::any>& field_value_map
) {
  ExprNodePtr root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::StringConst) {
    return root->str_value;
  } else if (node_type == NodeType::StringAttr) {
    auto name = root->field_name;
    if (field_value_map.find(name) != field_value_map.end()) {
      return std::any_cast<std::string>(field_value_map.at(name));
    } else {
      return "";
    }
  } else if (node_type == NodeType::Add) {
    auto left = StrEvaluate(root->left, field_value_map);
    auto right = StrEvaluate(root->right, field_value_map);
    return left + right;
  }
  return "";
}

double ExprEvaluator::NumEvaluate(const int& node_index, std::unordered_map<std::string, std::any>& field_value_map) {
  ExprNodePtr root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::IntConst) {
    return static_cast<double>(root->int_value);
  } else if (node_type == NodeType::DoubleConst) {
    return root->double_value;
  } else if (node_type == NodeType::IntAttr) {
    auto name = root->field_name;
    if (field_value_map.find(name) != field_value_map.end()) {
      return std::any_cast<int64_t>(field_value_map.at(name));
    } else {
      return 0.0;
    }
  } else if (node_type == NodeType::DoubleAttr) {
    auto name = root->field_name;
    if (field_value_map.find(name) != field_value_map.end()) {
      return std::any_cast<double>(field_value_map.at(name));
    } else {
      return 0.0;
    }
  } else if (root->left != -1 && root->right != -1) {
    auto left = NumEvaluate(root->left, field_value_map);
    auto right = NumEvaluate(root->right, field_value_map);
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
        return std::fmod(left, right);
    }
  }
  return 0.0;
}

bool ExprEvaluator::LogicalEvaluate(const int& node_index, std::unordered_map<std::string, std::any>& field_value_map) {
  if (node_index < 0) {
    return true;
  }

  ExprNodePtr root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::BoolConst) {
    return root->bool_value;
  } else if (node_type == NodeType::BoolAttr) {
    auto name = root->field_name;
    if (field_value_map.find(name) != field_value_map.end()) {
      return std::any_cast<bool>(field_value_map.at(name));
    } else {
      return false;
    }
  } else if (node_type == NodeType::NOT) {
    auto child_index = root->left;
    auto left = LogicalEvaluate(child_index, field_value_map);
    return !left;
  } else if (root->left != -1 && root->right != -1) {
    auto left_index = root->left;
    auto right_index = root->right;

    auto value_type = root->value_type;
    if (node_type == NodeType::EQ || node_type == NodeType::NE) {
      auto child_value_type = nodes_[left_index]->value_type;
      if (child_value_type == ValueType::STRING) {
        auto left = StrEvaluate(left_index, field_value_map);
        auto right = StrEvaluate(right_index, field_value_map);
        return node_type == NodeType::EQ ? left == right : left != right;
      } else if (child_value_type == ValueType::BOOL) {
        auto left = LogicalEvaluate(left_index, field_value_map);
        auto right = LogicalEvaluate(right_index, field_value_map);
        return node_type == NodeType::EQ ? left == right : left != right;
      } else {
        auto left = NumEvaluate(left_index, field_value_map);
        auto right = NumEvaluate(right_index, field_value_map);
        return node_type == NodeType::EQ ? left == right : left != right;
      }
    } else if (node_type == NodeType::AND || node_type == NodeType::OR) {
      auto left = LogicalEvaluate(left_index, field_value_map);
      auto right = LogicalEvaluate(right_index, field_value_map);
      return node_type == NodeType::AND ? left && right : left || right;
    } else {
      auto left = NumEvaluate(left_index, field_value_map);
      auto right = NumEvaluate(right_index, field_value_map);
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

ExprEvaluator::~ExprEvaluator() { }

} // namespace expr
} // namespace query
} // namespace vector