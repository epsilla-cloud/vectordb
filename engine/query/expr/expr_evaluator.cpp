#include "expr_evaluator.hpp"

#include <cmath>
#include <iostream>

namespace vectordb {
namespace query {
namespace expr {

ExprEvaluator::ExprEvaluator(
    std::vector<ExprNodePtr>& nodes,
    std::unordered_map<std::string, size_t>& field_name_mem_offset_map,
    int64_t& primitive_offset,
    int64_t& string_num,
    char* attribute_table,
    std::string* string_table)
    : nodes_(nodes),
      field_name_mem_offset_map_(field_name_mem_offset_map),
      primitive_offset_(primitive_offset),
      string_num_(string_num),
      attribute_table_(attribute_table),
      string_table_(string_table) {
}

std::string ExprEvaluator::GetStrFieldValue(const std::string& field_name, const int64_t& cand_ind) {
  auto offset = field_name_mem_offset_map_[field_name] + cand_ind * string_num_;
  return string_table_[offset];
}

bool ExprEvaluator::GetBoolFieldValue(const std::string& field_name, const int64_t& cand_ind) {
  auto offset = field_name_mem_offset_map_[field_name] + cand_ind * primitive_offset_;
  return reinterpret_cast<bool*>(attribute_table_[offset]);
}

int64_t ExprEvaluator::GetIntFieldValue(const std::string& field_name, const int64_t& cand_ind, NodeType& node_type) {
  auto offset = field_name_mem_offset_map_[field_name] + cand_ind * primitive_offset_;
  int64_t result;
  switch (node_type) {
    case NodeType::Int1Attr: {
      int8_t* ptr = reinterpret_cast<int8_t*>(&attribute_table_[offset]);
      result = (int64_t)(*ptr);
      break;
    }
    case NodeType::Int2Attr: {
      int16_t* ptr = reinterpret_cast<int16_t*>(&attribute_table_[offset]);
      result = (int64_t)(*ptr);
      break;
    }
    case NodeType::Int4Attr: {
      int32_t* ptr = reinterpret_cast<int32_t*>(&attribute_table_[offset]);
      result = (int64_t)(*ptr);
      break;
    }
    case NodeType::Int8Attr: {
      int64_t* ptr = reinterpret_cast<int64_t*>(&attribute_table_[offset]);
      result = (int64_t)(*ptr);
      break;
    }
    default:
      result = 0;
  }
  std::cout << result << std::endl;
  return result;
}

double ExprEvaluator::GetRealNumberFieldValue(const std::string& field_name, const int64_t& cand_ind, NodeType& node_type) {
  auto offset = field_name_mem_offset_map_[field_name] + cand_ind * primitive_offset_;
  if (node_type == NodeType::DoubleAttr) {
    double* ptr = reinterpret_cast<double*>(&attribute_table_[offset]);
    return *ptr;
  } else if (node_type == NodeType::FloatAttr) {
    float* ptr = reinterpret_cast<float*>(&attribute_table_[offset]);
    return (double)(*ptr);
  } else {
    return 0.0;
  }
}

std::string ExprEvaluator::StrEvaluate(const int& node_index, const int64_t& cand_ind) {
  ExprNodePtr root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::StringConst) {
    return root->str_value;
  } else if (node_type == NodeType::StringAttr) {
    auto name = root->field_name;
    return GetStrFieldValue(name, cand_ind);
  } else if (node_type == NodeType::Add) {
    auto left = StrEvaluate(root->left, cand_ind);
    auto right = StrEvaluate(root->right, cand_ind);
    return left + right;
  }
  return "";
}

double ExprEvaluator::NumEvaluate(const int& node_index, const int64_t& cand_ind) {
  ExprNodePtr root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::IntConst) {
    return static_cast<double>(root->int_value);
  } else if (node_type == NodeType::DoubleConst) {
    return root->double_value;
  } else if (
      node_type == NodeType::Int1Attr ||
      node_type == NodeType::Int2Attr ||
      node_type == NodeType::Int4Attr ||
      node_type == NodeType::Int8Attr) {
    auto name = root->field_name;
    return GetIntFieldValue(name, cand_ind, node_type);
  } else if (node_type == NodeType::DoubleAttr || node_type == NodeType::FloatAttr) {
    auto name = root->field_name;
    return GetRealNumberFieldValue(name, cand_ind, node_type);
  } else if (root->left != -1 && root->right != -1) {
    auto left = NumEvaluate(root->left, cand_ind);
    auto right = NumEvaluate(root->right, cand_ind);
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

bool ExprEvaluator::LogicalEvaluate(const int& node_index, const int64_t& cand_ind) {
  if (node_index < 0) {
    return true;
  }

  ExprNodePtr root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::BoolConst) {
    return root->bool_value;
  } else if (node_type == NodeType::BoolAttr) {
    auto name = root->field_name;
    return GetBoolFieldValue(name, cand_ind);
  } else if (node_type == NodeType::NOT) {
    auto child_index = root->left;
    auto left = LogicalEvaluate(child_index, cand_ind);
    return !left;
  } else if (root->left != -1 && root->right != -1) {
    auto left_index = root->left;
    auto right_index = root->right;

    auto value_type = root->value_type;
    if (node_type == NodeType::EQ || node_type == NodeType::NE) {
      auto child_value_type = nodes_[left_index]->value_type;
      if (child_value_type == ValueType::STRING) {
        auto left = StrEvaluate(left_index, cand_ind);
        auto right = StrEvaluate(right_index, cand_ind);
        return node_type == NodeType::EQ ? left == right : left != right;
      } else if (child_value_type == ValueType::BOOL) {
        auto left = LogicalEvaluate(left_index, cand_ind);
        auto right = LogicalEvaluate(right_index, cand_ind);
        return node_type == NodeType::EQ ? left == right : left != right;
      } else {
        auto left = NumEvaluate(left_index, cand_ind);
        auto right = NumEvaluate(right_index, cand_ind);
        return node_type == NodeType::EQ ? left == right : left != right;
      }
    } else if (node_type == NodeType::AND || node_type == NodeType::OR) {
      auto left = LogicalEvaluate(left_index, cand_ind);
      auto right = LogicalEvaluate(right_index, cand_ind);
      return node_type == NodeType::AND ? (left && right) : (left || right);
    } else {
      auto left = NumEvaluate(left_index, cand_ind);
      auto right = NumEvaluate(right_index, cand_ind);
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

ExprEvaluator::~ExprEvaluator() {}

}  // namespace expr
}  // namespace query
}  // namespace vectordb