#include "expr_evaluator.hpp"

#include <cmath>
#include <iostream>
#include <vector>
#include <regex>
#include <string>

namespace vectordb {
namespace query {
namespace expr {


// Function to escape regex special characters in the input string
std::string escapeRegexSpecialChars(const std::string& likePattern) {
  static const std::regex specialChars(R"([\.\+\*\?\^\$\(\)\[\]\{\}\|\\])");
  std::string regexPattern = std::regex_replace(likePattern, specialChars, R"(\$&)");
  return regexPattern;
}

// Function to convert SQL LIKE pattern to regex pattern
std::string likeToRegexPattern(const std::string& likePattern) {
  std::string regexPattern = escapeRegexSpecialChars(likePattern);
  // Replace SQL LIKE wildcard '%' with regex '.*'
  regexPattern = std::regex_replace(regexPattern, std::regex("%"), ".*");
  // Replace SQL LIKE wildcard '_' with regex '.'
  regexPattern = std::regex_replace(regexPattern, std::regex("_"), ".");
  return regexPattern;
}

// Function to perform regex match
bool regexMatch(const std::string& str, const std::string& pattern) {
  std::regex regexPattern(pattern);
  return std::regex_match(str, regexPattern);
}

ExprEvaluator::ExprEvaluator(
    std::vector<ExprNodePtr>& nodes,
    std::unordered_map<std::string, size_t>& field_name_mem_offset_map,
    int64_t& primitive_offset,
    int64_t& string_num,
    char* attribute_table,
    std::vector<engine::VariableLenAttrColumnContainer>& var_len_attr_table_)
    : nodes_(nodes),
      field_name_mem_offset_map_(field_name_mem_offset_map),
      primitive_offset_(primitive_offset),
      var_len_attr_num_(string_num),
      attribute_table_(attribute_table),
      var_len_attr_table_(var_len_attr_table_) {
}

std::string ExprEvaluator::GetStrFieldValue(const std::string& field_name, const int64_t& cand_ind) {
  return std::get<std::string>(var_len_attr_table_[field_name_mem_offset_map_[field_name]][cand_ind]);
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

vectordb::engine::index::GeospatialIndex::point_t ExprEvaluator::GeoPointEvaluate(const std::string& field_name,  const int64_t& cand_ind) {
  auto offset = field_name_mem_offset_map_[field_name] + cand_ind * primitive_offset_;
  double lat = *(reinterpret_cast<double*>(&attribute_table_[offset]));
  double lon = *(reinterpret_cast<double*>(&attribute_table_[offset + sizeof(double)]));
  return vectordb::engine::index::GeospatialIndex::point_t(lat, lon);
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

double ExprEvaluator::NumEvaluate(const int& node_index, const int64_t& cand_ind, const double distance) {
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
    if (name == "@distance") {
      return distance;
    }
    return GetRealNumberFieldValue(name, cand_ind, node_type);
  } else if (root->left != -1 && root->right != -1) {
    auto left = NumEvaluate(root->left, cand_ind, distance);
    auto right = NumEvaluate(root->right, cand_ind, distance);
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
  return LogicalEvaluate(node_index, cand_ind, 0);
}

bool ExprEvaluator::LogicalEvaluate(const int& node_index, const int64_t& cand_ind, const double distance) {
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
  } else if (node_type == NodeType::IN) {
    auto len = root->arguments.size();
    auto src = StrEvaluate(root->arguments[len - 1], cand_ind);
    for (auto i = 0; i < len - 1; i++) {
      auto tgt = StrEvaluate(root->arguments[i], cand_ind);
      if (src == tgt) {
        return true;
      }
    }
    return false;
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
        auto left = NumEvaluate(left_index, cand_ind, distance);
        auto right = NumEvaluate(right_index, cand_ind, distance);
        return node_type == NodeType::EQ ? left == right : left != right;
      }
    } else if (node_type == NodeType::AND || node_type == NodeType::OR) {
      auto left = LogicalEvaluate(left_index, cand_ind);
      auto right = LogicalEvaluate(right_index, cand_ind);
      return node_type == NodeType::AND ? (left && right) : (left || right);
    } else if (node_type == NodeType::FunctionCall) {
      if (root->function_name == "NEARBY") {
        auto point = GeoPointEvaluate(nodes_[root->arguments[0]]->field_name, cand_ind);
        auto lat = NumEvaluate(root->arguments[1], cand_ind, distance);
        auto lon = NumEvaluate(root->arguments[2], cand_ind, distance);
        auto dist = NumEvaluate(root->arguments[3], cand_ind, distance);
        return vectordb::engine::index::GeospatialIndex::distance(point, vectordb::engine::index::GeospatialIndex::point_t(lat, lon)) <= dist;
      }
      // Support more functions
    } else if (node_type == NodeType::LIKE) {
      auto left = StrEvaluate(left_index, cand_ind);
      auto right = StrEvaluate(right_index, cand_ind);
      // str LIKE '' iff str == ''
      if (right == "") {
        return left == "";
      }
      // str LIKE '%' always matches
      if (right == "%") {
        return true;
      }
      std::string regexPattern = likeToRegexPattern(right);
      return regexMatch(left, regexPattern);
    } else {
      auto left = NumEvaluate(left_index, cand_ind, distance);
      auto right = NumEvaluate(right_index, cand_ind, distance);
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

int64_t ExprEvaluator::UpliftingGeoIndex(const std::string& field_name, const int& node_index) {
  auto root = nodes_[node_index];
  auto node_type = root->node_type;
  if (node_type == NodeType::FunctionCall) {
    if (root->function_name == "NEARBY") {
      if (nodes_[root->arguments[0]]->field_name == field_name) {
        return node_index;
      }
    }
    return -1;
  }
  if (node_type == NodeType::AND) {
    int64_t left = UpliftingGeoIndex(field_name, root->left);
    if (left != -1) {
      return left;
    }
    int64_t right = UpliftingGeoIndex(field_name, root->right);
    if (right != -1) {
      return right;
    }
  }
  return -1;
}

ExprEvaluator::~ExprEvaluator() {}

}  // namespace expr
}  // namespace query
}  // namespace vectordb