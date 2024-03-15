#pragma once

#include <memory>
#include <string>
#include <unordered_map>

namespace vectordb {
namespace query {
namespace expr {

enum class NodeType {
  Invalid,
  IntConst,
  StringConst,
  DoubleConst,
  BoolConst,
  Int1Attr,
  Int2Attr,
  Int4Attr,
  Int8Attr,
  StringAttr,
  DoubleAttr,
  FloatAttr,
  BoolAttr,
  GeoPointAttr,
  Add,
  Subtract,
  Multiply,
  Divide,
  Module,
  LT,
  LTE,
  EQ,
  GT,
  GTE,
  NE,
  AND,
  OR,
  NOT,
  FunctionCall,
};

const std::unordered_map<std::string, NodeType> OperatorNodeTypeMap = {
    {"+", NodeType::Add},
    {"-", NodeType::Subtract},
    {"*", NodeType::Multiply},
    {"/", NodeType::Divide},
    {"%", NodeType::Module},
    {">", NodeType::GT},
    {">=", NodeType::GTE},
    {"=", NodeType::EQ},
    {"<=", NodeType::LTE},
    {"<", NodeType::LT},
    {"<>", NodeType::NE},
    {"AND", NodeType::AND},
    {"OR", NodeType::OR},
    {"NOT", NodeType::NOT}};

enum class ValueType {
  STRING,
  INT,
  DOUBLE,
  BOOL,
  GEO_POINT,
};

struct ExprNode {
  ValueType value_type;
  NodeType node_type;
  std::string field_name;  // Only attribute has it.
  size_t left;
  size_t right;
  std::string str_value;
  int64_t int_value;
  double double_value;
  bool bool_value;
  std::string function_name;  // Name of the function being called
  std::vector<size_t> arguments;  // Indices of the nodes representing arguments
};
using ExprNodePtr = std::shared_ptr<ExprNode>;

}  // namespace expr
}  // namespace query
}  // namespace vectordb