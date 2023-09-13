#pragma once

#include <memory>
#include <string>
#include <unordered_map>

namespace vectordb {
namespace query {
namespace expr {

enum class ArithmeticOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Module
};

enum class CompareOperator {
    LT,
    LTE,
    EQ,
    GT,
    GTE,
    NE,
};

enum class LogicalOperator {
    INVALID,
    AND,
    OR,
    NOT
};

enum class NodeType {
    Invalid,
    IntConst,
    StringConst,
    DoubleConst,
    BoolConst,
    IntAttr,
    StringAttr,
    DoubleAttr,
    BoolAttr,
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
    NOT
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
    {"NOT", NodeType::NOT}
};

enum class ValueType {
    STRING,
    INT,
    DOUBLE,
    BOOL
};

struct ExprNode {
    ValueType value_type;
    NodeType node_type;
    std::string field_name; // Only attribute has it.
    size_t left;
    size_t right;
    std::string str_value;
    int int_value;
    double double_value;
    bool bool_value;
};
using ExprNodePtr = std::shared_ptr<ExprNode>;

} // namespace expr
} // namespace query
} // namespace vectordb