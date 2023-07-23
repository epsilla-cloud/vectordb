#pragma once

#include <iostream>
#include <string>

namespace vectordb {
namespace query {
namespace expr {

    enum class ArithmeticOperator {
        Addition,
        Subtraction,
        Multiplication,
        Division,
        Modulo
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

    union NodeDataType {
        std::string strValue;
        int intValue;
        double doubleValue;
        float floatValue;
        bool boolValue;

        NodeDataType(): strValue("") {}
        ~NodeDataType() {}
    };

    union NodeOperatorType {
        ArithmeticOperator arithmetic;
        CompareOperator compare;
        LogicalOperator logical;

        NodeOperatorType(ArithmeticOperator op): arithmetic(op) {}
        NodeOperatorType(CompareOperator op): compare(op) {}
        NodeOperatorType(LogicalOperator op): logical(op) {}
        NodeOperatorType(): logical(LogicalOperator::INVALID) {}
        ~NodeOperatorType() {}
    };

    struct ExprNode {
        NodeDataType value;
        NodeOperatorType op;
        std::vector<ExprNode> children;
    };
    using ExprNodePtr = std::shared_ptr<ExprNode>;

} // namespace expr
} // namespace query
} // namespace vectordb