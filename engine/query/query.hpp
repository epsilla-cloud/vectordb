#pragma once

#include <iostream>
#include <string>
#include <vector>

namespace vectordb {
namespace query {

enum class CompareOperator {
    LT,
    LTE,
    EQ,
    GT,
    GTE,
    NE,
};

enum class QueryRelation {
    INVALID,
    AND,
    OR,
};


} // namespace query
} // namespace vectord