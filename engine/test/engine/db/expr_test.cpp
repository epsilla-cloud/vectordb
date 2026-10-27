#include "query/expr/expr.hpp"

#include <gtest/gtest.h>
#include <unordered_map>
#include <vector>

TEST(ExprTest, UnbalancedParentheses) {
  std::unordered_map<std::string, vectordb::engine::meta::FieldType> field_map;
  field_map["age"] = vectordb::engine::meta::FieldType::INT4;
  std::vector<vectordb::query::expr::ExprNodePtr> nodes;

  // Unmatched closing parenthesis
  auto status_unmatched_close = vectordb::query::expr::Expr::ParseNodeFromStr("age > 10)", nodes, field_map);
  EXPECT_FALSE(status_unmatched_close.ok());
  EXPECT_EQ(status_unmatched_close.code(), vectordb::INVALID_EXPR);

  // Unmatched opening parenthesis
  auto status_unmatched_open = vectordb::query::expr::Expr::ParseNodeFromStr("(age > 10", nodes, field_map);
  EXPECT_FALSE(status_unmatched_open.ok());
  EXPECT_EQ(status_unmatched_open.code(), vectordb::INVALID_EXPR);

  // Balanced parenthesis should succeed
  auto status_balanced = vectordb::query::expr::Expr::ParseNodeFromStr("(age > 10)", nodes, field_map);
  EXPECT_TRUE(status_balanced.ok());
}
