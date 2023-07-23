#pragma once

#include <iostream>
#include <stack>
#include <queue>
#include <vector>
#include <boost/algorithm/string/join.hpp>

#include "utils/status.hpp"
#include "expr.hpp"

namespace vectordb {
namespace query {
namespace expr {

  enum class State {
    Start,
    Number,
    Attribute,
    Operator,
  };

  std::vector<char> arith_operator = {'+', '-', '*', '/', '%'};
  std::vector<char> compare_operator = {'>', '<', '='};

  bool checkCharExists(std::vector<char> arr, char c) {
    auto it = std::find(arr.begin(), arr.end(), c);
    return it != arr.end();
  };

  ExprNodePtr Expr::ParseFromStr(std::string expression) {
    // Parse string into token arr
    std::vector<std::string> token_list;
    bool parsing_err = false;
    State state = State::Start;
    std::string cur_token;

    size_t last_index = expression.length() - 1;
    for (size_t i = 0; i < expression.length(); i++) {
      char c = expression[i];
      switch (state) {
        case State::Start:
          if (std::isspace(c)) {
            continue;
          } else if (std::isdigit(c)) {
            cur_token += c;
            state = State::Number;
          } else if (std::isalpha(c) || c == '_') {
            cur_token += c;
            state = State::Attribute;
          } else if (checkCharExists(arith_operator, c) || c == '(') {
            if (c == '-' && i != last_index && std::isdigit(expression[i + 1])) {
              cur_token += c;
              state = State::Number;
            } else {
              token_list.push_back(std::string(1, c));
            }
          } else if (checkCharExists(compare_operator, c)) {
            if (i != last_index && checkCharExists(compare_operator, expression[i + 1])) {
              cur_token += c;
              state = State::Operator;
            } else {
              token_list.push_back(std::string(1, c));
            }
          } else {
            parsing_err = true;
          }
          break;
        case State::Attribute:
          if (std::isspace(c)) {
            token_list.push_back(cur_token);
            cur_token.clear();
            state = State::Start;
          } else if (checkCharExists(arith_operator, c) || c == ')') {
            token_list.push_back(cur_token);
            cur_token.clear();
            token_list.push_back(std::string(1, c));
          } else if (checkCharExists(compare_operator, c)) {
            token_list.push_back(cur_token);
            cur_token.clear();
            if (i != last_index && checkCharExists(compare_operator, expression[i + 1])) {
              cur_token += c;
              state = State::Operator;
            } else {
              token_list.push_back(std::string(1, c));
              state = State::Start;
            }
          } else if (std::isalnum(c) || c == '_') {
            cur_token += c;
          } else {
            parsing_err = true;
          }
          break;
        case State::Number:
          if (std::isspace(c)) {
            token_list.push_back(cur_token);
            cur_token.clear();
            state = State::Start;
          } else if (c == '%' || c == '/' || c == '*' || c == '+' || c == '-' || c == ')') {
            token_list.push_back(cur_token);
            cur_token.clear();
            token_list.push_back(std::string(1, c));
            state = State::Start;
          } else if (checkCharExists(compare_operator, c)) {
            token_list.push_back(cur_token);
            cur_token.clear();
            if (i != last_index && checkCharExists(compare_operator, expression[i + 1])) {
              cur_token += c;
              state = State::Operator;
            } else {
              token_list.push_back(std::string(1, c));
              state = State::Start;
            }
          } else if (std::isdigit(c)) {
            cur_token += c;
          } else {
            parsing_err = true;
          }
          break;
        case State::Operator:
          if (cur_token.length() == 1 && c == '=') {
            cur_token += c;
            token_list.push_back(cur_token);
            cur_token.clear();
            state = State::Start;
          } else {
            parsing_err = true;
          }
          break;
      }
    }
    if (!cur_token.empty()) {
      token_list.push_back(cur_token);
      cur_token.clear();
    }

    if (parsing_err) {
      return nullptr;
    }
    std::cout << expression + " transfer to: " + boost::algorithm::join(token_list, ", ") << std::endl;

    std::stack<std::string> op_stack;
    std::queue<std::string> output_queue;
    ExprNodePtr node = std::make_shared<ExprNode>();
    node->op = LogicalOperator::AND;
    node->value.intValue = 12345;
    return node;
  };

} // namespace expr
} // namespace query
} // namespace vectordb