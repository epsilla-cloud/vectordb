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

  bool isArithChar(char c) {
    return c == '+' || c == '-' || c == '*' || c == '/' || c == '%';
  };

  bool isCompareChar(char c) {
    return c == '>' || c == '<' || c == '=';
  };

  bool isArithStr(std::string str) {
    return str == "+" || str == "-" || str == "*" || str == "/" || str == "%";
  };

  bool isCompareStr(std::string str) {
    return str == "+" || str == "-" || str == "*" || str == "/" || str == "%";
  };

  bool isLogicalStr(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
        return std::toupper(c);
    });
    return str == "AND" || str == "OR" || str == "NOT";
  }

  bool isOperator(std::string str) {
    return isArithStr(str) || isCompareStr(str) || isLogicalStr(str);
  };

  int getPrecedence(std::string& op) {
    if (op == "+" || op == "-")
      return 1;
    else if (op == "*" || op == "/" || op == "%")
      return 2;
    else if (isCompareStr(op))
      return 3;
    else if (isLogicalStr(op))
      return 4;
    return 0;
}

  std::vector<std::string> ShuntingYard(const std::vector<std::string>& tokens) {
    std::vector<std::string> res;
    std::stack<std::string> operatorStack;

    for (std::string str : tokens) {
      if (str == "(") {
        operatorStack.push(str);
      } else if (str == ")") {
        while (!operatorStack.empty() && operatorStack.top() != "(") {
          res.push_back(operatorStack.top());
          operatorStack.pop();
        }
        operatorStack.pop(); // Pop the '('
      } else if (isOperator(str)) {
        while (!operatorStack.empty() && getPrecedence(operatorStack.top()) >= getPrecedence(str)) {
          res.push_back(operatorStack.top());
          operatorStack.pop();
        }
        operatorStack.push(str);
      } else {
        res.push_back(str);
      }
    }

    while (!operatorStack.empty()) {
        res.push_back(operatorStack.top());
        operatorStack.pop();
    }

    return res;
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
          } else if (isArithChar(c) || c == '(' || c == ')') {
            if (c == '-' && i != last_index && std::isdigit(expression[i + 1])) {
              if (!token_list.empty()) {
                std::string ele = token_list.back();
                if (!isArithStr(ele) && ele != "(") {
                  token_list.push_back(std::string(1, c));
                } else {
                  cur_token += c;
                  state = State::Number;
                }
              } else {
                cur_token += c;
                state = State::Number;
              }
            } else {
              token_list.push_back(std::string(1, c));
            }
          } else if (isCompareChar(c)) {
            if (i != last_index && isCompareChar(expression[i + 1]) && expression[i + 1] == '=') {
              cur_token += c;
              state = State::Operator;
            } else if (c != '=') {
              token_list.push_back(std::string(1, c));
            } else {
              parsing_err = true;
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
          } else if (isArithChar(c) || c == ')') {
            token_list.push_back(cur_token);
            cur_token.clear();
            token_list.push_back(std::string(1, c));
            state = State::Start;
          } else if (isCompareChar(c)) {
            token_list.push_back(cur_token);
            cur_token.clear();
            if (i != last_index && isCompareChar(expression[i + 1])) {
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
          } else if (isArithChar(c) || c == ')') {
            token_list.push_back(cur_token);
            cur_token.clear();
            token_list.push_back(std::string(1, c));
            state = State::Start;
          } else if (isCompareChar(c)) {
            token_list.push_back(cur_token);
            cur_token.clear();
            if (i != last_index && isCompareChar(expression[i + 1])) {
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

    std::vector<std::string> tokens_queue;
    tokens_queue = ShuntingYard(token_list);
    std::cout << "After SY: " + boost::algorithm::join(tokens_queue, ", ") << std::endl;

    ExprNodePtr node = std::make_shared<ExprNode>();
    node->op = LogicalOperator::AND;
    node->value.intValue = 12345;
    return node;
  };

} // namespace expr
} // namespace query
} // namespace vectordb