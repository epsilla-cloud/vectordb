#pragma once

#include <iostream>
#include <sstream>
#include <algorithm>
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
    String,
    Attribute,
    Operator,
  };

  bool isArithChar(char c) {
    return c == '+' || c == '-' || c == '*' || c == '/' || c == '%';
  };

  bool isCompareChar(char c) {
    return c == '>' || c == '<' || c == '=' || c == '!';
  };

  bool isArithStr(std::string str) {
    return str == "+" || str == "-" || str == "*" || str == "/" || str == "%";
  };

  bool isCompareStr(std::string str) {
    return str == ">" || str == ">=" || str == "==" || str == "<=" || str == "<" || str == "!=";
  };

  bool isLogicalStr(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
        return std::toupper(c);
    });
    return str == "AND" || str == "OR" || str == "NOT";
  };

  bool isOperator(std::string str) {
    return isArithStr(str) || isCompareStr(str) || isLogicalStr(str);
  };

  int getPrecedence(std::string& op) {
    if (isLogicalStr(op))
      return 1;
    else if (isCompareStr(op))
      return 2;
    if (op == "+" || op == "-")
      return 3;
    else if (op == "*" || op == "/" || op == "%")
      return 4;
    return 0;
  };

  Status SplitTokens(std::string& expression, std::vector<std::string>& tokens) {
    std::vector<std::string> token_list;
    State state = State::Start;
    std::string cur_token;
    bool parsing_err = false;

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
            } else if (c != '=' && c != '!') {
              token_list.push_back(std::string(1, c));
            } else {
              parsing_err = true;
            }
          } else if (c == '\'') {
            cur_token += c;
            state = State::String;
          } else {
            parsing_err = true;
          }
          break;
        case State::String:
          if (c == '\'') {
            if (cur_token.empty()) {
              parsing_err = true;
            } else if (cur_token.back() == '\'' || i != last_index && expression[i + 1] == '\'') {
              cur_token += c;
            } else {
              cur_token += c;
              token_list.push_back(cur_token);
              cur_token.clear();
              state = State::Start;
            }
          } else {
            cur_token += c;
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
      if (parsing_err) {
        return Status(DB_UNEXPECTED_ERROR, "Expression is not valid.");
      }
    }
    if (!cur_token.empty()) {
      token_list.push_back(cur_token);
      cur_token.clear();
    }

    tokens = token_list;
    return Status::OK();
  };

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

  bool isBoolConstant(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
        return std::toupper(c);
    });
    return str == "TRUE" || str == "FALSE";
  };

  bool isIntConstant(const std::string& str) {
    size_t size;
    std::string value = str[0] == '-' ? str.substr(1) : str.substr(0);
    int intValue = std::stoi(value, &size);

    return size == value.length();
  };

  bool isDoubleConstant(const std::string& str) {
    size_t size;
    std::string value = str[0] == '-' ? str.substr(1) : str.substr(0);
    int intValue = std::stod(value, &size);

    return size == value.length();
  };

  bool to_bool(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    std::istringstream is(str);
    bool b;
    is >> std::boolalpha >> b;
    return b;
  };

  bool isNotOperator(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
        return std::toupper(c);
    });
    return str == "NOT";
  };

  NodeType GetOperatorNodeType(std::string op) {
    if (isLogicalStr(op)) {
      std::transform(op.begin(), op.end(), op.begin(), [](unsigned char c) {
          return std::toupper(c);
      });
    }

    auto it = OperatorNodeTypeMap.find(op);
    if (it != OperatorNodeTypeMap.end()) {
        return it->second;
    }

    return NodeType::Invalid;
  };

  Status CheckCompatible(std::string& op, ValueType& left, ValueType& right, ValueType& root) {
    if (isLogicalStr(op)) {
      if (left != ValueType::BOOL || right != ValueType::BOOL) {
        return Status(DB_UNEXPECTED_ERROR, op + " statement is invalid.");
      }

      root = ValueType::BOOL;
    }
    if (isCompareStr(op)) {
      if (op != "==" && op != "!=") {
        if (left == ValueType::STRING || left == ValueType::BOOL ||
            right == ValueType::STRING || right == ValueType::BOOL
        ) {
          return Status(DB_UNEXPECTED_ERROR, op + " statement is invalid.");
        }
      } else {
        if (left != right) {
          bool compatible = left == ValueType::INT && right == ValueType::DOUBLE ||
                            left == ValueType::DOUBLE && right == ValueType::INT;
          if (!compatible) {
            return Status(DB_UNEXPECTED_ERROR, op + " statement is invalid.");
          }
        }
      }

      root = ValueType::BOOL;
    }
    if (isArithStr(op)) {
      if (left == ValueType::BOOL || right == ValueType::BOOL) {
        return Status(DB_UNEXPECTED_ERROR, "Boolean value is not compatible with " + op + " operation.");
      }
      if (op != "+") {
        if (left == ValueType::STRING || right == ValueType::STRING) {
          return Status(DB_UNEXPECTED_ERROR, "String value is not compatible with " + op + " operation.");
        } else {
          if (left == ValueType::DOUBLE || right == ValueType::DOUBLE) {
            root = ValueType::DOUBLE;
          } else {
            root = ValueType::INT;
          }
        }
      } else {
        if (left == ValueType::STRING && right == ValueType::STRING) {
          root = ValueType::STRING;
        } else {
          if (left == ValueType::STRING || right == ValueType::STRING) {
            return Status(DB_UNEXPECTED_ERROR, op + " statement is invalid.");
          }
          if (left == ValueType::DOUBLE || right == ValueType::DOUBLE) {
            root = ValueType::DOUBLE;
          } else {
            root = ValueType::INT;
          }
        }
      }
    }

    return Status::OK();
  };

  Status GenerateNodes(std::vector<std::string>& tokens, std::vector<ExprNodePtr>& nodes) {
    std::stack<ExprNodePtr> nodeStack;
    std::vector<ExprNodePtr> node_list;

    for (std::string token : tokens) {
      if (isOperator(token)) {
        ExprNodePtr node = std::make_shared<ExprNode>();
        if (isNotOperator(token)) {
          if (nodeStack.empty()) {
            return Status(DB_UNEXPECTED_ERROR, "Expression is invalid.");
          }

          ExprNodePtr child_node = nodeStack.top();
          nodeStack.pop();
          if (child_node->value_type != ValueType::BOOL) {
            return Status(DB_UNEXPECTED_ERROR, "NOT statement is invalid.");
          }

          node->node_type = NodeType::NOT;
          node->value_type = ValueType::BOOL;
          node_list.push_back(child_node);
          node->left = node_list.size() - 1;

          nodeStack.push(node);
        } else {
          if (nodeStack.size() < 2) {
            return Status(DB_UNEXPECTED_ERROR, "Expression is invalid.");
          }

          node->node_type = GetOperatorNodeType(token);
          ExprNodePtr left_node = nodeStack.top();
          nodeStack.pop();
          ExprNodePtr right_node = nodeStack.top();
          nodeStack.pop();

          Status compability_status = CheckCompatible(token, left_node->value_type, right_node->value_type, node->value_type);
          if (!compability_status.ok()) {
            return compability_status;
          }
          node_list.push_back(left_node);
          node_list.push_back(right_node);
          node->left = node_list.size() - 2;
          node->right = node_list.size() - 1;

          nodeStack.push(node);
        }
      } else {
        ExprNodePtr node = std::make_shared<ExprNode>();
        if (isBoolConstant(token)) {
          node->node_type = NodeType::BoolConst;
          node->value_type = ValueType::BOOL;
          node->value.boolValue = to_bool(token);
          std::cout << node->value.boolValue << std::endl;
        } else if (token[0] == '\'') {
          node->node_type = NodeType::StringConst;
          node->value_type = ValueType::STRING;
          node->value.strValue = token.substr(1, token.size() - 2);
          std::cout << node->value.strValue << std::endl;
        } else if (isIntConstant(token)) {
          node->node_type = NodeType::IntConst;
          node->value_type = ValueType::INT;
          node->value.intValue = std::stoi(token);
          std::cout << node->value.intValue << std::endl;
        } else if (isDoubleConstant(token)) {
          node->node_type = NodeType::DoubleConst;
          node->value_type = ValueType::DOUBLE;
          node->value.doubleValue = std::stod(token);
          std::cout << node->value.doubleValue << std::endl;
        } else {
          // TODO: attribute node validating with table schema
          // node->node_type = NodeType::StringAttr;
          // node->value_type = ValueType::STRING;
          node->field_name = token;
        }

        nodeStack.push(node);
      }
    }

    if (nodeStack.size() != 1) {
      return Status(DB_UNEXPECTED_ERROR, "expression is invalid.");
    }

    node_list.push_back(nodeStack.top());
    nodeStack.pop();

    nodes = node_list;
    return Status::OK();
  };

  Status Expr::ParseNodeFromStr(std::string expression, std::vector<ExprNodePtr>& nodes) {
    // Parse string into token arr
    std::vector<std::string> token_list;
    Status parsing_status = SplitTokens(expression, token_list);
    if (!parsing_status.ok()) {
      return parsing_status;
    }

    std::cout << expression + " transfer to: " + boost::algorithm::join(token_list, ", ") << std::endl;

    std::vector<std::string> tokens_queue;
    tokens_queue = ShuntingYard(token_list);
    std::cout << "After SY: " + boost::algorithm::join(tokens_queue, ", ") << std::endl;

    Status nodes_status = GenerateNodes(tokens_queue, nodes);
    if (!nodes_status.ok()) {
      return nodes_status;
    }

    return Status::OK();
  };

  Status Expr::DumpToJson(ExprNodePtr& node, Json& json) {
    json.LoadFromString("{}");
    json.SetInt("valueType", static_cast<int>(node->value_type));
    json.SetInt("nodeType", static_cast<int>(node->node_type));
    if (node->field_name != "") {
      json.SetString("fieldName", node->field_name);
    }
    json.SetString("valueString", node->value.strValue);
    json.SetInt("valueInt", node->value.intValue);
    json.SetDouble("valueDouble", node->value.doubleValue);
    json.SetBool("valueBool", node->value.boolValue);
    json.SetInt("leftChild", node->left);
    json.SetInt("rightChild", node->right);

    return Status::OK();
  }

} // namespace expr
} // namespace query
} // namespace vectordb