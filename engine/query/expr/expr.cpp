#include <iostream>
#include <sstream>
#include <algorithm>
#include <regex>
#include <stack>
#include <queue>
#include <unordered_map>
#include <vector>
#include <boost/algorithm/string/join.hpp>

#include "db/catalog/meta_types.hpp"
#include "utils/status.hpp"
#include "expr.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace query {
namespace expr {

std::shared_ptr<vectordb::engine::Logger> logger = std::make_shared<vectordb::engine::Logger>();

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
  return c == '>' || c == '<' || c == '=';
};

bool isArithStr(std::string str) {
  return str == "+" || str == "-" || str == "*" || str == "/" || str == "%";
};

bool isCompareStr(std::string str) {
  return str == ">" || str == ">=" || str == "=" || str == "<=" || str == "<" || str == "<>";
};

bool isLogicalStr(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
      return std::toupper(c);
  });
  return str == "AND" || str == "OR" || str == "NOT";
};

bool isUnsupportedLogicalOp(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
      return std::toupper(c);
  });
  return str == "ALL" || str == "ANY" || str == "BETWEEN" || str == "EXISTS" || str == "IN" ||
    str == "LIKE" || str == "SOME";
}

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

  size_t last_index = expression.length() - 1;
  for (size_t i = 0; i < expression.length(); ) {
    char c = expression[i];
    switch (state) {
      case State::Start:
        if (std::isspace(c)) {
          i++;
          continue;
        } else if (std::isdigit(c)) {
          state = State::Number;
        } else if (std::isalpha(c) || c == '_') {
          state = State::Attribute;
        } else if (c == '(' || c == ')') {
          token_list.push_back(std::string(1, c));
          i++;
        } else if (isArithChar(c) || isCompareChar(c)) {
          if (c == '-' && i != last_index && std::isdigit(expression[i + 1])) {
            if (!token_list.empty()) {
              std::string ele = token_list.back();
              if (!isOperator(ele) && ele != "(") {
                state = State::Operator;
              } else {
                cur_token += c;
                i++;
                state = State::Number;
              }
            } else {
              cur_token += c;
              i++;
              state = State::Number;
            }
          } else {
            state = State::Operator;
          }
        } else if (c == '\'') {
          state = State::String;
        } else if (c == '&' || c == '|' || c == '^') {
          return Status(NOT_IMPLEMENTED_ERROR, "Epsilla does not support bitwise operators yet.");
        } else {
          return Status(INVALID_EXPR, "Filter expression is not valid.");
        }
        break;
      case State::String:
        if (c == '\'') {
          i++;
          cur_token += c;
          if (cur_token == "''") {
            return Status(INVALID_EXPR, "String constant cannot be empty.");
          }
          if (cur_token.size() > 2) {
            token_list.push_back(cur_token);
            cur_token.clear();
            state = State::Start;
          }
        } else {
          if (i == last_index) {
            return Status(INVALID_EXPR, "Missing terminating '.");
          } else {
            cur_token += c;
            i++;
          }
        }
        break;
      case State::Attribute:
        if (std::isspace(c) || c == ')' || isArithChar(c) || isCompareChar(c)) {
          token_list.push_back(cur_token);
          cur_token.clear();
          state = State::Start;
        } else if (std::isalnum(c) || c == '_') {
          cur_token += c;
          i++;
        } else {
          return Status(INVALID_EXPR, "Invalid name: " + (cur_token += c));
        }
        break;
      case State::Number:
        if (std::isspace(c) || c == ')' || isArithChar(c) || isCompareChar(c)) {
          if (std::count(cur_token.begin(), cur_token.end(), '.') > 1) {
            return Status(INVALID_EXPR, cur_token + " is not a valid number.");
          } else {
            token_list.push_back(cur_token);
            cur_token.clear();
            i++;
            state = State::Start;
          }
        } else if (std::isdigit(c)) {
          cur_token += c;
          i++;
        } else if (c == '.' && i != last_index && std::isdigit(expression[i + 1])) {
          cur_token += c;
          i++;
        } else {
          return Status(INVALID_EXPR, "Filter expression is not valid.");
        }
        break;
      case State::Operator:
        if (isArithChar(c)) {
          if (i != last_index && expression[i + 1] == '=') {
            return Status(NOT_IMPLEMENTED_ERROR, "Epsilla does not support compound operators yet.");
          }
          token_list.push_back(std::string(1, c));
          i++;
          state = State::Start;
        } else if (isCompareChar(c)) {
          cur_token += c;
          if (i != last_index && isCompareChar(expression[i + 1])) {
            i++;
          } else {
            if (isCompareStr(cur_token)) {
              token_list.push_back(cur_token);
              cur_token.clear();
              i++;
              state = State::Start;
            } else {
              return Status(INVALID_EXPR, "'" + cur_token + "' is an invalid operator.");
            }
          }
        }
        break;
    }
  }
  if (!cur_token.empty()) {
    token_list.push_back(cur_token);
    cur_token.clear();
  }

  if (token_list.size() < 3) {
    return Status(INVALID_EXPR, "Filter expression is invalid.");
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
  std::regex integerPattern("^[-+]?\\d+$");

  return std::regex_match(str, integerPattern);
};

bool isDoubleConstant(const std::string& str) {
  std::regex doublePattern("^[-+]?\\d+\\.\\d+(?:[eE][-+]?\\d+)?$");

  return std::regex_match(str, doublePattern);
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
      return Status(INVALID_EXPR, op + " statement is invalid.");
    }

    root = ValueType::BOOL;
  }
  if (isCompareStr(op)) {
    if (op != "=" && op != "<>") {
      if (left == ValueType::STRING || left == ValueType::BOOL ||
          right == ValueType::STRING || right == ValueType::BOOL
      ) {
        return Status(INVALID_EXPR, op + " statement is invalid.");
      }
    } else {
      if (left != right) {
        bool compatible = left == ValueType::INT && right == ValueType::DOUBLE ||
                          left == ValueType::DOUBLE && right == ValueType::INT;
        if (!compatible) {
          return Status(INVALID_EXPR, op + " statement is invalid.");
        }
      }
    }

    root = ValueType::BOOL;
  }
  if (isArithStr(op)) {
    if (left == ValueType::BOOL || right == ValueType::BOOL) {
      return Status(INVALID_EXPR, "Boolean value is not compatible with " + op + " operation.");
    }
    if (op != "+") {
      if (left == ValueType::STRING || right == ValueType::STRING) {
        return Status(INVALID_EXPR, "String value is not compatible with " + op + " operation.");
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
          return Status(INVALID_EXPR, op + " statement is invalid.");
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

Status GenerateNodes(
  std::vector<std::string>& tokens,
  std::vector<ExprNodePtr>& nodes,
  std::unordered_map<std::string, engine::meta::FieldType>& field_map
) {
  std::stack<ExprNodePtr> nodeStack;
  std::vector<ExprNodePtr> node_list;

  for (std::string token : tokens) {
    if (isUnsupportedLogicalOp(token)) {
      return Status(NOT_IMPLEMENTED_ERROR, "Epsilla does not support " + token + " yet.");
    } else if (isOperator(token)) {
      ExprNodePtr node = std::make_shared<ExprNode>();
      if (isNotOperator(token)) {
        if (nodeStack.empty()) {
          return Status(INVALID_EXPR, "Filter expression is invalid.");
        }

        ExprNodePtr child_node = nodeStack.top();
        nodeStack.pop();
        if (child_node->value_type != ValueType::BOOL) {
          return Status(INVALID_EXPR, "NOT statement is invalid.");
        }

        node->node_type = NodeType::NOT;
        node->value_type = ValueType::BOOL;
        node_list.push_back(child_node);
        node->left = node_list.size() - 1;
        node->right = -1;

        nodeStack.push(node);
      } else {
        if (nodeStack.size() < 2) {
          return Status(INVALID_EXPR, "Filter expression is invalid.");
        }

        node->node_type = GetOperatorNodeType(token);
        ExprNodePtr right_node = nodeStack.top();
        nodeStack.pop();
        ExprNodePtr left_node = nodeStack.top();
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
      node->left = -1;
      node->right = -1;
      if (isBoolConstant(token)) {
        node->node_type = NodeType::BoolConst;
        node->value_type = ValueType::BOOL;
        node->bool_value = to_bool(token);
        std::cout << node->bool_value << std::endl;
      } else if (token[0] == '\'') {
        node->node_type = NodeType::StringConst;
        node->value_type = ValueType::STRING;
        node->str_value = token.substr(1, token.size() - 2);
        std::cout << node->str_value << std::endl;
      } else if (isIntConstant(token)) {
        node->node_type = NodeType::IntConst;
        node->value_type = ValueType::INT;
        node->int_value = std::stoi(token);
        std::cout << node->int_value << std::endl;
      } else if (isDoubleConstant(token)) {
        node->node_type = NodeType::DoubleConst;
        node->value_type = ValueType::DOUBLE;
        node->double_value = std::stod(token);
        std::cout << node->double_value << std::endl;
      } else {
        // TODO: attribute node validating with table schema
        // node->node_type = NodeType::StringAttr;
        // node->value_type = ValueType::STRING;
        if (field_map.find(token) == field_map.end()) {
          return Status(INVALID_EXPR, "Invalid filter expression: field name '" + token + "' not found.");
        }
        node->field_name = token;
        engine::meta::FieldType field_type = field_map[token];
        switch (field_type) {
          case engine::meta::FieldType::INT1:
          case engine::meta::FieldType::INT2:
          case engine::meta::FieldType::INT4:
          case engine::meta::FieldType::INT8:
            node->node_type = NodeType::IntAttr;
            node->value_type = ValueType::INT;
            break;
          case engine::meta::FieldType::DOUBLE:
          case engine::meta::FieldType::FLOAT:
            node->node_type = NodeType::DoubleAttr;
            node->value_type = ValueType::DOUBLE;
            break;
          case engine::meta::FieldType::BOOL:
            node->node_type = NodeType::BoolAttr;
            node->value_type = ValueType::BOOL;
            break;
          case engine::meta::FieldType::STRING:
            node->node_type = NodeType::StringAttr;
            node->value_type = ValueType::STRING;
            break;
          default:
            return Status(INVALID_EXPR, "Type of field '" + token + "' is not supported in filter expression.");
        }
      }

      nodeStack.push(node);
    }
  }

  if (nodeStack.size() != 1) {
    return Status(INVALID_EXPR, "Filter expression is invalid.");
  }

  node_list.push_back(nodeStack.top());
  nodeStack.pop();

  if (node_list.back()->value_type != ValueType::BOOL) {
    return Status(INVALID_EXPR, "Filter should be a boolean expression,");
  }

  nodes = node_list;
  return Status::OK();
};

Status Expr::ParseNodeFromStr(
  std::string expression,
  std::vector<ExprNodePtr>& nodes,
  std::unordered_map<std::string, engine::meta::FieldType>& field_map
) {
  // Skip if expression is empty.
  if (expression == "") {
    return Status::OK();
  }

  // Parse string into token arr
  std::vector<std::string> token_list;
  Status parsing_status = SplitTokens(expression, token_list);
  if (!parsing_status.ok()) {
    std::cout << parsing_status.message() << std::endl;
    return parsing_status;
  }

  std::cout << expression + " transfer to: " + boost::algorithm::join(token_list, ", ") << std::endl;

  std::vector<std::string> tokens_queue;
  tokens_queue = ShuntingYard(token_list);
  std::cout << "After SY: " + boost::algorithm::join(tokens_queue, ", ") << std::endl;

  Status nodes_status = GenerateNodes(tokens_queue, nodes, field_map);
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
  json.SetInt("leftChild", node->left);
  json.SetInt("rightChild", node->right);
  switch (node->value_type) {
    case ValueType::BOOL:
      json.SetBool("value", node->bool_value);
      break;
    case ValueType::DOUBLE:
      json.SetDouble("value", node->double_value);
      break;
    case ValueType::INT:
      json.SetInt("value", node->int_value);
    case ValueType::STRING:
      json.SetString("value", node->str_value);
  }

  return Status::OK();
};

} // namespace expr
} // namespace query
} // namespace vectordb