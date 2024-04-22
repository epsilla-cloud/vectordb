#include "expr.hpp"
#include <iostream>
#include <algorithm>
#include <queue>
#include <regex>
#include <stack>
#include <unordered_map>
#include <vector>

#include "logger/logger.hpp"

namespace vectordb {
namespace query {
namespace expr {

enum class State {
  Start,
  Number,
  String,
  Attribute,
  Operator,
  Function,
  InList,
  InListString,
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

bool isLike(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
    return std::toupper(c);
  });
  return str == "LIKE";
};

bool isIn(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
    return std::toupper(c);
  });
  return str == "IN";
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
  return str == "ALL" || str == "ANY" || str == "BETWEEN" || str == "EXISTS" || str == "SOME";
}

bool isOperator(std::string str) {
  return isArithStr(str) || isCompareStr(str) || isLogicalStr(str) || isLike(str) || isIn(str);
};

int getPrecedence(std::string& op) {
  if (isLogicalStr(op)) {
    return 1;
  } else if (isCompareStr(op)) {
    return 2;
  } else if (isLike(op) || isIn(op)) {
    return 3;
  } else if (op == "+" || op == "-") {
    return 4;
  } else if (op == "*" || op == "/" || op == "%") {
    return 5;
  }
  return 0;
};

Status SplitTokens(std::string& expression, std::vector<std::string>& tokens) {
  std::vector<std::string> token_list;
  State state = State::Start;
  std::string cur_token;

  size_t last_index = expression.length() - 1;
  for (size_t i = 0; i < expression.length();) {
    char c = expression[i];
    std::cout << i << " " << c << std::endl;
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
        } else if (c == '@') {
          if (i + 8 <= last_index && expression.substr(i, 9) == "@distance") {
            state = State::Attribute;
            cur_token = "@distance";
            i += 9;
          } else {
            return Status(INVALID_EXPR, "Filter expression is not valid.");
          }
        } else {
          return Status(INVALID_EXPR, "Filter expression is not valid.");
        }
        break;
      case State::String:
      case State::InListString:
        if (c == '\'') {
          // check if last character of cur_token is '\', pop the '\' and add the '\'' to cur_token
          if (i != last_index && cur_token.size() > 0 && cur_token[cur_token.size() - 1] == '\\') {
            cur_token.pop_back();
            cur_token += c;
            i++;
          } else {
            i++;
            cur_token += c;
            if (cur_token.size() >= 2) {
              token_list.push_back(cur_token);
              cur_token.clear();
              if (state == State::InListString) {
                state = State::InList;
              } else {
                std::cout << "Here?? " << std::endl;
                state = State::Start;
              }
            }
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
          if (isLike(cur_token)) {
            token_list.push_back("LIKE");
          } else {
            token_list.push_back(cur_token);
          }
          cur_token.clear();
          state = State::Start;
        } else if (std::isalnum(c) || c == '_') {
          cur_token += c;
          i++;
        } else if (c == '(') {
          if (isIn(cur_token)) {
            state = State::InList;
            token_list.push_back("IN");
            token_list.push_back("(");
            cur_token.clear();
            i++;
          } else {
            // Function call
            // TODO: handle string with '(' and ')'
            state = State::Function;
            cur_token += c;
            i++;
            int parenCount = 1;
            while (i < expression.length() && parenCount > 0) {
              c = expression[i];
              if (c == '(') parenCount++;
              if (c == ')') parenCount--;
              cur_token += c;
              i++;
            }
            token_list.push_back(cur_token);
            cur_token.clear();
            state = State::Start;
          }
        } else {
          return Status(INVALID_EXPR, "Invalid name: " + (cur_token += c));
        }
        break;
      case State::InList:
        if (c == '\'') {
          state = State::InListString;
          cur_token = "'";
          i++;
        } else if (c == ')') {
          token_list.push_back(")");
          i++;
          state = State::Start;
        } else if (std::isspace(c) || c == ',') {
          i++;
        } else {
          std::cout << "here " << c << std::endl;
          return Status(INVALID_EXPR, "Filter expression is not valid.");
        }
        break;
      case State::Number:
        if (std::isspace(c) || c == ')' || isArithChar(c) || isCompareChar(c)) {
          if (std::count(cur_token.begin(), cur_token.end(), '.') > 1) {
            return Status(INVALID_EXPR, cur_token + " is not a valid number.");
          } else {
            token_list.push_back(cur_token);
            cur_token.clear();
            if (std::isspace(c)) {
              i++;
            }
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

  tokens = token_list;
  return Status::OK();
};

std::vector<std::string> ShuntingYard(const std::vector<std::string>& tokens) {
  std::vector<std::string> res;
  std::stack<std::string> operator_stack;

  for (std::string str : tokens) {
    if (str == "(") {
      operator_stack.push(str);
    } else if (str == ")") {
      while (!operator_stack.empty() && operator_stack.top() != "(") {
        res.push_back(operator_stack.top());
        operator_stack.pop();
      }
      operator_stack.pop();  // Pop the '('
    } else if (isOperator(str)) {
      while (!operator_stack.empty() && getPrecedence(operator_stack.top()) >= getPrecedence(str)) {
        res.push_back(operator_stack.top());
        operator_stack.pop();
      }
      operator_stack.push(str);
    } else {
      res.push_back(str);
    }
  }

  while (!operator_stack.empty()) {
    res.push_back(operator_stack.top());
    operator_stack.pop();
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

bool isDistance(const std::string& str) {
  return str == "@distance";
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

bool isNearbyFunction(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) {
    return std::toupper(c);
  });
  return str.substr(0, 7) == "NEARBY(";
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
          right == ValueType::STRING || right == ValueType::BOOL) {
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
  if (isLike(op)) {
    if (left != ValueType::STRING || right != ValueType::STRING) {
      return Status(INVALID_EXPR, "LIKE statement is invalid.");
    }
    root = ValueType::BOOL;
  }

  return Status::OK();
};

Status GenerateNodes(
    std::vector<std::string>& tokens,
    std::vector<ExprNodePtr>& nodes,
    std::unordered_map<std::string, engine::meta::FieldType>& field_map,
    bool check_bool) {
  std::stack<ExprNodePtr> node_stack;
  std::vector<ExprNodePtr> node_list;

  // FIXME: remove
  for (std::string token : tokens) {
    std::cout << token << std::endl;
  }
  for (std::string token : tokens) {
    if (isUnsupportedLogicalOp(token)) {
      return Status(NOT_IMPLEMENTED_ERROR, "Epsilla does not support " + token + " yet.");
    } else if (isOperator(token)) {
      ExprNodePtr node = std::make_shared<ExprNode>();
      if (isNotOperator(token)) {
        if (node_stack.empty()) {
          return Status(INVALID_EXPR, "Filter expression is invalid.");
        }

        ExprNodePtr child_node = node_stack.top();
        node_stack.pop();
        if (child_node->value_type != ValueType::BOOL) {
          return Status(INVALID_EXPR, "NOT statement is invalid.");
        }

        node->node_type = NodeType::NOT;
        node->value_type = ValueType::BOOL;
        node_list.push_back(child_node);
        node->left = node_list.size() - 1;
        node->right = -1;

        node_stack.push(node);
      } else if (isIn(token)) {
        if (node_stack.size() < 2) {
          return Status(INVALID_EXPR, "Not enough operands for IN operator.");
        }

        ExprNodePtr list_node = std::make_shared<ExprNode>();
        list_node->node_type = NodeType::ListString;
        list_node->value_type = ValueType::LIST_STRING;

        while (node_stack.top()->node_type != NodeType::StringAttr) {
          ExprNodePtr element_node = node_stack.top();
          node_stack.pop();
          list_node->arguments.push_back(node_list.size());
          node_list.push_back(element_node);
        }

        // std::reverse(list_node->arguments.begin(), list_node->arguments.end());  // Correct the order

        ExprNodePtr attr_node = node_stack.top();
        node_stack.pop();
        if (attr_node->value_type != ValueType::STRING) {
          return Status(INVALID_EXPR, "IN operation is only supported for string attributes.");
        }

        ExprNodePtr in_node = std::make_shared<ExprNode>();
        in_node->node_type = NodeType::IN;
        in_node->value_type = ValueType::BOOL;
        in_node->arguments.push_back(node_list.size()); // Attribute index
        node_list.push_back(attr_node);
        in_node->arguments.push_back(node_list.size()); // List index
        node_list.push_back(list_node);

        node_stack.push(in_node);
      } else {
        if (node_stack.size() < 2) {
          return Status(INVALID_EXPR, "Filter expression is invalid.");
        }

        node->node_type = GetOperatorNodeType(token);
        ExprNodePtr right_node = node_stack.top();
        node_stack.pop();
        ExprNodePtr left_node = node_stack.top();
        node_stack.pop();

        Status compability_status = CheckCompatible(token, left_node->value_type, right_node->value_type, node->value_type);
        if (!compability_status.ok()) {
          return compability_status;
        }
        node_list.push_back(left_node);
        node_list.push_back(right_node);
        node->left = node_list.size() - 2;
        node->right = node_list.size() - 1;

        node_stack.push(node);
      }
    } else if (isNearbyFunction(token)) {
      // NEARBY(geo_point_attr, latitude, longitude, distance)
      // Extract the argument string inside the parentheses of "NEARBY(...)"
      std::string argsStr = token.substr(7, token.length() - 8); // Remove "NEARBY(" and the closing ")"

      // Split the arguments by commas
      std::vector<std::string> arguments;
      std::stringstream ss(argsStr);
      std::string arg;
      while (std::getline(ss, arg, ',')) {
        // Trim spaces from the beginning and end of arg
        arg.erase(0, arg.find_first_not_of(' ')); // Leading spaces
        arg.erase(arg.find_last_not_of(' ') + 1); // Trailing spaces
        arguments.push_back(arg);
      }

      if (arguments.size() == 4) {
        // Successfully extracted 4 arguments
        std::string geo_point_attr = arguments[0];
        std::string latitude = arguments[1];
        std::string longitude = arguments[2];
        std::string distance = arguments[3];

        // Validate the arguments
        if (field_map.find(geo_point_attr) == field_map.end()) {
          return Status(INVALID_EXPR, "Invalid filter expression: field name '" + geo_point_attr + "' not found.");
        }
        engine::meta::FieldType field_type = field_map[geo_point_attr];
        if (field_type != engine::meta::FieldType::GEO_POINT) {
          return Status(INVALID_EXPR, "Type of field '" + geo_point_attr + "' is not a GEO_POINT.");
        }
        for (int idx = 1; idx < 4; ++idx) {
          if (!isDoubleConstant(arguments[idx]) && !isIntConstant(arguments[idx])) {
            return Status(INVALID_EXPR, "Invalid filter expression: argument " + std::to_string(idx + 1) + " is not a valid number.");
          }
        }

        // Create a new ExprNode for the NEARBY function
        ExprNodePtr attr_node = std::make_shared<ExprNode>();
        attr_node->field_name = geo_point_attr;
        attr_node->node_type = NodeType::GeoPointAttr;
        attr_node->value_type = ValueType::GEO_POINT;
        node_list.push_back(attr_node);
        ExprNodePtr lat_node = std::make_shared<ExprNode>();
        lat_node->node_type = NodeType::DoubleConst;
        lat_node->value_type = ValueType::DOUBLE;
        lat_node->double_value = std::stod(latitude);
        if (lat_node->double_value < -90 || lat_node->double_value > 90) {
          return Status(INVALID_EXPR, "Invalid filter expression: latitude should be in the range of [-90, 90].");
        }
        node_list.push_back(lat_node);
        ExprNodePtr lon_node = std::make_shared<ExprNode>();
        lon_node->node_type = NodeType::DoubleConst;
        lon_node->value_type = ValueType::DOUBLE;
        lon_node->double_value = std::stod(longitude);
        node_list.push_back(lon_node);
        if (lon_node->double_value < -180 || lon_node->double_value > 180) {
          return Status(INVALID_EXPR, "Invalid filter expression: longitude should be in the range of [-180, 180].");
        }
        ExprNodePtr dist_node = std::make_shared<ExprNode>();
        dist_node->node_type = NodeType::DoubleConst;
        dist_node->value_type = ValueType::DOUBLE;
        dist_node->double_value = std::stod(distance);
        node_list.push_back(dist_node);
        ExprNodePtr node = std::make_shared<ExprNode>();
        node->node_type = NodeType::FunctionCall;
        node->function_name = "NEARBY";
        node->value_type = ValueType::BOOL;
        node->arguments.push_back(node_list.size() - 4);
        node->arguments.push_back(node_list.size() - 3);
        node->arguments.push_back(node_list.size() - 2);
        node->arguments.push_back(node_list.size() - 1);
        node_list.push_back(node);
        node_stack.push(node);
      } else {
        return Status(INVALID_EXPR, "Wrong number of arguments in NEARBY function.");
      }
    } else {
      ExprNodePtr node = std::make_shared<ExprNode>();
      node->left = -1;
      node->right = -1;
      if (isBoolConstant(token)) {
        node->node_type = NodeType::BoolConst;
        node->value_type = ValueType::BOOL;
        node->bool_value = to_bool(token);
      } else if (token[0] == '\'') {
        node->node_type = NodeType::StringConst;
        node->value_type = ValueType::STRING;
        node->str_value = token.substr(1, token.size() - 2);
      } else if (isIntConstant(token)) {
        node->node_type = NodeType::IntConst;
        node->value_type = ValueType::INT;
        node->int_value = std::stoi(token);
      } else if (isDoubleConstant(token)) {
        node->node_type = NodeType::DoubleConst;
        node->value_type = ValueType::DOUBLE;
        node->double_value = std::stod(token);
      } else if (isDistance(token)) {
        node->field_name = token;
        node->node_type = NodeType::DoubleAttr;
        node->value_type = ValueType::DOUBLE;
      } else {
        if (field_map.find(token) == field_map.end()) {
          return Status(INVALID_EXPR, "Invalid filter expression: field name '" + token + "' not found.");
        }
        node->field_name = token;
        engine::meta::FieldType field_type = field_map[token];
        switch (field_type) {
          case engine::meta::FieldType::INT1:
            node->node_type = NodeType::Int1Attr;
            node->value_type = ValueType::INT;
            break;
          case engine::meta::FieldType::INT2:
            node->node_type = NodeType::Int2Attr;
            node->value_type = ValueType::INT;
            break;
          case engine::meta::FieldType::INT4:
            node->node_type = NodeType::Int4Attr;
            node->value_type = ValueType::INT;
            break;
          case engine::meta::FieldType::INT8:
            node->node_type = NodeType::Int8Attr;
            node->value_type = ValueType::INT;
            break;
          case engine::meta::FieldType::DOUBLE:
            node->node_type = NodeType::DoubleAttr;
            node->value_type = ValueType::DOUBLE;
            break;
          case engine::meta::FieldType::FLOAT:
            node->node_type = NodeType::FloatAttr;
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

      node_stack.push(node);
    }
  }

  if (node_stack.size() != 1) {
    return Status(INVALID_EXPR, "Filter expression is invalid.");
  }

  node_list.push_back(node_stack.top());
  node_stack.pop();

  if (check_bool && node_list.back()->value_type != ValueType::BOOL) {
    return Status(INVALID_EXPR, "Filter should be a boolean expression,");
  }

  nodes = node_list;
  return Status::OK();
};

Status Expr::ParseNodeFromStr(
    std::string expression,
    std::vector<ExprNodePtr>& nodes,
    std::unordered_map<std::string, vectordb::engine::meta::FieldType>& field_map,
    bool check_bool) {
  // Skip if expression is empty.
  if (expression == "") {
    return Status::OK();
  }

  vectordb::engine::Logger logger;

  // Parse string into token arr
  std::vector<std::string> token_list;
  Status parsing_status = SplitTokens(expression, token_list);
  if (!parsing_status.ok()) {
    logger.Error(parsing_status.message());
    return parsing_status;
  }

  std::vector<std::string> tokens_queue;
  tokens_queue = ShuntingYard(token_list);

  Status nodes_status = GenerateNodes(tokens_queue, nodes, field_map, check_bool);
  if (!nodes_status.ok()) {
    logger.Error(nodes_status.message());
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

}  // namespace expr
}  // namespace query
}  // namespace vectordb