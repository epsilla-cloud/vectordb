#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace vectordb {
namespace engine {
namespace meta {

constexpr int32_t DEFAULT_VECTOR_DIMENSION = 0;

using DateT = int;
const DateT EmptyDate = -1;

struct DatabaseSchema {
  size_t id_ = 0;
  std::string name_;
};

struct TableSchema {
  size_t id_ = 0;
  std::string name_;
  size_t database_id_ = 0;
};

enum class FieldType {
  INT1 = 1,  // TINYINT
  INT2 = 2,  // SMALLINT
  INT4 = 3,  // INT
  INT8 = 4,  // BIGINT
  
  FLOAT = 10,
  DOUBLE = 11,

  STRING = 20,

  BOOL = 30,

  VECTOR_FLOAT = 40,
  VECTOR_DOUBLE = 41,

  UNKNOWN = 999,
};

struct FieldSchema {
  size_t id_ = 0;
  std::string name_;
  FieldType field_type_ = FieldType::INT4;
  int32_t vector_dimension_ = DEFAULT_VECTOR_DIMENSION;
  size_t table_id_ = 0;
};

using FieldSchemaPtr = std::shared_ptr<FieldSchema>;

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
