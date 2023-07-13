#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace vectordb {
namespace engine {
namespace meta {

constexpr int32_t DEFAULT_VECTOR_DIMENSION = 0;
constexpr const char* DEFAULT_MODEL_NAME = "sentence-transformers/paraphrase-albert-small-v2";

using DateT = int;
const DateT EmptyDate = -1;

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

enum class MetricType {
  EUCLIDEAN = 1,
  COSINE = 2,
  DOT_PRODUCT = 3,
};

struct FieldSchema {
  size_t id_ = 0;
  std::string name_;
  FieldType field_type_ = FieldType::INT4;
  int32_t vector_dimension_ = DEFAULT_VECTOR_DIMENSION;
  MetricType metric_type_ = MetricType::EUCLIDEAN;
};

struct AutoEmbedding {
  size_t src_field_id_ = 0;
  size_t tgt_field_id_ = 0;
  std::string model_name_ = DEFAULT_MODEL_NAME;
};

struct TableSchema {
  size_t id_ = 0;
  std::string name_;
  std::vector<FieldSchema> fields_;
  std::vector<AutoEmbedding> auto_embeddings_;
};

struct DatabaseSchema {
  size_t id_ = 0;
  std::string name_;
  std::vector<TableSchema> tables_;
};

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
