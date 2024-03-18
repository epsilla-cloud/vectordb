#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace vectordb {
namespace engine {
namespace meta {

constexpr size_t DEFAULT_VECTOR_DIMENSION = 0;
constexpr const char* DEFAULT_MODEL_NAME = "BAAI/bge-small-en-v1.5";

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

  JSON = 31,

  VECTOR_FLOAT = 40,
  VECTOR_DOUBLE = 41,

  SPARSE_VECTOR_FLOAT = 50,
  SPARSE_VECTOR_DOUBLE = 51,

  SET = 60,
  LIST = 61,

  UNKNOWN = 999,
};

enum class MetricType {
  EUCLIDEAN = 1,
  COSINE = 2,
  DOT_PRODUCT = 3,
  UNKNOWN = 999,
};

struct FieldSchema {
  int64_t id_ = 0;
  std::string name_;
  bool is_primary_key_ = false;
  bool is_index_field_ = false;
  FieldType field_type_ = FieldType::INT4;
  size_t vector_dimension_ = DEFAULT_VECTOR_DIMENSION;
  MetricType metric_type_ = MetricType::EUCLIDEAN;
  FieldType element_type = FieldType::INT4;
};

struct AutoEmbedding {
  int64_t src_field_id_ = 0;
  int64_t tgt_field_id_ = 0;
  std::string model_name_ = DEFAULT_MODEL_NAME;
};

struct Index {
  std::string name_;
  std::string field_name_;
  std::string embedding_model_name_;
  int64_t src_field_id_ = 0;
  int64_t tgt_field_id_ = 0;
  int64_t dimensions = 0;
};

struct TableSchema {
  int64_t id_ = 0;
  std::string name_;
  std::vector<FieldSchema> fields_;
  std::vector<Index> indices_;
  std::vector<AutoEmbedding> auto_embeddings_;
};

struct DatabaseSchema {
  int64_t id_ = 0;
  std::string name_;
  std::string path_;  // path to the database catalog file
  std::vector<TableSchema> tables_;
};

static const std::unordered_map<std::string, FieldType> fieldTypeMap = {
    {"TINYINT", FieldType::INT1},
    {"SMALLINT", FieldType::INT2},
    {"INT", FieldType::INT4},
    {"BIGINT", FieldType::INT8},
    {"FLOAT", FieldType::FLOAT},
    {"DOUBLE", FieldType::DOUBLE},
    {"STRING", FieldType::STRING},
    {"BOOL", FieldType::BOOL},
    {"JSON", FieldType::JSON},
    {"VECTOR_FLOAT", FieldType::VECTOR_FLOAT},
    {"VECTOR_DOUBLE", FieldType::VECTOR_DOUBLE},
    {"SPARSE_VECTOR_FLOAT", FieldType::SPARSE_VECTOR_FLOAT},
    {"SPARSE_VECTOR_DOUBLE", FieldType::SPARSE_VECTOR_DOUBLE},
    {"SET", FieldType::SET},
    {"LIST", FieldType::LIST},
    {"UNKNOWN", FieldType::UNKNOWN}};

static const std::unordered_map<std::string, MetricType> metricTypeMap = {
    {"EUCLIDEAN", MetricType::EUCLIDEAN},
    {"COSINE", MetricType::COSINE},
    {"DOT_PRODUCT", MetricType::DOT_PRODUCT}};

}  // namespace meta
}  // namespace engine
}  // namespace vectordb
