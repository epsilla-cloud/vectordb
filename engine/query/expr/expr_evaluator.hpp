#pragma once

#include <any>
#include <unordered_map>
#include <vector>

#include "db/vector.hpp"
#include "expr_types.hpp"
#include "db/index/spatial/geoindex.hpp"

namespace vectordb {
namespace query {
namespace expr {

class ExprEvaluator {
 public:
  explicit ExprEvaluator(
      std::vector<ExprNodePtr>& nodes,
      std::unordered_map<std::string, size_t>& field_name_mem_offset_map,
      int64_t& primitive_offset_,
      int64_t& var_len_attr_num_,
      char* attribute_table_,
      std::vector<engine::VariableLenAttrColumnContainer>& var_len_attr_table_);

  ~ExprEvaluator();

  bool LogicalEvaluate(const int& node_index, const int64_t& cand_ind);
  bool LogicalEvaluate(const int& node_index, const int64_t& cand_ind, const double distance);

  // Check if the geo index can be uplifted to top level for optimization.
  int64_t UpliftingGeoIndex(const std::string& field_name, const int& node_index);
  double NumEvaluate(const int& node_index, const int64_t& cand_ind, const double distance);
  std::string StrEvaluate(const int& node_index, const int64_t& cand_ind);

 private:
  std::string GetStrFieldValue(const std::string& field_name, const int64_t& cand_ind);
  bool GetBoolFieldValue(const std::string& field_name, const int64_t& cand_ind);
  int64_t GetIntFieldValue(const std::string& field_name, const int64_t& cand_ind, NodeType& node_type);
  double GetRealNumberFieldValue(const std::string& field_name, const int64_t& cand_ind, NodeType& node_type);
  vectordb::engine::index::GeospatialIndex::point_t GeoPointEvaluate(const std::string& field_name, const int64_t& cand_ind);

 public:
  std::vector<ExprNodePtr>& nodes_;
  std::unordered_map<std::string, size_t>& field_name_mem_offset_map_;
  int64_t primitive_offset_;
  int64_t var_len_attr_num_;
  char* attribute_table_;
  std::vector<engine::VariableLenAttrColumnContainer>& var_len_attr_table_;
};

}  // namespace expr
}  // namespace query
}  // namespace vectordb
