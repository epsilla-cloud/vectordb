#include "db/table_mvp.hpp"

#include <numeric>

#include "db/catalog/meta_types.hpp"

namespace vectordb {
namespace engine {

constexpr const int IntraQueryThreads = 4;
constexpr const int MasterQueueSize = 500;
constexpr const int LocalQueueSize = 500;
constexpr const int GlobalSyncInterval = 15;
constexpr const int MinimalGraphSize = 100;

TableMVP::TableMVP(meta::TableSchema& table_schema, const std::string& db_catalog_path)
    : table_schema_(table_schema),
      // executors_num_(executors_num),
      table_segment_(nullptr) {
  // Construct field name to field type map.
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    field_name_type_map_[table_schema_.fields_[i].name_] = table_schema_.fields_[i].field_type_;
  }

  // Load the table data from disk.
  table_segment_ = std::make_shared<TableSegmentMVP>(table_schema, db_catalog_path);

  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    if (table_schema_.fields_[i].field_type_ == meta::FieldType::VECTOR_FLOAT ||
        table_schema_.fields_[i].field_type_ == meta::FieldType::VECTOR_DOUBLE) {
      // Load the ann graph from disk.
      ann_graph_segment_.push_back(std::make_shared<vectordb::engine::ANNGraphSegment>(
          db_catalog_path,
          table_schema_.id_,
          table_schema_.fields_[i].id_));
      // Construct the executor.
      l2space_.push_back(std::make_shared<vectordb::L2Space>(table_schema_.fields_[i].vector_dimension_));
      executor_.push_back(std::make_shared<execution::VecSearchExecutor>(
          ann_graph_segment_->record_number_,
          table_schema_.fields_[i].vector_dimension_,
          ann_graph_segment_.back()->navigation_point_,
          ann_graph_segment_.back()->offset_table_,
          ann_graph_segment_.back()->neighbor_list_,
          table_segment_->vector_tables_[table_segment_->field_name_mem_offset_map_[table_schema_.fields_[i].name_]],
          l2space_.back()->get_dist_func(),
          l2space_.back()->get_dist_func_param(),
          IntraQueryThreads,
          MasterQueueSize,
          LocalQueueSize,
          GlobalSyncInterval));
    }
  }
}

Status TableMVP::Rebuild(const std::string& db_catalog_path) {
  // Get the current record number.
  int64_t record_number = table_segment_->record_number_;

  // Write the table data to disk.
  std::cout << "Save table segment." << std::endl;
  table_segment_->SaveTableSegment(table_schema_, db_catalog_path);

  int64_t index = 0;
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    if (table_schema_.fields_[i].field_type_ == meta::FieldType::VECTOR_FLOAT ||
        table_schema_.fields_[i].field_type_ == meta::FieldType::VECTOR_DOUBLE) {
      if (ann_graph_segment_[index]->record_number_ == record_number || record_number < MinimalGraphSize) {
        // No need to rebuild the ann graph.
        std::cout << "Skip rebuild ANN graph for attribute: " << table_schema_.fields_[i].name_ << std::endl;
        ++index;
        continue;
      }
      // Rebuild the ann graph.
      std::cout << "Rebuild ANN graph for attribute: " << table_schema_.fields_[i].name_ << std::endl;
      auto new_ann = std::make_shared<vectordb::engine::ANNGraphSegment>(
          db_catalog_path,
          table_schema_.id_,
          table_schema_.fields_[i].id_);
      new_ann->BuildFromVectorTable(
          table_segment_->vector_tables_[table_segment_->field_name_mem_offset_map_[table_schema_.fields_[i].name_]],
          record_number,
          table_schema_.fields_[i].vector_dimension_);
      std::shared_ptr<vectordb::engine::ANNGraphSegment> ann_ptr = ann_graph_segment_[index];
      ann_graph_segment_[index] = new_ann;
      // Write the ANN graph to disk.
      std::cout << "Save ANN graph segment." << std::endl;
      ann_graph_segment_[index]->SaveANNGraph(db_catalog_path, table_schema_.id_, table_schema_.fields_[i].id_);

      // Replace the executor.
      executor_[index] = std::make_shared<execution::VecSearchExecutor>(
          table_segment_->record_number_,
          table_schema_.fields_[i].vector_dimension_,
          ann_graph_segment_[index]->navigation_point_,
          ann_graph_segment_[index]->offset_table_,
          ann_graph_segment_[index]->neighbor_list_,
          table_segment_->vector_tables_[table_segment_->field_name_mem_offset_map_[table_schema_.fields_[i].name_]],
          l2space_[index]->get_dist_func(),
          l2space_[index]->get_dist_func_param(),
          IntraQueryThreads,
          MasterQueueSize,
          LocalQueueSize,
          GlobalSyncInterval);

      ++index;
    }
  }

  std::cout << "Rebuild done." << std::endl;
  return Status::OK();
}

Status TableMVP::Insert(vectordb::Json& record) {
  return table_segment_->Insert(table_schema_, record);
}

Status TableMVP::Search(
    const std::string& field_name,
    std::vector<std::string>& query_fields,
    const float* query_data,
    const int64_t K,
    vectordb::Json& result) {
  // Check if field_name exists.
  if (field_name_type_map_.find(field_name) == field_name_type_map_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "Field name not found: " + field_name);
  }
  for (auto field : query_fields) {
    if (field_name_type_map_.find(field) == field_name_type_map_.end()) {
      return Status(DB_UNEXPECTED_ERROR, "Field name not found: " + field);
    }
  }
  // Get the field data type. If the type is not VECTOR, return error.
  meta::FieldType field_type = field_name_type_map_[field_name];
  if (field_type != meta::FieldType::VECTOR_FLOAT && field_type != meta::FieldType::VECTOR_DOUBLE) {
    return Status(DB_UNEXPECTED_ERROR, "Field type is not vector.");
  }

  // Get the field offset in the vector table.
  int64_t field_offset = table_segment_->field_name_mem_offset_map_[field_name];
  // Get the executor.
  std::shared_ptr<execution::VecSearchExecutor> executor = executor_[field_offset];
  // Search.
  int64_t result_num = 0;
  executor->Search(query_data, K, table_segment_->record_number_, result_num);
  // Construct the result.
  result.LoadFromString("[]");
  // If query fields is empty, fill in with all fields.
  if (query_fields.size() == 0) {
    for (int i = 0; i < table_schema_.fields_.size(); ++i) {
      query_fields.push_back(table_schema_.fields_[i].name_);
    }
  }
  for (auto i = 0; i < result_num; ++i) {
    int64_t id = executor->search_result_[i];
    vectordb::Json record;
    record.LoadFromString("{}");
    for (auto field : query_fields) {
      if (field_name_type_map_[field] == meta::FieldType::VECTOR_FLOAT ||
          field_name_type_map_[field] == meta::FieldType::VECTOR_DOUBLE) {
        // Vector field.
        vectordb::Json vector;
        vector.LoadFromString("[]");
        int64_t offset = table_segment_->field_name_mem_offset_map_[field];
        int64_t dim = table_segment_->vector_dims_[offset];
        for (int k = 0; k < dim; ++k) {
          vector.AddDoubleToArray(table_segment_->vector_tables_[offset][id * dim + k]);
        }
        record.SetObject(field, vector);
      } else {
        // Primitive field.
        auto offset = table_segment_->field_name_mem_offset_map_[field] + id * table_segment_->primitive_offset_;
        switch (field_name_type_map_[field]) {
          case meta::FieldType::INT1: {
            int8_t* ptr = reinterpret_cast<int8_t*>(&table_segment_->attribute_table_[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT2: {
            int16_t* ptr = reinterpret_cast<int16_t*>(&table_segment_->attribute_table_[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT4: {
            int32_t* ptr = reinterpret_cast<int32_t*>(&table_segment_->attribute_table_[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT8: {
            int64_t* ptr = reinterpret_cast<int64_t*>(&table_segment_->attribute_table_[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::FLOAT: {
            float* ptr = reinterpret_cast<float*>(&table_segment_->attribute_table_[offset]);
            record.SetDouble(field, (double)(*ptr));
            break;
          }
          case meta::FieldType::DOUBLE: {
            double* ptr = reinterpret_cast<double*>(&table_segment_->attribute_table_[offset]);
            record.SetDouble(field, *ptr);
            break;
          }
          case meta::FieldType::BOOL: {
            bool* ptr = reinterpret_cast<bool*>(&table_segment_->attribute_table_[offset]);
            record.SetBool(field, *ptr);
            break;
          }
          case meta::FieldType::STRING: {
            auto string_offset = table_segment_->field_name_mem_offset_map_[field] + id * table_segment_->string_num_;
            record.SetString(field, table_segment_->string_table_[string_offset]);
            break;
          }
          default:
            return Status(DB_UNEXPECTED_ERROR, "Unknown field type.");
        }
      }
    }
    result.AddObjectToArray(record);
  }
  return Status::OK();
}

TableMVP::~TableMVP() {
}

}  // namespace engine
}  // namespace vectordb
