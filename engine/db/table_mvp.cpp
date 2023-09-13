#include "db/table_mvp.hpp"

#include <omp.h>

#include <numeric>

#include "db/catalog/meta_types.hpp"

namespace vectordb {
namespace engine {

TableMVP::TableMVP(meta::TableSchema &table_schema,
                   const std::string &db_catalog_path, int64_t init_table_scale)
    : table_schema_(table_schema),
      // executors_num_(executors_num),
      table_segment_(nullptr) {
  db_catalog_path_ = db_catalog_path;
  // Construct field name to field type map.
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    field_name_type_map_[table_schema_.fields_[i].name_] =
        table_schema_.fields_[i].field_type_;
  }

  // Load the table data from disk.
  table_segment_ = std::make_shared<TableSegmentMVP>(
      table_schema, db_catalog_path, init_table_scale);

  // Replay operations in write ahead log.
  wal_ = std::make_shared<WriteAheadLog>(db_catalog_path_, table_schema.id_);
  wal_->Replay(table_schema, table_segment_);

  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    if (table_schema_.fields_[i].field_type_ == meta::FieldType::VECTOR_FLOAT ||
        table_schema_.fields_[i].field_type_ ==
            meta::FieldType::VECTOR_DOUBLE) {
      // Load the ann graph from disk.
      ann_graph_segment_.push_back(
          std::make_shared<vectordb::engine::ANNGraphSegment>(
              db_catalog_path, table_schema_.id_,
              table_schema_.fields_[i].id_));
      // Construct the executor.
      l2space_.push_back(std::make_shared<vectordb::L2Space>(
          table_schema_.fields_[i].vector_dimension_));

      auto pool = std::make_shared<execution::ExecutorPool>();
      for (int executorIdx = 0; executorIdx < NumExecutorPerField;
           executorIdx++) {
        pool->release(std::make_shared<execution::VecSearchExecutor>(
            ann_graph_segment_.back()->record_number_,
            table_schema_.fields_[i].vector_dimension_,
            ann_graph_segment_.back()->navigation_point_,
            ann_graph_segment_.back(), ann_graph_segment_.back()->offset_table_,
            ann_graph_segment_.back()->neighbor_list_,
            table_segment_
                ->vector_tables_[table_segment_->field_name_mem_offset_map_
                                     [table_schema_.fields_[i].name_]],
            l2space_.back()->get_dist_func(),
            l2space_.back()->get_dist_func_param(), IntraQueryThreads,
            MasterQueueSize, LocalQueueSize, GlobalSyncInterval));
      }
      executor_pool_.push_back(pool);
    }
  }
}

Status TableMVP::Rebuild(const std::string &db_catalog_path) {
  // Limit how many threads rebuild takes.
  omp_set_num_threads(RebuildThreads);

  // Get the current record number.
  int64_t record_number = table_segment_->record_number_;

  // Write the table data to disk.
  std::cout << "Save table segment." << std::endl;
  table_segment_->SaveTableSegment(table_schema_, db_catalog_path);

  // Clean up old WAL files.
  wal_->CleanUpOldFiles();

  int64_t index = 0;
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    if (table_schema_.fields_[i].field_type_ == meta::FieldType::VECTOR_FLOAT ||
        table_schema_.fields_[i].field_type_ ==
            meta::FieldType::VECTOR_DOUBLE) {
      if (ann_graph_segment_[index]->record_number_ == record_number ||
          record_number < MinimalGraphSize) {
        // No need to rebuild the ann graph.
        std::cout << "Skip rebuild ANN graph for attribute: "
                  << table_schema_.fields_[i].name_ << std::endl;
        ++index;
        continue;
      }
      // Rebuild the ann graph.
      std::cout << "Rebuild ANN graph for attribute: "
                << table_schema_.fields_[i].name_ << std::endl;
      auto new_ann = std::make_shared<vectordb::engine::ANNGraphSegment>(
          db_catalog_path, table_schema_.id_, table_schema_.fields_[i].id_);
      new_ann->BuildFromVectorTable(
          table_segment_
              ->vector_tables_[table_segment_->field_name_mem_offset_map_
                                   [table_schema_.fields_[i].name_]],
          record_number, table_schema_.fields_[i].vector_dimension_);
      std::shared_ptr<vectordb::engine::ANNGraphSegment> ann_ptr =
          ann_graph_segment_[index];
      ann_graph_segment_[index] = new_ann;
      // Write the ANN graph to disk.
      std::cout << "Save ANN graph segment." << std::endl;
      ann_graph_segment_[index]->SaveANNGraph(
          db_catalog_path, table_schema_.id_, table_schema_.fields_[i].id_);

      // Replace the executors.
      auto pool = std::make_shared<execution::ExecutorPool>();

      for (int executorIdx = 0; executorIdx < NumExecutorPerField;
           executorIdx++) {
        pool->release(std::make_shared<execution::VecSearchExecutor>(
            table_segment_->record_number_,
            table_schema_.fields_[i].vector_dimension_,
            ann_graph_segment_[index]->navigation_point_,
            ann_graph_segment_[index],
            ann_graph_segment_[index]->offset_table_,
            ann_graph_segment_[index]->neighbor_list_,
            table_segment_
                ->vector_tables_[table_segment_->field_name_mem_offset_map_
                                     [table_schema_.fields_[i].name_]],
            l2space_[index]->get_dist_func(),
            l2space_[index]->get_dist_func_param(),
            IntraQueryThreads,
            MasterQueueSize,
            LocalQueueSize,
            GlobalSyncInterval));
      }
      std::unique_lock<std::mutex> lock(executor_pool_mutex_);
      executor_pool_.set(index, pool);
      lock.unlock();

      ++index;
    }
  }

  std::cout << "Rebuild done." << std::endl;
  return Status::OK();
}

Status TableMVP::Insert(vectordb::Json &record) {
  int64_t wal_id =
      wal_->WriteEntry(LogEntryType::INSERT, record.DumpToString());
  return table_segment_->Insert(table_schema_, record, wal_id);
}

Status TableMVP::DeleteByPK(vectordb::Json &records) {
  int64_t wal_id =
      wal_->WriteEntry(LogEntryType::DELETE, records.DumpToString());
  return table_segment_->DeleteByPK(records, wal_id);
}

Status TableMVP::Search(const std::string &field_name,
                        std::vector<std::string> &query_fields,
                        int64_t query_dimension, const float *query_data,
                        const int64_t limit, vectordb::Json &result,
                        std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
                        bool with_distance) {
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
  if (field_type != meta::FieldType::VECTOR_FLOAT &&
      field_type != meta::FieldType::VECTOR_DOUBLE) {
    return Status(DB_UNEXPECTED_ERROR, "Field type is not vector.");
  }

  // Get the field offset in the vector table.
  int64_t field_offset = table_segment_->field_name_mem_offset_map_[field_name];

  // [Note] the following invocation is wrong
  //   execution::RAIIVecSearchExecutor(executor_pool_[field_offset],
  //   executor_pool_[field_offset]->acquire);
  // because the value of executor_pool_[field_offset] could be different when
  // evaluating twice, which will result in memory leak or core dump
  std::unique_lock<std::mutex> lock(executor_pool_mutex_);
  auto pool = executor_pool_.at(field_offset);
  auto executor = execution::RAIIVecSearchExecutor(pool, pool->acquire());
  lock.unlock();

  // The query dimension needs to match the vector dimension.
  if (query_dimension != executor.exec_->dimension_) {
    return Status(DB_UNEXPECTED_ERROR,
                  "Query dimension doesn't match the vector field dimension.");
  }

  // Search.
  int64_t result_num = 0;
  executor.exec_->Search(query_data, table_segment_.get(), limit,
                         filter_nodes, result_num);
  result_num = result_num > limit ? limit : result_num;
  auto status =
      Project(query_fields, result_num, executor.exec_->search_result_, result,
              with_distance, executor.exec_->distance_);
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

Status TableMVP::Project(
    std::vector<std::string> &query_fields,
    int64_t idlist_size,        // -1 means project all.
    std::vector<int64_t> &ids,  // doesn't matter if idlist_size is -1.
    vectordb::Json &result, bool with_distance,
    std::vector<double> &distances) {
  // Construct the result.
  result.LoadFromString("[]");
  // If query fields is empty, fill in with all fields.
  if (query_fields.size() == 0) {
    for (int i = 0; i < table_schema_.fields_.size(); ++i) {
      query_fields.push_back(table_schema_.fields_[i].name_);
    }
  }

  // If idlist_size = -1, actually project everything from table.
  bool from_id_list = true;
  if (idlist_size == -1) {
    idlist_size = table_segment_->record_number_.load();
    from_id_list = false;
  }

  for (auto i = 0; i < idlist_size; ++i) {
    int64_t id = from_id_list ? ids[i] : i;
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
          vector.AddDoubleToArray(
              table_segment_->vector_tables_[offset][id * dim + k]);
        }
        record.SetObject(field, vector);
      } else {
        // Primitive field.
        auto offset = table_segment_->field_name_mem_offset_map_[field] +
                      id * table_segment_->primitive_offset_;
        switch (field_name_type_map_[field]) {
          case meta::FieldType::INT1: {
            int8_t *ptr = reinterpret_cast<int8_t *>(
                &table_segment_->attribute_table_[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT2: {
            int16_t *ptr = reinterpret_cast<int16_t *>(
                &table_segment_->attribute_table_[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT4: {
            int32_t *ptr = reinterpret_cast<int32_t *>(
                &table_segment_->attribute_table_[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT8: {
            int64_t *ptr = reinterpret_cast<int64_t *>(
                &table_segment_->attribute_table_[offset]);
            record.SetInt(field, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::FLOAT: {
            float *ptr = reinterpret_cast<float *>(
                &table_segment_->attribute_table_[offset]);
            record.SetDouble(field, (double)(*ptr));
            break;
          }
          case meta::FieldType::DOUBLE: {
            double *ptr = reinterpret_cast<double *>(
                &table_segment_->attribute_table_[offset]);
            record.SetDouble(field, *ptr);
            break;
          }
          case meta::FieldType::BOOL: {
            bool *ptr = reinterpret_cast<bool *>(
                &table_segment_->attribute_table_[offset]);
            record.SetBool(field, *ptr);
            break;
          }
          case meta::FieldType::STRING: {
            auto string_offset =
                table_segment_->field_name_mem_offset_map_[field] +
                id * table_segment_->string_num_;
            record.SetString(field,
                             table_segment_->string_table_[string_offset]);
            break;
          }
          case meta::FieldType::JSON: {
            auto json_offset =
                table_segment_->field_name_mem_offset_map_[field] +
                id * table_segment_->string_num_;
            vectordb::Json json;
            json.LoadFromString(table_segment_->string_table_[json_offset]);
            record.SetObject(field, json);
            break;
          }
          default:
            return Status(DB_UNEXPECTED_ERROR, "Unknown field type.");
        }
      }
    }
    // Add distance if needed.
    if (with_distance) {
      record.SetDouble("@distance", distances[i]);
    }
    result.AddObjectToArray(std::move(record));
  }
  return Status::OK();
}

TableMVP::~TableMVP() {}

}  // namespace engine
}  // namespace vectordb
