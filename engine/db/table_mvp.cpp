#include "db/table_mvp.hpp"

#include <omp.h>

#include <numeric>

#include "db/catalog/meta_types.hpp"

#include "config/config.hpp"

namespace vectordb {

extern Config globalConfig;

namespace engine {

TableMVP::TableMVP(meta::TableSchema &table_schema,
                   const std::string &db_catalog_path,
                   int64_t init_table_scale,
                   bool is_leader,
                   std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service,
                   std::unordered_map<std::string, std::string> &headers)
    : table_schema_(table_schema),
      // executors_num_(executors_num),
      table_segment_(nullptr),
      is_leader_(is_leader) {
  embedding_service_ = embedding_service;
  db_catalog_path_ = db_catalog_path;
  // Construct field name to field type map.
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    field_name_field_type_map_[table_schema_.fields_[i].name_] =
        table_schema_.fields_[i].field_type_;
    field_name_metric_type_map_[table_schema_.fields_[i].name_] =
        table_schema_.fields_[i].metric_type_;
  }

  // Load the table data from disk.
  table_segment_ = std::make_shared<TableSegmentMVP>(
      table_schema, db_catalog_path, init_table_scale, embedding_service_);

  // Replay operations in write ahead log.
  wal_ = std::make_shared<WriteAheadLog>(db_catalog_path_, table_schema.id_, is_leader_);
  wal_->Replay(table_schema, field_name_field_type_map_, table_segment_, headers);

  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    auto fType = table_schema_.fields_[i].field_type_;
    auto mType = table_schema_.fields_[i].metric_type_;
    if (fType == meta::FieldType::VECTOR_FLOAT ||
        fType == meta::FieldType::VECTOR_DOUBLE ||
        fType == meta::FieldType::SPARSE_VECTOR_FLOAT ||
        fType == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
      VectorColumnData columnData;
      if (fType == meta::FieldType::VECTOR_FLOAT || fType == meta::FieldType::VECTOR_DOUBLE) {
        columnData = table_segment_
                         ->vector_tables_[table_segment_->field_name_mem_offset_map_
                                              [table_schema_.fields_[i].name_]];
      } else {
        // sparse vector
        columnData = &table_segment_
                          ->var_len_attr_table_[table_segment_->field_name_mem_offset_map_
                                                    [table_schema_.fields_[i].name_]];
      }
      // Load the ann graph from disk.
      ann_graph_segment_.push_back(
          std::make_shared<vectordb::engine::ANNGraphSegment>(
              db_catalog_path, table_schema_.id_,
              table_schema_.fields_[i].id_));

      // Construct the executor.
      auto distFunc = GetDistFunc(fType, mType);

      auto pool = std::make_shared<execution::ExecutorPool>();
      for (int executorIdx = 0; executorIdx < globalConfig.NumExecutorPerField;
           executorIdx++) {
        pool->release(std::make_shared<execution::VecSearchExecutor>(
            table_schema_.fields_[i].vector_dimension_,
            ann_graph_segment_.back()->navigation_point_,
            ann_graph_segment_.back(), ann_graph_segment_.back()->offset_table_,
            ann_graph_segment_.back()->neighbor_list_,
            columnData,
            distFunc,
            &table_schema_.fields_[i].vector_dimension_,
            globalConfig.IntraQueryThreads,
            globalConfig.MasterQueueSize,
            globalConfig.LocalQueueSize,
            globalConfig.GlobalSyncInterval));
      }
      executor_pool_.push_back(pool);
    }
  }
}

Status TableMVP::Rebuild(const std::string &db_catalog_path) {
  // Limit how many threads rebuild takes.
  omp_set_num_threads(globalConfig.RebuildThreads);
  std::cout << "Rebuild table segment with threads: " << globalConfig.RebuildThreads << std::endl;

  // Get the current record number.
  int64_t record_number = table_segment_->record_number_;

  // Only leader sync table to disk.
  if (is_leader_) {
    // Write the table data to disk.
    std::cout << "Save table segment." << std::endl;
    table_segment_->SaveTableSegment(table_schema_, db_catalog_path);

    // Clean up old WAL files.
    wal_->CleanUpOldFiles();
  }

  int64_t index = 0;
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    auto fType = table_schema_.fields_[i].field_type_;
    auto mType = table_schema_.fields_[i].metric_type_;

    if (fType == meta::FieldType::VECTOR_FLOAT ||
        fType == meta::FieldType::VECTOR_DOUBLE ||
        fType == meta::FieldType::SPARSE_VECTOR_FLOAT ||
        fType == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
      if (ann_graph_segment_[index]->record_number_ == record_number ||
          record_number < globalConfig.MinimalGraphSize) {
        // No need to rebuild the ann graph.
        std::cout << "Skip rebuild ANN graph for attribute: "
                  << table_schema_.fields_[i].name_ << std::endl;
        ++index;
        continue;
      }

      VectorColumnData columnData;
      if (fType == meta::FieldType::VECTOR_FLOAT || fType == meta::FieldType::VECTOR_DOUBLE) {
        columnData = table_segment_
                         ->vector_tables_[table_segment_->field_name_mem_offset_map_
                                              [table_schema_.fields_[i].name_]];
      } else {
        // sparse vector
        columnData = &table_segment_
                          ->var_len_attr_table_[table_segment_->field_name_mem_offset_map_
                                                    [table_schema_.fields_[i].name_]];
      }

      // Rebuild the ann graph.
      std::cout << "Rebuild ANN graph for attribute: "
                << table_schema_.fields_[i].name_ << std::endl;
      if (is_leader_) {
        std::cout << "leader" << std::endl;
        // For leader, rebuild the ann graph.
        auto new_ann = std::make_shared<vectordb::engine::ANNGraphSegment>(false);
        new_ann->BuildFromVectorTable(
            columnData,
            record_number, table_schema_.fields_[i].vector_dimension_,
            mType);
        std::shared_ptr<vectordb::engine::ANNGraphSegment> ann_ptr =
            ann_graph_segment_[index];
        ann_graph_segment_[index] = new_ann;
        // Write the ANN graph to disk.
        std::cout << "Save ANN graph segment." << std::endl;
        ann_graph_segment_[index]->SaveANNGraph(
            db_catalog_path, table_schema_.id_, table_schema_.fields_[i].id_);
      } else {
        std::cout << "follower" << std::endl;
        // For follower, directly reload the ann graph from disk.
        auto new_ann = std::make_shared<vectordb::engine::ANNGraphSegment>(
            db_catalog_path, table_schema_.id_, table_schema_.fields_[i].id_);
        // Check if the ann graph has went ahead of the table. If so, skip.
        if (new_ann->record_number_ > record_number) {
          std::cout << "Skip sync ANN graph for attribute: "
                    << table_schema_.fields_[i].name_ << std::endl;
          ++index;
          continue;
        }
        std::shared_ptr<vectordb::engine::ANNGraphSegment> ann_ptr =
            ann_graph_segment_[index];
        ann_graph_segment_[index] = new_ann;
      }

      // Replace the executors.
      auto pool = std::make_shared<execution::ExecutorPool>();
      auto distFunc = GetDistFunc(fType, mType);

      for (int executorIdx = 0; executorIdx < globalConfig.NumExecutorPerField;
           executorIdx++) {
        pool->release(std::make_shared<execution::VecSearchExecutor>(
            table_schema_.fields_[i].vector_dimension_,
            ann_graph_segment_[index]->navigation_point_,
            ann_graph_segment_[index],
            ann_graph_segment_[index]->offset_table_,
            ann_graph_segment_[index]->neighbor_list_,
            columnData,
            distFunc,
            &table_schema_.fields_[i].vector_dimension_,
            globalConfig.IntraQueryThreads,
            globalConfig.MasterQueueSize,
            globalConfig.LocalQueueSize,
            globalConfig.GlobalSyncInterval));
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

Status TableMVP::SwapExecutors() {
  // Get the current record number.
  int64_t record_number = table_segment_->record_number_;

  int64_t index = 0;
  for (int i = 0; i < table_schema_.fields_.size(); ++i) {
    auto fType = table_schema_.fields_[i].field_type_;
    auto mType = table_schema_.fields_[i].metric_type_;

    if (fType == meta::FieldType::VECTOR_FLOAT ||
        fType == meta::FieldType::VECTOR_DOUBLE ||
        fType == meta::FieldType::SPARSE_VECTOR_FLOAT ||
        fType == meta::FieldType::SPARSE_VECTOR_DOUBLE) {

      VectorColumnData columnData;
      if (fType == meta::FieldType::VECTOR_FLOAT || fType == meta::FieldType::VECTOR_DOUBLE) {
        columnData = table_segment_
                         ->vector_tables_[table_segment_->field_name_mem_offset_map_
                                              [table_schema_.fields_[i].name_]];
      } else {
        // sparse vector
        columnData = &table_segment_
                          ->var_len_attr_table_[table_segment_->field_name_mem_offset_map_
                                                    [table_schema_.fields_[i].name_]];
      }

      // Rebuild the ann graph.
      std::cout << "Swap executors for attribute: "
                << table_schema_.fields_[i].name_ << std::endl;

      // Replace the executors.
      auto pool = std::make_shared<execution::ExecutorPool>();
      auto distFunc = GetDistFunc(fType, mType);

      for (int executorIdx = 0; executorIdx < globalConfig.NumExecutorPerField;
           executorIdx++) {
        pool->release(std::make_shared<execution::VecSearchExecutor>(
            table_schema_.fields_[i].vector_dimension_,
            ann_graph_segment_[index]->navigation_point_,
            ann_graph_segment_[index],
            ann_graph_segment_[index]->offset_table_,
            ann_graph_segment_[index]->neighbor_list_,
            columnData,
            distFunc,
            &table_schema_.fields_[i].vector_dimension_,
            globalConfig.IntraQueryThreads,
            globalConfig.MasterQueueSize,
            globalConfig.LocalQueueSize,
            globalConfig.GlobalSyncInterval));
      }
      std::unique_lock<std::mutex> lock(executor_pool_mutex_);
      executor_pool_.set(index, pool);
      lock.unlock();

      ++index;
    }
  }

  std::cout << "Swap executors done." << std::endl;
  return Status::OK();
}

Status TableMVP::Insert(vectordb::Json &record, std::unordered_map<std::string, std::string> &headers, bool upsert) {
  int64_t wal_id =
      wal_->WriteEntry(upsert ? LogEntryType::UPSERT : LogEntryType::INSERT, record.DumpToString());
  return table_segment_->Insert(table_schema_, record, wal_id, headers, upsert);
}

Status TableMVP::InsertPrepare(vectordb::Json &pks, vectordb::Json &result) {
  return table_segment_->InsertPrepare(table_schema_, pks, result);
}

Status TableMVP::Delete(
    vectordb::Json &records,
    const std::string &filter,
    std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes) {
  vectordb::Json delete_wal;
  delete_wal.LoadFromString("{}");
  delete_wal.SetObject("pk", records);
  delete_wal.SetString("filter", filter);
  int64_t wal_id =
      wal_->WriteEntry(LogEntryType::DELETE, delete_wal.DumpToString());
  return table_segment_->Delete(records, filter_nodes, wal_id);
}

Status TableMVP::Search(const std::string &field_name,
                        std::vector<std::string> &query_fields,
                        int64_t query_dimension, const VectorPtr query_data,
                        const int64_t limit, vectordb::Json &result,
                        std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
                        bool with_distance) {
  // Check if field_name exists.
  if (field_name_field_type_map_.find(field_name) == field_name_field_type_map_.end()) {
    return Status(DB_UNEXPECTED_ERROR, "Field name not found: " + field_name);
  }
  for (auto field : query_fields) {
    if (field_name_field_type_map_.find(field) == field_name_field_type_map_.end()) {
      return Status(DB_UNEXPECTED_ERROR, "Field name not found: " + field);
    }
  }

  // Get the field data type. If the type is not VECTOR, return error.
  auto field_type = field_name_field_type_map_[field_name];
  if (field_type != meta::FieldType::VECTOR_FLOAT &&
      field_type != meta::FieldType::VECTOR_DOUBLE &&
      field_type != meta::FieldType::SPARSE_VECTOR_FLOAT &&
      field_type != meta::FieldType::SPARSE_VECTOR_DOUBLE) {
    return Status(USER_ERROR, "Field type is not vector.");
  }

  if (std::holds_alternative<DenseVectorPtr>(query_data) && field_type != meta::FieldType::VECTOR_FLOAT &&
          field_type != meta::FieldType::VECTOR_DOUBLE ||
      std::holds_alternative<SparseVectorPtr>(query_data) && field_type != meta::FieldType::SPARSE_VECTOR_FLOAT &&
          field_type != meta::FieldType::SPARSE_VECTOR_DOUBLE) {
    return Status(USER_ERROR, "Query vector and field vector type must be both dense or sparse");
  }

  auto metric_type = field_name_metric_type_map_[field_name];

  // normalize the query data
  VectorPtr updatedQueryData = query_data;
  std::vector<DenseVectorElement> denseVec;
  auto sparseVecPtr = std::make_shared<SparseVector>();
  if (metric_type == meta::MetricType::COSINE) {
    if (std::holds_alternative<DenseVectorPtr>(query_data)) {
      auto q = std::get<DenseVectorPtr>(query_data);
      denseVec.resize(query_dimension);
      denseVec.insert(denseVec.begin(), q, q + query_dimension);
      Normalize((DenseVectorPtr)(denseVec.data()), query_dimension);
      updatedQueryData = denseVec.data();
    } else if (std::holds_alternative<SparseVectorPtr>(query_data)) {
      *sparseVecPtr = *std::get<SparseVectorPtr>(query_data);
      Normalize(*sparseVecPtr);
      updatedQueryData = sparseVecPtr;
    }
  }

  // Get the field offset in the vector table.
  int64_t field_offset = table_segment_->vec_field_name_executor_pool_idx_map_[field_name];

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
  if (std::holds_alternative<DenseVectorPtr>(query_data) && query_dimension != executor.exec_->dimension_) {
    return Status(DB_UNEXPECTED_ERROR,
                  "Query dimension doesn't match the vector field dimension.");
  }

  // Search.
  int64_t result_num = 0;
  executor.exec_->Search(updatedQueryData, table_segment_.get(), limit,
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

Status TableMVP::SearchByAttribute(
    std::vector<std::string> &query_fields,
    vectordb::Json &primary_keys,
    std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
    const int64_t skip,
    const int64_t limit,
    vectordb::Json &result) {
  // TODO: create a separate pool for search by attribute.
  int64_t field_offset = 0;
  // [Note] the following invocation is wrong
  //   execution::RAIIVecSearchExecutor(executor_pool_[field_offset],
  //   executor_pool_[field_offset]->acquire);
  // because the value of executor_pool_[field_offset] could be different when
  // evaluating twice, which will result in memory leak or core dump
  std::unique_lock<std::mutex> lock(executor_pool_mutex_);
  auto pool = executor_pool_.at(field_offset);
  auto executor = execution::RAIIVecSearchExecutor(pool, pool->acquire());
  lock.unlock();

  // Search.
  int64_t result_num = 0;
  executor.exec_->SearchByAttribute(
      table_segment_.get(),
      skip,
      limit,
      primary_keys,
      filter_nodes,
      result_num);
  auto status =
      Project(query_fields, result_num, executor.exec_->search_result_, result,
              false, executor.exec_->distance_);
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
      // Index fields are not responsed by default.
      if (table_schema_.fields_[i].is_index_field_) {
        continue;
      }
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
      if (field_name_field_type_map_[field] == meta::FieldType::VECTOR_FLOAT ||
          field_name_field_type_map_[field] == meta::FieldType::VECTOR_DOUBLE) {
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
      } else if (field_name_field_type_map_[field] == meta::FieldType::SPARSE_VECTOR_FLOAT ||
                 field_name_field_type_map_[field] == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
        // Vector field.
        vectordb::Json vector;
        vector.LoadFromString("{}");
        int64_t offset = table_segment_->field_name_mem_offset_map_[field];
        auto vec = std::get<SparseVectorPtr>(table_segment_->var_len_attr_table_[offset][id]);
        record.SetObject(field, ToJson(*vec));
      } else {
        // Primitive field.
        auto offset = table_segment_->field_name_mem_offset_map_[field] +
                      id * table_segment_->primitive_offset_;
        switch (field_name_field_type_map_[field]) {
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
            record.SetString(field,
                             std::get<std::string>(table_segment_->var_len_attr_table_[table_segment_->field_name_mem_offset_map_[field]][id]));
            break;
          }
          case meta::FieldType::JSON: {
            vectordb::Json json;
            json.LoadFromString(std::get<std::string>(table_segment_->var_len_attr_table_[table_segment_->field_name_mem_offset_map_[field]][id]));
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

size_t TableMVP::GetRecordCount() {
  return table_segment_->GetRecordCount();
}

void TableMVP::SetLeader(bool is_leader) {
  wal_->SetLeader(is_leader);
  is_leader_ = is_leader;
}

TableMVP::~TableMVP() {}

}  // namespace engine
}  // namespace vectordb
