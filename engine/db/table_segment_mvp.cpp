#include "db/table_segment_mvp.hpp"

#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <iostream>
#include <vector>

#include "utils/common_util.hpp"

namespace vectordb {
namespace engine {
// The number of bytes of a field value is stored in a continuous memory block.
constexpr size_t FieldTypeSizeMVP(meta::FieldType type) {
  switch (type) {
    case meta::FieldType::INT1:
      return 1;
    case meta::FieldType::INT2:
      return 2;
    case meta::FieldType::INT4:
      return 4;
    case meta::FieldType::INT8:
      return 8;
    case meta::FieldType::FLOAT:
      return 4;
    case meta::FieldType::DOUBLE:
      return 8;
    case meta::FieldType::BOOL:
      return 1;
    case meta::FieldType::STRING:
    case meta::FieldType::JSON:
    case meta::FieldType::SPARSE_VECTOR_DOUBLE:
    case meta::FieldType::SPARSE_VECTOR_FLOAT:
      // Variable length attribute requires a 8-byte pointer to the string table.
      return 8;
    case meta::FieldType::VECTOR_FLOAT:
    case meta::FieldType::VECTOR_DOUBLE:
      // For these types, we can't determine the size without additional information
      // like the length of the string or the dimension of the vector. You might want
      // to handle these cases differently.
      return 0;
    case meta::FieldType::UNKNOWN:
    default:
      // Unknown type
      return 0;
  }
}

Status TableSegmentMVP::Init(meta::TableSchema& table_schema, int64_t size_limit) {
  size_limit_ = size_limit;
  primitive_offset_ = 0;
  schema = table_schema;

  // Get how many primitive, vectors, and variable-length attributes (string, sparse vectors).
  for (auto& field_schema : table_schema.fields_) {
    auto current_total_vec_num = dense_vector_num_ + sparse_vector_num_;
    if (field_schema.field_type_ == meta::FieldType::STRING ||
        field_schema.field_type_ == meta::FieldType::JSON ||
        field_schema.field_type_ == meta::FieldType::SPARSE_VECTOR_DOUBLE ||
        field_schema.field_type_ == meta::FieldType::SPARSE_VECTOR_FLOAT) {
      field_id_mem_offset_map_[field_schema.id_] = var_len_attr_num_;
      field_name_mem_offset_map_[field_schema.name_] = var_len_attr_num_;
      if (field_schema.field_type_ == meta::FieldType::SPARSE_VECTOR_DOUBLE ||
          field_schema.field_type_ == meta::FieldType::SPARSE_VECTOR_FLOAT) {
        vec_field_name_executor_pool_idx_map_[field_schema.name_] = current_total_vec_num;
        sparse_vector_num_++;
      }
      if (field_schema.is_primary_key_) {
        string_pk_offset_ = std::make_unique<int64_t>(var_len_attr_num_);
      }
      var_len_attr_field_type_.push_back(field_schema.field_type_);
      ++var_len_attr_num_;
    } else if (field_schema.field_type_ == meta::FieldType::VECTOR_FLOAT ||
               field_schema.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
      vector_dims_.push_back(field_schema.vector_dimension_);
      field_id_mem_offset_map_[field_schema.id_] = current_total_vec_num;
      field_name_mem_offset_map_[field_schema.name_] = current_total_vec_num;
      vec_field_name_executor_pool_idx_map_[field_schema.name_] = current_total_vec_num;
      dense_vector_num_++;
    } else {
      field_id_mem_offset_map_[field_schema.id_] = primitive_offset_;
      field_name_mem_offset_map_[field_schema.name_] = primitive_offset_;
      primitive_offset_ += FieldTypeSizeMVP(field_schema.field_type_);
      ++primitive_num_;
    }

    if (field_schema.is_primary_key_) {
      pk_field_idx_ = std::make_unique<int64_t>(field_schema.id_);
    }
  }

  attribute_table_ = new char[size_limit * primitive_offset_];
  var_len_attr_table_.resize(var_len_attr_num_);

  // attribute_table_ = std::shared_ptr<char[]>(new char[size_limit * primitive_offset], std::default_delete<char[]>());
  // attribute_table_ = std::shared_ptr<char*>(new char[size_limit * primitive_offset]);
  vector_tables_ = new float*[dense_vector_num_];
  for (auto i = 0; i < dense_vector_num_; ++i) {
    // vector_tables_.emplace_back(std::make_shared<float[]>(size_limit * vector_dims_[i]);
    // vector_tables_.emplace_back(std::shared_ptr<float[]>(new float[size_limit * vector_dims_[i]], std::default_delete<float[]>()));
    vector_tables_[i] = new float[size_limit * vector_dims_[i]];
  }
  deleted_ = new ConcurrentBitset(size_limit);

  return Status::OK();
}

TableSegmentMVP::TableSegmentMVP(meta::TableSchema& table_schema, int64_t init_table_scale)
    : skip_sync_disk_(false),
      size_limit_(init_table_scale),
      first_record_id_(0),
      record_number_(0),
      field_name_mem_offset_map_(0),
      field_id_mem_offset_map_(0),
      primitive_num_(0),
      var_len_attr_num_(0),
      dense_vector_num_(0),
      sparse_vector_num_(0),
      vector_dims_(0) {
  Init(table_schema, init_table_scale);
}

TableSegmentMVP::TableSegmentMVP(meta::TableSchema& table_schema, const std::string& db_catalog_path, int64_t init_table_scale)
    : skip_sync_disk_(true),
      size_limit_(init_table_scale),
      first_record_id_(0),
      record_number_(0),
      field_name_mem_offset_map_(0),
      field_id_mem_offset_map_(0),
      primitive_num_(0),
      var_len_attr_num_(0),
      dense_vector_num_(0),
      sparse_vector_num_(0),
      vector_dims_(0),
      wal_global_id_(-1) {
  // Init the containers.
  Init(table_schema, init_table_scale);
  std::string path = db_catalog_path + "/" + std::to_string(table_schema.id_) + "/data_mvp.bin";
  if (server::CommonUtil::IsFileExist(path)) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      throw std::runtime_error("Cannot open file: " + path);
    }

    // Read the number of records and the first record id
    file.read(reinterpret_cast<char*>(&record_number_), sizeof(record_number_));
    file.read(reinterpret_cast<char*>(&first_record_id_), sizeof(first_record_id_));
    // If the table contains more records than the size limit, throw to avoid future core dump.
    if (record_number_ > init_table_scale) {
      file.close();
      throw std::runtime_error("The table contains " + std::to_string(record_number_) +
                               " records, which is larger than provided vector scale " +
                               std::to_string(init_table_scale));
    }

    // Read the bitset
    int64_t bitset_size = 0;
    file.read(reinterpret_cast<char*>(&bitset_size), sizeof(bitset_size));
    std::vector<uint8_t> bitset_data(bitset_size);
    file.read(reinterpret_cast<char*>(deleted_->data()), bitset_size);

    // Read the attribute table
    file.read(attribute_table_, record_number_ * primitive_offset_);

    // add pk into set
    if (isIntPK()) {
      auto field = table_schema.fields_[*pk_field_idx_];
      for (auto rIdx = 0; rIdx < record_number_; rIdx++) {
        // skip deleted entry
        if (deleted_->test(rIdx)) {
          continue;
        }
        switch (field.field_type_) {
          case meta::FieldType::INT1: {
            int8_t value = 0;
            std::memcpy(&value, &(attribute_table_[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int8_t));
            // do not check existance to avoid overhead
            primary_key_.addKeyIfNotExist(value, rIdx);
            break;
          }
          case meta::FieldType::INT2: {
            int16_t value = 0;
            std::memcpy(&value, &(attribute_table_[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int16_t));
            // do not check existance to avoid overhead
            primary_key_.addKeyIfNotExist(value, rIdx);
            break;
          }
          case meta::FieldType::INT4: {
            int32_t value = 0;
            std::memcpy(&value, &(attribute_table_[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int32_t));
            // do not check existance to avoid overhead
            primary_key_.addKeyIfNotExist(value, rIdx);
            break;
          }
          case meta::FieldType::INT8: {
            int64_t value = 0;
            std::memcpy(&value, &(attribute_table_[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int64_t));
            // do not check existance to avoid overhead
            primary_key_.addKeyIfNotExist(value, rIdx);
            break;
          }
          default:
            // other types cannot be PK, do nothing
            break;
        }
      }
    }

    var_len_attr_table_.resize(var_len_attr_num_);
    for (auto attrIdx = 0; attrIdx < var_len_attr_num_; ++attrIdx) {
      var_len_attr_table_[attrIdx].resize(record_number_);
    }

    // Read the string table
    // Loop order matters: on disk, the var-len attributes of whole entry is stored together
    for (auto recordIdx = 0; recordIdx < record_number_; ++recordIdx) {
      for (auto attrIdx = 0; attrIdx < var_len_attr_num_; ++attrIdx) {
        int64_t dataLen = 0;
        file.read(reinterpret_cast<char*>(&dataLen), sizeof(dataLen));
        switch (var_len_attr_field_type_[attrIdx]) {
          case meta::FieldType::STRING: {
            std::string str(dataLen, '\0');
            file.read(&str[0], dataLen);
            var_len_attr_table_[attrIdx][recordIdx] = std::move(str);
            // add pk into set
            if (!deleted_->test(recordIdx) && string_pk_offset_ && *string_pk_offset_ == attrIdx) {
              // do not check existance to avoid additional overhead
              primary_key_.addKeyIfNotExist(str, recordIdx);
            }
            break;
          }
          case meta::FieldType::JSON: {
            std::string str(dataLen, '\0');
            file.read(&str[0], dataLen);
            var_len_attr_table_[attrIdx][recordIdx] = std::move(str);
            break;
          }
          case meta::FieldType::SPARSE_VECTOR_DOUBLE:
          case meta::FieldType::SPARSE_VECTOR_FLOAT:
            auto v = std::make_shared<SparseVector>(dataLen);
            file.read(reinterpret_cast<char*>(v->data()), dataLen);
            var_len_attr_table_[attrIdx][recordIdx] = std::move(v);
            break;
        }
      }
    }

    // Read the vector table
    for (auto i = 0; i < dense_vector_num_; ++i) {
      file.read(reinterpret_cast<char*>(vector_tables_[i]), sizeof(float) * record_number_ * vector_dims_[i]);
    }

    // Last, read the global wal id.
    file.read(reinterpret_cast<char*>(&wal_global_id_), sizeof(wal_global_id_));

    // Close the file
    file.close();
  } else {
    // Create directory with an empty table segment.
    std::string folder_path = db_catalog_path + "/" + std::to_string(table_schema.id_);
    auto mkdir_status = server::CommonUtil::CreateDirectory(folder_path);
    if (!mkdir_status.ok()) {
      throw mkdir_status.message();
    }
    skip_sync_disk_ = false;
    auto status = SaveTableSegment(table_schema, db_catalog_path);
    if (!status.ok()) {
      throw status.message();
    }
  }
}

bool TableSegmentMVP::isStringPK() const {
  return string_pk_offset_ != nullptr;
}

bool TableSegmentMVP::isIntPK() const {
  return pk_field_idx_ && !string_pk_offset_;
}

int64_t TableSegmentMVP::pkFieldIdx() const {
  return *pk_field_idx_;
}

meta::FieldSchema TableSegmentMVP::pkField() const {
  return schema.fields_[*pk_field_idx_];
}

meta::FieldType TableSegmentMVP::pkType() const {
  return schema.fields_[*pk_field_idx_].field_type_;
}

bool TableSegmentMVP::isEntryDeleted(int64_t id) const {
  return deleted_->test(id);
}

Status TableSegmentMVP::Delete(Json& records, std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes, int64_t wal_id) {
  wal_global_id_ = wal_id;
  size_t deleted_record = 0;
  size_t pk_list_size = records.GetSize();
  vectordb::query::expr::ExprEvaluator expr_evaluator(
      filter_nodes,
      field_name_mem_offset_map_,
      primitive_offset_,
      var_len_attr_num_,
      attribute_table_,
      var_len_attr_table_);
  int filter_root_index = filter_nodes.size() - 1;

  if (pk_list_size > 0) {
    // Delete by the pk list.
    for (auto i = 0; i < pk_list_size; ++i) {
      auto pkField = records.GetArrayElement(i);
      if (isIntPK()) {
        auto fieldType = pkType();
        auto pk = pkField.GetInt();
        switch (pkType()) {
          case meta::FieldType::INT1:
            deleted_record += DeleteByIntPK(static_cast<int8_t>(pk), expr_evaluator, filter_root_index).ok();
            break;
          case meta::FieldType::INT2:
            deleted_record += DeleteByIntPK(static_cast<int16_t>(pk), expr_evaluator, filter_root_index).ok();
            break;
          case meta::FieldType::INT4:
            deleted_record += DeleteByIntPK(static_cast<int32_t>(pk), expr_evaluator, filter_root_index).ok();
            break;
          case meta::FieldType::INT8:
            deleted_record += DeleteByIntPK(static_cast<int64_t>(pk), expr_evaluator, filter_root_index).ok();
            break;
        }
      } else if (isStringPK()) {
        auto pk = pkField.GetString();
        deleted_record += DeleteByStringPK(pk, expr_evaluator, filter_root_index).ok();
      }
    }
  } else {
    // Delete by scanning the whole segment.
    for (auto id = 0; id < record_number_; ++id) {
      if (isIntPK()) {
        auto offset = field_id_mem_offset_map_[pkFieldIdx()] + id * primitive_offset_;
        auto pk = &attribute_table_[offset];
        auto fieldType = pkType();
        switch (pkType()) {
          case meta::FieldType::INT1:
            deleted_record += DeleteByIntPK(static_cast<int8_t>(*pk), expr_evaluator, filter_root_index).ok();
            break;
          case meta::FieldType::INT2:
            deleted_record += DeleteByIntPK(static_cast<int16_t>(*pk), expr_evaluator, filter_root_index).ok();
            break;
          case meta::FieldType::INT4:
            deleted_record += DeleteByIntPK(static_cast<int32_t>(*pk), expr_evaluator, filter_root_index).ok();
            break;
          case meta::FieldType::INT8:
            deleted_record += DeleteByIntPK(static_cast<int64_t>(*pk), expr_evaluator, filter_root_index).ok();
            break;
        }
      } else if (isStringPK()) {
        auto pk = std::get<std::string>(var_len_attr_table_[field_id_mem_offset_map_[pkFieldIdx()]][id]);
        deleted_record += DeleteByStringPK(pk, expr_evaluator, filter_root_index).ok();
      } else {
        deleted_record += DeleteByID(id, expr_evaluator, filter_root_index).ok();
      }
    }
  }
  return Status(DB_SUCCESS, "successfully deleted " + std::to_string(deleted_record) + " records.");
}

// Convert a primary key to an internal id
bool TableSegmentMVP::PK2ID(Json& record, size_t& id) {
  if (isIntPK()) {
    auto fieldType = pkType();
    auto pk = record.GetInt();
    switch (pkType()) {
      case meta::FieldType::INT1:
        return primary_key_.getKey(static_cast<int8_t>(pk), id);
      case meta::FieldType::INT2:
        return primary_key_.getKey(static_cast<int16_t>(pk), id);
      case meta::FieldType::INT4:
        return primary_key_.getKey(static_cast<int32_t>(pk), id);
      case meta::FieldType::INT8:
        return primary_key_.getKey(static_cast<int64_t>(pk), id);
    }
  } else if (isStringPK()) {
    auto pk = record.GetString();
    return primary_key_.getKey(pk, id);
  }
  return false;
}

Status TableSegmentMVP::DeleteByStringPK(
    const std::string& pk,
    vectordb::query::expr::ExprEvaluator& evaluator,
    int filter_root_index) {
  size_t result = 0;
  auto found = primary_key_.getKey(pk, result);
  if (found && evaluator.LogicalEvaluate(filter_root_index, result)) {
    deleted_->set(result);
    primary_key_.removeKey(pk);
    return Status::OK();
  }
  return Status(RECORD_NOT_FOUND, "Record with primary key not exist or skipped by filter: " + pk);
}

Status TableSegmentMVP::DeleteByID(
    const size_t id,
    vectordb::query::expr::ExprEvaluator& evaluator,
    int filter_root_index) {
  // Caller needs to guarantee the id is within record range.
  if (evaluator.LogicalEvaluate(filter_root_index, id)) {
    deleted_->set(id);
    return Status::OK();
  }
  return Status(RECORD_NOT_FOUND, "Record skipped by filter: " + id);
}

// Status TableSegmentMVP::DoubleSize() {
//   std::cout << "DoubleSizing ..." << std::endl;
//   int64_t new_size = size_limit_ * 2;

//   // Resize the attribute table
//   std::shared_ptr<AttributeTable> new_attribute_table = std::make_shared<AttributeTable>(AttributeTable(new_size * primitive_offset_));
//   memcpy(new_attribute_table->data, attribute_table_->data, record_number_ * primitive_offset_);
//   attribute_table_ = new_attribute_table;

//   // Resize the string table
//   std::string* new_string_table = new std::string[new_size * var_len_attr_num_];
//   memcpy(new_string_table, var_len_attr_table_.get(), size_limit_ * var_len_attr_num_);
//   var_len_attr_table_ = std::make_shared<std::string*>(new_string_table);

//   // Resize the vector tables
//   for (auto i = 0; i < vector_num_; ++i) {
//     float* new_vector_table = new float[new_size * vector_dims_[i]];
//     memcpy(new_vector_table, vector_tables_[i].get(), size_limit_ * vector_dims_[i]);
//     vector_tables_[i] = std::make_shared<float*>(new_vector_table);
//   }

//   // Resize the deleted bitset
//   auto new_bitset = std::make_shared<ConcurrentBitset>(new_size);
//   for (int i = 0; i < deleted_->size(); ++i) {
//     if (deleted_->test(i)) {
//       new_bitset->set(i);
//     }
//   }
//   deleted_ = new_bitset;

//   // Update the size limit
//   size_limit_ = new_size;

//   return Status::OK();
// }
Status TableSegmentMVP::Insert(meta::TableSchema& table_schema, Json& records, int64_t wal_id) {
  wal_global_id_ = wal_id;
  size_t new_record_size = records.GetSize();
  if (new_record_size == 0) {
    std::cout << "No records to insert." << std::endl;
    return Status::OK();
  }
  // Check if the records are valid.
  for (auto i = 0; i < new_record_size; ++i) {
    auto record = records.GetArrayElement(i);
    for (auto& field : table_schema.fields_) {
      if (!record.HasMember(field.name_)) {
        return Status(INVALID_RECORD, "Record " + std::to_string(i) + " missing field: " + field.name_);
      }
      if ((field.field_type_ == meta::FieldType::VECTOR_FLOAT ||
           field.field_type_ == meta::FieldType::VECTOR_DOUBLE) &&
          record.GetArraySize(field.name_) != field.vector_dimension_) {
        return Status(INVALID_RECORD, "Record " + std::to_string(i) + " field " + field.name_ + " has wrong dimension, expecting: " +
                                          std::to_string(field.vector_dimension_) + " actual: " + std::to_string(record.GetArraySize(field.name_)));
      }
      if ((field.field_type_ == meta::FieldType::SPARSE_VECTOR_FLOAT ||
           field.field_type_ == meta::FieldType::SPARSE_VECTOR_DOUBLE)) {
        auto indices = record.GetObject(field.name_).GetArray(SparseVecObjIndicesKey);
        auto size = indices.GetSize();
        auto vecDim = size > 0 ? indices.GetArrayElement(size - 1).GetInt() : field.vector_dimension_;  // if size is 0, the vector is zero
        if (vecDim >= field.vector_dimension_) {
          return Status(INVALID_RECORD,
                        "Record " + std::to_string(i) + " field " + field.name_ +
                            " has wrong dimension, expecting: " + std::to_string(field.vector_dimension_) + " actual: " + std::to_string(vecDim));
        }
        for (int i = 0; i + 1 < indices.GetSize(); i++) {
          if (indices.GetArrayElement(i).GetInt() >= indices.GetArrayElement(i + 1).GetInt()) {
            return Status(INVALID_RECORD,
                          "Record " + std::to_string(i) + " indices are not increasing");
          }
        }
      }
    }
  }

  // Resize if needed.
  if (record_number_ + new_record_size > size_limit_) {
    return Status(
        DB_UNEXPECTED_ERROR,
        "Currently, each table in this database can hold up to " + std::to_string(size_limit_) + " records. " +
            "To insert more records, please unload the database and reload with a larger vectorScale parameter.");
    // DoubleSize();
  }

  // Segment is modified.
  skip_sync_disk_.store(false);
  size_t skipped_entry = 0;

  // Process the insert.
  size_t cursor = record_number_;

  // reserve vector memory size
  for (auto& field : table_schema.fields_) {
    switch (field.field_type_) {
      case meta::FieldType::STRING:
      case meta::FieldType::JSON:
      case meta::FieldType::SPARSE_VECTOR_FLOAT:
      case meta::FieldType::SPARSE_VECTOR_DOUBLE:
        var_len_attr_table_[field_id_mem_offset_map_[field.id_]].resize(new_record_size + record_number_);
        break;
      default:
          // do nothing
          ;
    }
  }

  for (auto i = 0; i < new_record_size; ++i) {
    auto record = records.GetArrayElement(i);
    for (auto& field : table_schema.fields_) {
      if (field.field_type_ == meta::FieldType::STRING) {
        // Insert string attribute.
        auto value = record.GetString(field.name_);
        if (field.is_primary_key_) {
          auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
          if (exist) {
            // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
            skipped_entry++;
            goto LOOP_END;
          }
        }
        var_len_attr_table_[field_id_mem_offset_map_[field.id_]][cursor] = value;
      } else if (field.field_type_ == meta::FieldType::JSON) {
        // Insert json dumped string attribute.
        auto value = record.Get(field.name_).DumpToString();
        var_len_attr_table_[field_id_mem_offset_map_[field.id_]][cursor] = value;
      } else if (field.field_type_ == meta::FieldType::SPARSE_VECTOR_FLOAT ||
                 field.field_type_ == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
        // Insert vector attribute.
        auto sparseVecObject = record.GetObject(field.name_);

        auto indices = record.Get(field.name_).GetArray(SparseVecObjIndicesKey);
        auto values = record.Get(field.name_).GetArray(SparseVecObjValuesKey);
        auto nonZeroValueSize = indices.GetSize();
        if (indices.GetSize() != values.GetSize()) {
          std::cerr << "mismatched indices array length (" << indices.GetSize() << ") and value array length (" << values.GetSize() << "), skipping." << std::endl;
          skipped_entry++;
          goto LOOP_END;
        }

        auto vec = std::make_shared<SparseVector>();

        float sum = 0;
        for (auto j = 0; j < indices.GetSize(); ++j) {
          size_t index = static_cast<size_t>(indices.GetArrayElement(j).GetInt());
          float value = static_cast<float>(values.GetArrayElement(j).GetDouble());
          sum += value * value;
          vec->emplace_back(SparseVectorElement{index, value});
        }
        // convert to length
        if (field.metric_type_ == meta::MetricType::COSINE && sum > 1e-10) {
          sum = std::sqrt(sum);
          // normalize value
          for (auto& elem : *vec) {
            elem.value /= sum;
          }
        }
        var_len_attr_table_[field_id_mem_offset_map_[field.id_]][cursor] = std::move(vec);
      } else if (field.field_type_ == meta::FieldType::VECTOR_FLOAT ||
                 field.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
        // Insert vector attribute.
        auto vector = record.GetArray(field.name_);
        float sum = 0;
        for (auto j = 0; j < field.vector_dimension_; ++j) {
          float value = static_cast<float>((float)(vector.GetArrayElement(j).GetDouble()));
          sum += value * value;
          std::memcpy(&(vector_tables_[field_id_mem_offset_map_[field.id_]][cursor * vector_dims_[field_id_mem_offset_map_[field.id_]] + j]), &value, sizeof(float));
        }
        // convert to length
        if (field.metric_type_ == meta::MetricType::COSINE && sum > 1e-10) {
          sum = std::sqrt(sum);
          // normalize value
          for (auto j = 0; j < field.vector_dimension_; ++j) {
            vector_tables_[field_id_mem_offset_map_[field.id_]][cursor * vector_dims_[field_id_mem_offset_map_[field.id_]] + j] /= sum;
          }
        }
      } else {
        // Insert primitive attribute.
        switch (field.field_type_) {
          case meta::FieldType::INT1: {
            int8_t value = static_cast<int8_t>((int8_t)(record.GetInt(field.name_)));
            if (field.is_primary_key_) {
              auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
              if (exist) {
                // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
                skipped_entry++;
                goto LOOP_END;
              }
            }
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int8_t));
            break;
          }
          case meta::FieldType::INT2: {
            int16_t value = static_cast<int16_t>((int16_t)(record.GetInt(field.name_)));
            if (field.is_primary_key_) {
              auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
              if (exist) {
                // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
                skipped_entry++;
                goto LOOP_END;
              }
            }
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int16_t));
            break;
          }
          case meta::FieldType::INT4: {
            int32_t value = static_cast<int32_t>((int32_t)(record.GetInt(field.name_)));
            if (field.is_primary_key_) {
              auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
              if (exist) {
                // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
                skipped_entry++;
                goto LOOP_END;
              }
            }
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int32_t));
            break;
          }
          case meta::FieldType::INT8: {
            int64_t value = static_cast<int64_t>((int64_t)(record.GetInt(field.name_)));
            if (field.is_primary_key_) {
              auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
              if (exist) {
                // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
                skipped_entry++;
                goto LOOP_END;
              }
            }
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int64_t));
            break;
          }
          case meta::FieldType::FLOAT: {
            float value = static_cast<float>((float)(record.GetDouble(field.name_)));
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(float));
            break;
          }
          case meta::FieldType::DOUBLE: {
            double value = record.GetDouble(field.name_);
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(double));
            break;
          }
          case meta::FieldType::BOOL: {
            bool value = record.GetBool(field.name_);
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(bool));
            break;
          }
          default:
            break;
        }
      }
    }
    ++cursor;
  LOOP_END: {}
    // nothing should be done at the end of block
  }

  // shrink the size if necessary
  if (cursor != new_record_size + record_number_) {
    for (auto& field : table_schema.fields_) {
      switch (field.field_type_) {
        case meta::FieldType::STRING:
        case meta::FieldType::JSON:
        case meta::FieldType::SPARSE_VECTOR_FLOAT:
        case meta::FieldType::SPARSE_VECTOR_DOUBLE:
          var_len_attr_table_[field_id_mem_offset_map_[field.id_]].resize(cursor);
          break;
        default:
            // do nothing
            ;
      }
    }
  }
  // update the vector size
  record_number_.store(cursor);

  auto msg = std::string("successfully inserted " +
                         std::to_string(new_record_size - skipped_entry) +
                         " records. ");
  if (skipped_entry > 0) {
    msg += "skipped " +
           std::to_string(skipped_entry) + " records with primary key values that already exist.";
  }
  std::cerr << msg << std::endl;
  return Status(DB_SUCCESS, msg);
}

Status TableSegmentMVP::InsertPrepare(meta::TableSchema& table_schema, Json& pks, Json& result) {
  // Put the segment capacity and current occupation.
  result.SetInt("capacity", size_limit_);
  result.SetInt("recordNumber", record_number_);

  // Check which primary keys already exist.
  auto pk_list_size = pks.GetSize();
  if (pk_list_size > 0) {
    Json masks;
    masks.LoadFromString("[]");
    uint32_t mask;
    size_t temp;
    for (auto i = 0; i < pk_list_size; ++i) {
      size_t mod = i % 32;
      if (mod == 0) {
        mask = 4294967295;
      }
      auto pk = pks.GetArrayElement(i);
      if (isIntPK()) {
        auto fieldType = pkType();
        auto val = pk.GetInt();
        switch (pkType()) {
          case meta::FieldType::INT1:
            if (primary_key_.getKey(static_cast<int8_t>(val), temp)) {
              mask -= 1 << mod;
            }
            break;
          case meta::FieldType::INT2:
            if (primary_key_.getKey(static_cast<int16_t>(val), temp)) {
              mask -= 1 << mod;
            }
            break;
          case meta::FieldType::INT4:
            if (primary_key_.getKey(static_cast<int32_t>(val), temp)) {
              mask -= 1 << mod;
            }
            break;
          case meta::FieldType::INT8:
            if (primary_key_.getKey(static_cast<int64_t>(val), temp)) {
              mask -= 1 << mod;
            }
            break;
        }
      } else if (isStringPK()) {
        auto val = pk.GetString();
        if (primary_key_.getKey(val, temp)) {
          mask -= 1 << mod;
        }
      }
      if (mod == 31) {
        masks.AddIntToArray(mask);
      }
    }
    if (pk_list_size % 32 != 0) {
      masks.AddIntToArray(mask);
    }
    result.SetObject("masks", masks);
  }

  return Status(DB_SUCCESS, "");
}

// Status TableSegmentMVP::SaveTableSegment(meta::TableSchema& table_schema, const std::string& db_catalog_path) {
//   if (skip_sync_disk_) {
//     return Status::OK();
//   }

//   // Construct the file path
//   std::string path = db_catalog_path + "/" + std::to_string(table_schema.id_) + "/data_mvp.bin";
//   std::string tmp_path = path + ".tmp";

//   std::ofstream file(tmp_path, std::ios::binary);
//   if (!file) {
//     return Status(DB_UNEXPECTED_ERROR, "Cannot open file: " + path);
//   }

//   // Write the number of records and the first record id
//   file.write(reinterpret_cast<const char*>(&record_number_), sizeof(record_number_));
//   file.write(reinterpret_cast<const char*>(&first_record_id_), sizeof(first_record_id_));

//   // Write the biset
//   int64_t bitset_size = deleted_->size();
//   const uint8_t* bitset_data = deleted_->data();
//   file.write(reinterpret_cast<const char*>(&bitset_size), sizeof(bitset_size));
//   file.write(reinterpret_cast<const char*>(bitset_data), bitset_size);

//   // // Write the attribute table.
//   file.write(attribute_table_, record_number_ * primitive_offset_);

//   // Write the string table.
//   for (auto i = 0; i < record_number_; ++i) {
//     for (auto j = 0; j < var_len_attr_num_; ++j) {
//       int64_t offset = i * var_len_attr_num_ + j;
//       int64_t string_length = var_len_attr_table_[offset].size();
//       file.write(reinterpret_cast<const char*>(&string_length), sizeof(string_length));
//       file.write(var_len_attr_table_[offset].c_str(), string_length);
//     }
//   }

//   // Write the vector table.
//   for (auto i = 0; i < vector_num_; ++i) {
//     file.write(reinterpret_cast<const char*>(vector_tables_[i]), sizeof(float) * record_number_ * vector_dims_[i]);
//   }

//   // Flush change to disk and close the file
//   fsync(fileno(file));
//   file.close();

//   if (!file) {
//     return Status(DB_UNEXPECTED_ERROR, "Failed to write to file: " + path);
//   }

//   if (std::rename(tmp_path.c_str(), path.c_str()) != 0) {
//     // LOG_SERVER_ERROR_ << "Failed to rename temp file: " << temp_path << " to " << path;
//     return Status(INFRA_UNEXPECTED_ERROR, "Failed to rename temp file: " + tmp_path + " to " + path);
//   }

//   // Skip next time until the segment is modified.
//   skip_sync_disk_ = true;

//   return Status::OK();
// }

Status TableSegmentMVP::SaveTableSegment(meta::TableSchema& table_schema, const std::string& db_catalog_path) {
  if (skip_sync_disk_) {
    return Status::OK();
  }

  // Construct the file path
  std::string path = db_catalog_path + "/" + std::to_string(table_schema.id_) + "/data_mvp.bin";
  std::string tmp_path = path + ".tmp";

  FILE* file = fopen(tmp_path.c_str(), "wb");
  if (!file) {
    return Status(DB_UNEXPECTED_ERROR, "Cannot open file: " + path);
  }

  // Write the number of records and the first record id
  fwrite(&record_number_, sizeof(record_number_), 1, file);
  fwrite(&first_record_id_, sizeof(first_record_id_), 1, file);

  // Write the bitset
  int64_t bitset_size = deleted_->size();
  const uint8_t* bitset_data = deleted_->data();
  fwrite(&bitset_size, sizeof(bitset_size), 1, file);
  fwrite(bitset_data, bitset_size, 1, file);

  // Write the attribute table.
  fwrite(attribute_table_, record_number_ * primitive_offset_, 1, file);

  // Write the variable length table.
  for (auto recordIdx = 0; recordIdx < record_number_; ++recordIdx) {
    for (auto attrIdx = 0; attrIdx < var_len_attr_num_; ++attrIdx) {
      auto& entry = var_len_attr_table_[attrIdx][recordIdx];
      if (std::holds_alternative<std::string>(entry)) {
        auto& str = std::get<std::string>(entry);
        int64_t attr_len = str.size();
        fwrite(&attr_len, sizeof(attr_len), 1, file);
        fwrite(&str[0], attr_len, 1, file);
      } else {
        // sparse vector
        auto& vec = std::get<SparseVectorPtr>(entry);
        int64_t attr_len = vec->size() * sizeof(SparseVectorElement);
        fwrite(&attr_len, sizeof(attr_len), 1, file);
        fwrite(vec->data(), attr_len, 1, file);
      }
    }
  }

  // Write the vector table.
  for (auto i = 0; i < dense_vector_num_; ++i) {
    fwrite(vector_tables_[i], sizeof(float) * record_number_ * vector_dims_[i], 1, file);
  }

  // Last, write the global wal id.
  fwrite(&wal_global_id_, sizeof(wal_global_id_), 1, file);

  // Flush changes to disk
  fflush(file);
  fsync(fileno(file));

  // Close the file
  fclose(file);

  if (std::rename(tmp_path.c_str(), path.c_str()) != 0) {
    // LOG_SERVER_ERROR_ << "Failed to rename temp file: " << temp_path << " to " << path;
    return Status(INFRA_UNEXPECTED_ERROR, "Failed to rename temp file: " + tmp_path + " to " + path);
  }

  // Skip next time until the segment is modified.
  skip_sync_disk_ = true;

  return Status::OK();
}

void TableSegmentMVP::Debug(meta::TableSchema& table_schema) {
  std::cout << "skip_sync_disk_: " << skip_sync_disk_ << "\n";
  std::cout << "size_limit_: " << size_limit_ << "\n";
  std::cout << "first_record_id_: " << first_record_id_ << "\n";
  std::cout << "record_number_: " << record_number_ << "\n";

  std::cout << "field_id_mem_offset_map_: ";
  for (const auto& pair : field_id_mem_offset_map_) {
    std::cout << "{" << pair.first << ": " << pair.second << "}, ";
  }
  std::cout << "\n";

  std::cout << "primitive_num_: " << primitive_num_ << "\n";
  std::cout << "var_len_attr_num_: " << var_len_attr_num_ << "\n";
  std::cout << "vector_num_: " << dense_vector_num_ << "\n";

  // Print out attribute_table_
  std::cout << "attribute_table_: \n";
  for (size_t i = 0; i < record_number_ * primitive_offset_; ++i) {
    for (int j = 7; j >= 0; --j) {
      std::cout << ((attribute_table_[i] >> j) & 1);
    }
    std::cout << ' ';
  }
  std::cout << "\n";
  for (size_t i = 0; i < record_number_; ++i) {
    for (auto& field : table_schema.fields_) {
      if (field.field_type_ != meta::FieldType::STRING &&
          field.field_type_ != meta::FieldType::JSON &&
          field.field_type_ != meta::FieldType::VECTOR_FLOAT &&
          field.field_type_ != meta::FieldType::VECTOR_DOUBLE &&
          field.field_type_ != meta::FieldType::SPARSE_VECTOR_FLOAT &&
          field.field_type_ != meta::FieldType::SPARSE_VECTOR_DOUBLE) {
        // Extract primitive attribute.
        switch (field.field_type_) {
          case meta::FieldType::INT1: {
            int8_t value;
            std::memcpy(&value, &(attribute_table_[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int8_t));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::INT2: {
            int16_t value;
            std::memcpy(&value, &(attribute_table_[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int16_t));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::INT4: {
            int32_t value;
            std::memcpy(&value, &(attribute_table_[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int32_t));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::INT8: {
            int64_t value;
            std::memcpy(&value, &(attribute_table_[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int64_t));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::FLOAT: {
            float value;
            std::memcpy(&value, &(attribute_table_[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(float));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::DOUBLE: {
            double value;
            std::memcpy(&value, &(attribute_table_[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(double));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::BOOL: {
            bool value;
            std::memcpy(&value, &(attribute_table_[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(bool));
            std::cout << (value ? "true" : "false") << " ";
            break;
          }
          default:
            break;
        }
      }
    }
    std::cout << "\n";
  }

  // Print out var_len_attr_table_
  std::cout << "var_len_attr_table_:  \n";
  for (size_t recordIdx = 0; recordIdx < record_number_; ++recordIdx) {
    for (auto& field : table_schema.fields_) {
      if (field.field_type_ == meta::FieldType::STRING ||
          field.field_type_ == meta::FieldType::JSON ||
          field.field_type_ == meta::FieldType::SPARSE_VECTOR_FLOAT ||
          field.field_type_ == meta::FieldType::SPARSE_VECTOR_DOUBLE) {
        switch (field.field_type_) {
          case meta::FieldType::STRING:
          case meta::FieldType::JSON:
            std::cout << std::get<std::string>(var_len_attr_table_[field_id_mem_offset_map_[field.id_]][recordIdx])
                      << ", "
                      << std::endl;
            break;
          case meta::FieldType::SPARSE_VECTOR_FLOAT:
          case meta::FieldType::SPARSE_VECTOR_DOUBLE: {
            auto& vec = std::get<SparseVectorPtr>(var_len_attr_table_[field_id_mem_offset_map_[field.id_]][recordIdx]);
            for (int i = 0; i < vec->size(); i++) {
              std::cout << ((*vec)[i].index) << ":" << (*vec)[i].value << ",";
            }
            std::cout << std::endl;
            break;
          }
          default:
            // do nothing
            break;
        }
      }
    }
  }

  std::cout << "vector_dims_:  \n";
  for (const auto& dim : vector_dims_) {
    std::cout << dim << ", ";
  }
  std::cout << "\n";

  // Print out vector_tables_
  std::cout << "vector_tables_:  \n";
  for (size_t i = 0; i < dense_vector_num_; ++i) {
    for (size_t j = 0; j < record_number_; ++j) {
      for (size_t k = 0; k < vector_dims_[i]; ++k) {
        size_t offset = j * vector_dims_[i] + k;
        std::cout << vector_tables_[i][offset] << " ";
      }
      std::cout << "\n";
    }
    std::cout << "\n";
  }

  std::cout << "deleted_: ";
  std::cout << deleted_->size() << "\n";
  for (size_t i = 0; i < record_number_; ++i) {
    std::cout << deleted_->test(i);
  }
  std::cout << "\n";
}

TableSegmentMVP::~TableSegmentMVP() {
  if (attribute_table_ != nullptr) {
    delete[] attribute_table_;
  }
  if (vector_tables_ != nullptr) {
    for (auto i = 0; i < dense_vector_num_; ++i) {
      delete[] vector_tables_[i];
    }
    delete[] vector_tables_;
  }
  if (deleted_ != nullptr) {
    delete deleted_;
  }
}

}  // namespace engine
}  // namespace vectordb
