#include "db/table_segment_mvp.hpp"

#include <iostream>

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
      // String attribute requires a 8-byte pointer to the string table.
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

constexpr const int64_t InitTableSize = 6;

Status TableSegmentMVP::Init(meta::TableSchema& table_schema, int64_t size_limit) {
  size_limit_ = size_limit;
  primitive_offset_ = 0;

  // Get how many primitive, vectors, and strings attributes.
  for (auto& field_schema : table_schema.fields_) {
    if (field_schema.field_type_ == meta::FieldType::STRING) {
      field_id_mem_offset_map_[field_schema.id_] = string_num_;
      ++string_num_;
    } else if (field_schema.field_type_ == meta::FieldType::VECTOR_FLOAT ||
               field_schema.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
      vector_dims_.push_back(field_schema.vector_dimension_);
      field_id_mem_offset_map_[field_schema.id_] = vector_num_;
      ++vector_num_;
    } else {
      field_id_mem_offset_map_[field_schema.id_] = primitive_offset_;
      primitive_offset_ += FieldTypeSizeMVP(field_schema.field_type_);
      ++primitive_num_;
    }
  }
  string_table_ = std::make_shared<std::string*>(new std::string[size_limit * string_num_]);

  attribute_table_ = std::make_shared<char*>(new char[size_limit * primitive_offset_]);
  // attribute_table_ = std::shared_ptr<char[]>(new char[size_limit * primitive_offset], std::default_delete<char[]>());
  // attribute_table_ = std::shared_ptr<char*>(new char[size_limit * primitive_offset]);

  for (auto i = 0; i < vector_num_; ++i) {
    // vector_tables_.emplace_back(std::make_shared<float[]>(size_limit * vector_dims_[i]);
    // vector_tables_.emplace_back(std::shared_ptr<float[]>(new float[size_limit * vector_dims_[i]], std::default_delete<float[]>()));
    vector_tables_.emplace_back(std::make_shared<float*>(new float[size_limit * vector_dims_[i]]));
  }
  deleted_ = std::make_shared<ConcurrentBitset>(size_limit);

  return Status::OK();
}

TableSegmentMVP::TableSegmentMVP(meta::TableSchema& table_schema)
    : skip_sync_disk_(false),
      size_limit_(InitTableSize),
      first_record_id_(0),
      record_number_(0),
      field_id_mem_offset_map_(0),
      primitive_num_(0),
      string_num_(0),
      vector_num_(0),
      vector_dims_(0) {
  Init(table_schema, InitTableSize);
}

Status TableSegmentMVP::DoubleSize() {
  std::cout << "DoubleSizing ..." << std::endl;
  int64_t new_size = size_limit_ * 2;

  // Resize the attribute table
  char* new_attribute_table = new char[new_size * primitive_offset_];
  memcpy(new_attribute_table, attribute_table_.get(), size_limit_ * primitive_offset_);
  attribute_table_ = std::make_shared<char*>(new_attribute_table);

  // Resize the string table
  std::string* new_string_table = new std::string[new_size * string_num_];
  memcpy(new_string_table, string_table_.get(), size_limit_ * string_num_);
  string_table_ = std::make_shared<std::string*>(new_string_table);

  // Resize the vector tables
  for (auto i = 0; i < vector_num_; ++i) {
    float* new_vector_table = new float[new_size * vector_dims_[i]];
    memcpy(new_vector_table, vector_tables_[i].get(), size_limit_ * vector_dims_[i]);
    vector_tables_[i] = std::make_shared<float*>(new_vector_table);
  }

  // Resize the deleted bitset
  auto new_bitset = std::make_shared<ConcurrentBitset>(new_size);
  for (int i = 0; i < deleted_->size(); ++i) {
    if (deleted_->test(i)) {
      new_bitset->set(i);
    }
  }
  deleted_ = new_bitset;

  // Update the size limit
  size_limit_ = new_size;

  return Status::OK();
}

Status TableSegmentMVP::Insert(meta::TableSchema& table_schema, Json& records) {
  size_t new_record_size = records.GetSize();
  if (new_record_size == 0) {
    std::cout << "No records to insert." << std::endl;
    return Status::OK();
  }
  // Check if the records are valid.
  for (auto i = 0; i < new_record_size; ++i) {
    auto record = records.GetArrayElement(i);
    // std::cout << "records: " << records.DumpToString() << std::endl;
    // std::cout << "record: " << record.DumpToString() << std::endl;
    for (auto& field : table_schema.fields_) {
      if (!record.HasMember(field.name_)) {
        return Status(INVALID_RECORD, "Record " + std::to_string(i) + " missing field: " + field.name_);
      }
      if (field.field_type_ == meta::FieldType::VECTOR_FLOAT ||
          field.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
        if (record.GetArraySize(field.name_) != field.vector_dimension_) {
          return Status(INVALID_RECORD, "Record " + std::to_string(i) + " field " + field.name_ + " has wrong dimension.");
        }
      }
    }
  }

  // Resize if needed.
  if (record_number_ + new_record_size >= size_limit_) {
    DoubleSize();
  }

  // Process the insert.
  size_t cursor = record_number_;
  for (auto i = 0; i < new_record_size; ++i) {
    auto record = records.GetArrayElement(i);
    for (auto& field : table_schema.fields_) {
      if (field.field_type_ == meta::FieldType::STRING) {
        // Insert string attribute.
        (*string_table_)[cursor * string_num_ + field_id_mem_offset_map_[field.id_]] = record.GetString(field.name_);
      } else if (field.field_type_ == meta::FieldType::VECTOR_FLOAT ||
                 field.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
        // Insert vector attribute.
        auto vector = record.GetArray(field.name_);
        for (auto j = 0; j < field.vector_dimension_; ++j) {
          float value = static_cast<float>((float)(vector.GetArrayElement(j).GetDouble()));
          std::memcpy(&(*vector_tables_[field_id_mem_offset_map_[field.id_]])[cursor * vector_dims_[field_id_mem_offset_map_[field.id_]] + j], &value, sizeof(float));
        }
      } else {
        // Insert primitive attribute.
        switch (field.field_type_) {
          case meta::FieldType::INT1: {
            int8_t value = static_cast<int8_t>((int8_t)(record.GetInt(field.name_)));
            std::memcpy(&(*attribute_table_)[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]], &value, sizeof(int8_t));
            break;
          }
          case meta::FieldType::INT2: {
            int16_t value = static_cast<int16_t>((int16_t)(record.GetInt(field.name_)));
            std::memcpy(&(*attribute_table_)[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]], &value, sizeof(int16_t));
            break;
          }
          case meta::FieldType::INT4: {
            int32_t value = static_cast<int32_t>((int32_t)(record.GetInt(field.name_)));
            std::memcpy(&(*attribute_table_)[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]], &value, sizeof(int32_t));
            break;
          }
          case meta::FieldType::INT8: {
            int64_t value = static_cast<int64_t>((int64_t)(record.GetInt(field.name_)));
            std::memcpy(&(*attribute_table_)[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]], &value, sizeof(int64_t));
            break;
          }
          case meta::FieldType::FLOAT: {
            float value = static_cast<float>((float)(record.GetDouble(field.name_)));
            std::memcpy(&(*attribute_table_)[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]], &value, sizeof(float));
            break;
          }
          case meta::FieldType::DOUBLE: {
            double value = record.GetDouble(field.name_);
            std::memcpy(&(*attribute_table_)[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]], &value, sizeof(double));
            break;
          }
          case meta::FieldType::BOOL: {
            bool value = record.GetBool(field.name_);
            std::memcpy(&(*attribute_table_)[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]], &value, sizeof(bool));
            break;
          }
          default:
            break;
        }
      }
    }
    ++cursor;
  }
  record_number_.store(cursor);
  return Status::OK();
}

void TableSegmentMVP::Debug() {
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
  std::cout << "string_num_: " << string_num_ << "\n";
  std::cout << "vector_num_: " << vector_num_ << "\n";

  // Assuming attribute_table_ points to a single line string
  // std::cout << "attribute_table_: " << *attribute_table_ << "\n";

  // std::cout << "string_tables_: ";
  // for (const auto& vec : string_tables_) {
  //   std::cout << "{ ";
  //   for (const auto& str : vec) {
  //     std::cout << str << ", ";
  //   }
  //   std::cout << "}, ";
  // }
  // std::cout << "\n";

  std::cout << "vector_dims_: ";
  for (const auto& dim : vector_dims_) {
    std::cout << dim << ", ";
  }
  std::cout << "\n";

  // Assuming each vector_tables_ points to a single line string
  std::cout << "vector_tables_: ";
  // for (const auto& ptr : vector_tables_) {
  //   std::cout << *ptr << ", ";
  // }
  // std::cout << "\n";

  // Assuming deleted_->data() returns a string representation of the bitset
  // std::cout << "deleted_: " << deleted_->data() << "\n";
}

TableSegmentMVP::~TableSegmentMVP() {
  // if (attribute_table_ != nullptr) {
  //   delete[] attribute_table_;
  // }
  // if (vector_tables_ != nullptr) {
  //   for (auto i = 0; i < vector_num_; ++i) {
  //     delete[] vector_tables_[i];
  //   }
  //   delete[] vector_tables_;
  // }
}

}  // namespace engine
}  // namespace vectordb
