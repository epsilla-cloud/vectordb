#include "db/table_segment_mvp.hpp"

#include <fstream>
#include <iostream>

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

constexpr const int64_t InitTableSize = 50000;

Status TableSegmentMVP::Init(meta::TableSchema& table_schema, int64_t size_limit) {
  size_limit_ = size_limit;
  primitive_offset_ = 0;

  // Get how many primitive, vectors, and strings attributes.
  for (auto& field_schema : table_schema.fields_) {
    if (field_schema.field_type_ == meta::FieldType::STRING) {
      field_id_mem_offset_map_[field_schema.id_] = string_num_;
      field_name_mem_offset_map_[field_schema.name_] = string_num_;
      ++string_num_;
    } else if (field_schema.field_type_ == meta::FieldType::VECTOR_FLOAT ||
               field_schema.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
      vector_dims_.push_back(field_schema.vector_dimension_);
      field_id_mem_offset_map_[field_schema.id_] = vector_num_;
      field_name_mem_offset_map_[field_schema.name_] = vector_num_;
      ++vector_num_;
    } else {
      field_id_mem_offset_map_[field_schema.id_] = primitive_offset_;
      field_name_mem_offset_map_[field_schema.name_] = primitive_offset_;
      primitive_offset_ += FieldTypeSizeMVP(field_schema.field_type_);
      ++primitive_num_;
    }
  }

  string_table_ = new std::string[size_limit * string_num_];

  attribute_table_ = new char[size_limit * primitive_offset_];
    
  // attribute_table_ = std::shared_ptr<char[]>(new char[size_limit * primitive_offset], std::default_delete<char[]>());
  // attribute_table_ = std::shared_ptr<char*>(new char[size_limit * primitive_offset]);
  vector_tables_ = new float*[vector_num_];
  for (auto i = 0; i < vector_num_; ++i) {
    // vector_tables_.emplace_back(std::make_shared<float[]>(size_limit * vector_dims_[i]);
    // vector_tables_.emplace_back(std::shared_ptr<float[]>(new float[size_limit * vector_dims_[i]], std::default_delete<float[]>()));
    vector_tables_[i] = new float[size_limit * vector_dims_[i]];
  }
  deleted_ = new ConcurrentBitset(size_limit);

  return Status::OK();
}

TableSegmentMVP::TableSegmentMVP(meta::TableSchema& table_schema)
    : skip_sync_disk_(false),
      size_limit_(InitTableSize),
      first_record_id_(0),
      record_number_(0),
      field_name_mem_offset_map_(0),
      field_id_mem_offset_map_(0),
      primitive_num_(0),
      string_num_(0),
      vector_num_(0),
      vector_dims_(0) {
  Init(table_schema, InitTableSize);
}

TableSegmentMVP::TableSegmentMVP(meta::TableSchema& table_schema, const std::string& db_catalog_path)
    : skip_sync_disk_(true),
      size_limit_(InitTableSize),
      first_record_id_(0),
      record_number_(0),
      field_name_mem_offset_map_(0),
      field_id_mem_offset_map_(0),
      primitive_num_(0),
      string_num_(0),
      vector_num_(0),
      vector_dims_(0) {
  // Init the containers.
  Init(table_schema, InitTableSize);
  std::string path = db_catalog_path + "/" + std::to_string(table_schema.id_) + "/data_mvp.bin";
  if (server::CommonUtil::IsFileExist(path)) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      throw std::runtime_error("Cannot open file: " + path);
    }

    // Read the number of records and the first record id
    file.read(reinterpret_cast<char*>(&record_number_), sizeof(record_number_));
    file.read(reinterpret_cast<char*>(&first_record_id_), sizeof(first_record_id_));

    // Read the bitset
    int64_t bitset_size = 0;
    file.read(reinterpret_cast<char*>(&bitset_size), sizeof(bitset_size));
    std::vector<uint8_t> bitset_data(bitset_size);
    file.read(reinterpret_cast<char*>(deleted_->data()), bitset_size);

    // Read the attribute table
    file.read(attribute_table_, record_number_ * primitive_offset_);

    // Read the string table
    for (auto i = 0; i < record_number_; ++i) {
      for (auto j = 0; j < string_num_; ++j) {
        int64_t offset = i * string_num_ + j;
        int64_t string_length = 0;
        file.read(reinterpret_cast<char*>(&string_length), sizeof(string_length));
        std::string str(string_length, '\0');
        file.read(&str[0], string_length);
        string_table_[offset] = str;
      }
    }

    // Read the vector table
    for (auto i = 0; i < vector_num_; ++i) {
      file.read(reinterpret_cast<char*>(vector_tables_[i]), sizeof(float) * record_number_ * vector_dims_[i]);
    }

    // Close the file
    file.close();
  } else {
    // Create directory with an empty table segment.
    std::string folder_path = db_catalog_path + "/" + std::to_string(table_schema.id_);
    server::CommonUtil::CreateDirectory(folder_path);
    skip_sync_disk_ = false;
    auto status = SaveTableSegment(table_schema, db_catalog_path);
    if (!status.ok()) {
      throw status.message();
    }
  }
}

// Status TableSegmentMVP::DoubleSize() {
//   std::cout << "DoubleSizing ..." << std::endl;
//   int64_t new_size = size_limit_ * 2;

//   // Resize the attribute table
//   std::shared_ptr<AttributeTable> new_attribute_table = std::make_shared<AttributeTable>(AttributeTable(new_size * primitive_offset_));
//   memcpy(new_attribute_table->data, attribute_table_->data, record_number_ * primitive_offset_);
//   attribute_table_ = new_attribute_table;

//   // Resize the string table
//   std::string* new_string_table = new std::string[new_size * string_num_];
//   memcpy(new_string_table, string_table_.get(), size_limit_ * string_num_);
//   string_table_ = std::make_shared<std::string*>(new_string_table);

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
    return Status(
        DB_UNEXPECTED_ERROR,
        "For the MVP, each table will only support up to 50,000 records. Please talk to richard@epsilla.com for your specific requirements.");
    // DoubleSize();
  }

  // Segment is modified.
  skip_sync_disk_.store(false);

  // Process the insert.
  size_t cursor = record_number_;
  for (auto i = 0; i < new_record_size; ++i) {
    auto record = records.GetArrayElement(i);
    for (auto& field : table_schema.fields_) {
      if (field.field_type_ == meta::FieldType::STRING) {
        // Insert string attribute.
        string_table_[cursor * string_num_ + field_id_mem_offset_map_[field.id_]] = record.GetString(field.name_);
      } else if (field.field_type_ == meta::FieldType::VECTOR_FLOAT ||
                 field.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
        // Insert vector attribute.
        auto vector = record.GetArray(field.name_);
        for (auto j = 0; j < field.vector_dimension_; ++j) {
          float value = static_cast<float>((float)(vector.GetArrayElement(j).GetDouble()));
          std::memcpy(&(vector_tables_[field_id_mem_offset_map_[field.id_]][cursor * vector_dims_[field_id_mem_offset_map_[field.id_]] + j]), &value, sizeof(float));
        }
      } else {
        // Insert primitive attribute.
        switch (field.field_type_) {
          case meta::FieldType::INT1: {
            int8_t value = static_cast<int8_t>((int8_t)(record.GetInt(field.name_)));
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int8_t));
            break;
          }
          case meta::FieldType::INT2: {
            int16_t value = static_cast<int16_t>((int16_t)(record.GetInt(field.name_)));
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int16_t));
            break;
          }
          case meta::FieldType::INT4: {
            int32_t value = static_cast<int32_t>((int32_t)(record.GetInt(field.name_)));
            std::memcpy(&(attribute_table_[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int32_t));
            break;
          }
          case meta::FieldType::INT8: {
            int64_t value = static_cast<int64_t>((int64_t)(record.GetInt(field.name_)));
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
  }
  record_number_.store(cursor);
  return Status::OK();
}

Status TableSegmentMVP::SaveTableSegment(meta::TableSchema& table_schema, const std::string& db_catalog_path) {
  if (skip_sync_disk_) {
    return Status::OK();
  }

  // Construct the file path
  std::string path = db_catalog_path + "/" + std::to_string(table_schema.id_) + "/data_mvp.bin";
  std::string tmp_path = path + ".tmp";

  std::ofstream file(tmp_path, std::ios::binary);
  if (!file) {
    return Status(DB_UNEXPECTED_ERROR, "Cannot open file: " + path);
  }

  // Write the number of records and the first record id
  file.write(reinterpret_cast<const char*>(&record_number_), sizeof(record_number_));
  file.write(reinterpret_cast<const char*>(&first_record_id_), sizeof(first_record_id_));

  // Write the biset
  int64_t bitset_size = deleted_->size();
  const uint8_t* bitset_data = deleted_->data();
  file.write(reinterpret_cast<const char*>(&bitset_size), sizeof(bitset_size));
  file.write(reinterpret_cast<const char*>(bitset_data), bitset_size);

  // // Write the attribute table.
  file.write(attribute_table_, record_number_ * primitive_offset_);

  // Write the string table.
  for (auto i = 0; i < record_number_; ++i) {
    for (auto j = 0; j < string_num_; ++j) {
      int64_t offset = i * string_num_ + j;
      int64_t string_length = string_table_[offset].size();
      file.write(reinterpret_cast<const char*>(&string_length), sizeof(string_length));
      file.write(string_table_[offset].c_str(), string_length);
    }
  }

  // Write the vector table.
  for (auto i = 0; i < vector_num_; ++i) {
    file.write(reinterpret_cast<const char*>(vector_tables_[i]), sizeof(float) * record_number_ * vector_dims_[i]);
  }

  // Close the file
  file.close();

  if (!file) {
    return Status(DB_UNEXPECTED_ERROR, "Failed to write to file: " + path);
  }

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
  std::cout << "string_num_: " << string_num_ << "\n";
  std::cout << "vector_num_: " << vector_num_ << "\n";

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
          field.field_type_ != meta::FieldType::VECTOR_FLOAT &&
          field.field_type_ != meta::FieldType::VECTOR_DOUBLE) {
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

  // Print out string_table_
  std::cout << "string_table_:  \n";
  for (size_t i = 0; i < record_number_; ++i) {
    for (size_t j = 0; j < string_num_; ++j) {
      size_t offset = i * string_num_ + j;
      std::cout << string_table_[offset] << ", ";
    }
    std::cout << "\n";
  }

  std::cout << "vector_dims_:  \n";
  for (const auto& dim : vector_dims_) {
    std::cout << dim << ", ";
  }
  std::cout << "\n";

  // Print out vector_tables_
  std::cout << "vector_tables_:  \n";
  for (size_t i = 0; i < vector_num_; ++i) {
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
    for (auto i = 0; i < vector_num_; ++i) {
      delete[] vector_tables_[i];
    }
    delete[] vector_tables_;
  }
  if (string_table_ != nullptr) {
    delete[] string_table_;
  }
  if (deleted_ != nullptr) {
    delete deleted_;
  }
}

}  // namespace engine
}  // namespace vectordb
