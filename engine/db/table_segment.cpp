#include "db/table_segment.hpp"

#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <functional>
#include <iostream>
#include <numeric>
#include <vector>

#include "utils/common_util.hpp"

namespace vectordb {
namespace engine {
// The number of bytes of a field value is stored in a continuous memory block.
constexpr size_t FieldTypeSize(meta::FieldType type) {
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
    case meta::FieldType::GEO_POINT:
      return 16;
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

Status TableSegment::Init(meta::TableSchema& table_schema, int64_t size_limit) {
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
      field_id_mem_offset_map_[field_schema.id_] = dense_vector_num_;
      field_name_mem_offset_map_[field_schema.name_] = dense_vector_num_;
      vec_field_name_executor_pool_idx_map_[field_schema.name_] = current_total_vec_num;
      dense_vector_num_++;
    } else {
      field_id_mem_offset_map_[field_schema.id_] = primitive_offset_;
      field_name_mem_offset_map_[field_schema.name_] = primitive_offset_;
      primitive_offset_ += FieldTypeSize(field_schema.field_type_);
      ++primitive_num_;
      if (field_schema.field_type_ == meta::FieldType::GEO_POINT) {
        geospatial_indices_[field_schema.name_] = std::make_shared<vectordb::engine::index::GeospatialIndex>();
      }
    }

    if (field_schema.is_primary_key_) {
      pk_field_idx_ = std::make_unique<int64_t>(field_schema.id_);
    }
  }

  // Use dynamic memory blocks with initial capacity
  // Use a reasonable initial capacity for attribute table, allowing dynamic growth
  // rather than pre-allocating based on vector_scale which is intended for vectors
  capacity_ = std::min(size_limit, static_cast<int64_t>(1000));  // Cap initial capacity at 1000 records

  // Allocate dynamic attribute table
  if (primitive_offset_ > 0) {
    size_t total_size = 0;
    if (__builtin_mul_overflow(capacity_, primitive_offset_, &total_size)) {
      return Status(DB_UNEXPECTED_ERROR, "Integer overflow in attribute table size calculation");
    }
    attribute_table_ = std::make_unique<DynamicMemoryBlock<char>>(total_size, GrowthStrategy::ADAPTIVE);
    // Initialize to zero
    std::memset(attribute_table_->GetData(), 0, total_size);
  }
  
  var_len_attr_table_.resize(var_len_attr_num_);
  for (auto& elem : var_len_attr_table_) {
    elem.resize(size_limit_);
  }

  // Allocate dynamic vector tables
  if (dense_vector_num_ > 0) {
    vector_tables_.resize(dense_vector_num_);
    for (auto i = 0; i < dense_vector_num_; ++i) {
      // Create dynamic vector block with initial capacity
      vector_tables_[i] = std::make_unique<DynamicVectorBlock>(
          capacity_,  // initial number of vectors
          vector_dims_[i],  // dimension per vector
          GrowthStrategy::ADAPTIVE
      );
    }
  }
  
  deleted_ = std::make_unique<ConcurrentBitset>(size_limit);

  return Status::OK();
}

Status TableSegment::Resize(size_t new_capacity) {
  // Check if critical operations are in progress
  if (index_rebuild_in_progress_.load() || compaction_in_progress_.load()) {
    logger_.Debug("[TableSegment] Resize deferred - critical operation in progress");
    return Status::OK();  // Defer resize, will be retried on next insert
  }

  std::unique_lock lock(data_rw_mutex_);

  if (new_capacity <= capacity_) {
    // No need to resize if new capacity is smaller or equal
    return Status::OK();
  }

  logger_.Info("Resizing TableSegment from capacity " + std::to_string(capacity_) +
              " to " + std::to_string(new_capacity));

  // Track resize progress for rollback
  size_t resize_step = 0;
  size_t old_capacity = capacity_;
  size_t old_size_limit = size_limit_;
  std::vector<size_t> resized_vector_tables;

  // Resize attribute table
  if (primitive_offset_ > 0 && attribute_table_) {
    size_t new_size = new_capacity * primitive_offset_;
    auto status = attribute_table_->Resize(new_size);
    if (!status.ok()) {
      logger_.Error("Failed to resize attribute table: " + status.message());
      return status;
    }
    resize_step = 1;
  }

  // Resize vector tables (with rollback capability)
  for (size_t i = 0; i < vector_tables_.size(); ++i) {
    if (vector_tables_[i]) {
      auto status = vector_tables_[i]->ResizeVectors(new_capacity);
      if (!status.ok()) {
        logger_.Error("Failed to resize vector table " + std::to_string(i) + ": " + status.message());

        // Rollback: Resize back the vector tables that were already resized
        for (size_t j : resized_vector_tables) {
          vector_tables_[j]->ResizeVectors(old_capacity);
        }

        // Rollback: Resize back the attribute table if it was resized
        if (resize_step >= 1 && attribute_table_) {
          attribute_table_->Resize(old_capacity * primitive_offset_);
        }

        return status;
      }
      resized_vector_tables.push_back(i);
    }
  }
  resize_step = 2;

  // Resize variable length attribute tables
  try {
    for (auto& var_table : var_len_attr_table_) {
      var_table.resize(new_capacity);
    }
    resize_step = 3;
  } catch (const std::exception& e) {
    logger_.Error("Failed to resize variable length tables: " + std::string(e.what()));

    // Rollback all previous operations
    // Rollback vector tables
    for (size_t i = 0; i < vector_tables_.size(); ++i) {
      if (vector_tables_[i]) {
        vector_tables_[i]->ResizeVectors(old_capacity);
      }
    }

    // Rollback attribute table
    if (primitive_offset_ > 0 && attribute_table_) {
      attribute_table_->Resize(old_capacity * primitive_offset_);
    }

    return Status(DB_UNEXPECTED_ERROR, "Failed to resize variable length tables: " + std::string(e.what()));
  }

  // Resize deleted bitset
  try {
    auto new_deleted = std::make_unique<ConcurrentBitset>(new_capacity);
    // Copy existing deleted bits
    for (size_t i = 0; i < record_number_; ++i) {
      if (deleted_->test(i)) {
        new_deleted->set(i);
      }
    }
    deleted_ = std::move(new_deleted);
    resize_step = 4;
  } catch (const std::exception& e) {
    logger_.Error("Failed to resize deleted bitset: " + std::string(e.what()));

    // Rollback all previous operations
    // Rollback variable length tables
    for (auto& var_table : var_len_attr_table_) {
      var_table.resize(old_capacity);
    }

    // Rollback vector tables
    for (size_t i = 0; i < vector_tables_.size(); ++i) {
      if (vector_tables_[i]) {
        vector_tables_[i]->ResizeVectors(old_capacity);
      }
    }

    // Rollback attribute table
    if (primitive_offset_ > 0 && attribute_table_) {
      attribute_table_->Resize(old_capacity * primitive_offset_);
    }

    return Status(DB_UNEXPECTED_ERROR, "Failed to resize deleted bitset: " + std::string(e.what()));
  }

  // Update capacity and size limit (final commit point)
  capacity_ = new_capacity;
  size_limit_ = new_capacity;

  // CRITICAL FIX: Reset capacity_manager_ to match the new capacity
  // Preserve the current record count (current_size)
  size_t current_records = record_number_.load();
  capacity_manager_ = std::make_unique<AtomicCapacityManager>(new_capacity);
  // Reserve space for existing records
  size_t dummy_pos;
  if (current_records > 0) {
    capacity_manager_->TryReserve(current_records, dummy_pos);
  }

  logger_.Info("TableSegment resize completed: capacity=" + std::to_string(new_capacity) +
               ", current_records=" + std::to_string(current_records));
  return Status::OK();
}

Status TableSegment::DoubleSize() {
  size_t new_capacity = capacity_ * 2;

  // Check for overflow
  if (new_capacity < capacity_) {
    return Status(DB_UNEXPECTED_ERROR, "Capacity overflow when doubling size");
  }

  return Resize(new_capacity);
}

TableSegment::TableSegment(meta::TableSchema& table_schema, int64_t init_table_scale, std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service)
    : skip_sync_disk_(false),
      size_limit_(init_table_scale),
      capacity_(init_table_scale),  // Initialize capacity
      first_record_id_(0),
      record_number_(0),
      field_name_mem_offset_map_(0),
      field_id_mem_offset_map_(0),
      primitive_num_(0),
      var_len_attr_num_(0),
      dense_vector_num_(0),
      capacity_manager_(std::make_unique<AtomicCapacityManager>(init_table_scale)),
      snapshot_manager_(std::make_unique<SnapshotManager>()),
      upsert_manager_(std::make_unique<AtomicUpsertManager>()),
      wal_manager_(std::make_unique<WALTransactionManager>()),
      expansion_cooldown_until_(std::chrono::steady_clock::now() - std::chrono::seconds(1)),
      sparse_vector_num_(0),
      vector_dims_(0) {
  embedding_service_ = embedding_service;

  // Initialize insertion rate monitor for predictive expansion
  insertion_monitor_ = std::make_unique<InsertionRateMonitor>(10);

  Init(table_schema, init_table_scale);
}

TableSegment::TableSegment(meta::TableSchema& table_schema, const std::string& db_catalog_path, int64_t init_table_scale, std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service)
    : skip_sync_disk_(true),
      size_limit_(init_table_scale),
      capacity_(init_table_scale),  // Initialize capacity
      first_record_id_(0),
      record_number_(0),
      field_name_mem_offset_map_(0),
      field_id_mem_offset_map_(0),
      primitive_num_(0),
      var_len_attr_num_(0),
      dense_vector_num_(0),
      capacity_manager_(std::make_unique<AtomicCapacityManager>(init_table_scale)),
      snapshot_manager_(std::make_unique<SnapshotManager>()),
      upsert_manager_(std::make_unique<AtomicUpsertManager>()),
      wal_manager_(std::make_unique<WALTransactionManager>()),
      expansion_cooldown_until_(std::chrono::steady_clock::now() - std::chrono::seconds(1)),
      sparse_vector_num_(0),
      vector_dims_(0),
      wal_global_id_(-1) {
  embedding_service_ = embedding_service;

  // Initialize insertion rate monitor for predictive expansion
  insertion_monitor_ = std::make_unique<InsertionRateMonitor>(10);

  // Init the containers.
  Init(table_schema, init_table_scale);
  std::string path = db_catalog_path + "/" + std::to_string(table_schema.id_) + "/data_mvp.bin";
  if (server::CommonUtil::IsFileExist(path)) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
      logger_.Error("Cannot open table segment file: " + path);
      // Continue with empty initialization instead of throwing
      Init(table_schema, init_table_scale);
      return;
    }

    // Read the number of records and the first record id
    file.read(reinterpret_cast<char*>(&record_number_), sizeof(record_number_));
    file.read(reinterpret_cast<char*>(&first_record_id_), sizeof(first_record_id_));
    // If the table contains more records than the size limit, adjust the scale
    if (record_number_ > init_table_scale) {
      logger_.Warning("Table contains " + std::to_string(record_number_) +
                     " records, larger than initial scale " + std::to_string(init_table_scale) +
                     ". Adjusting scale automatically.");
      // Adjust scale to accommodate existing data
      size_t new_scale = record_number_ * 2;  // Double for growth room
      Init(table_schema, new_scale);
      
      // Re-read from beginning
      file.clear();
      file.seekg(0);
      file.read(reinterpret_cast<char*>(&record_number_), sizeof(record_number_));
      file.read(reinterpret_cast<char*>(&first_record_id_), sizeof(first_record_id_));
    }

    // Read the bitset
    int64_t bitset_size = 0;
    file.read(reinterpret_cast<char*>(&bitset_size), sizeof(bitset_size));
    std::vector<uint8_t> bitset_data(bitset_size);
    file.read(reinterpret_cast<char*>(deleted_->data()), bitset_size);

    // Read the attribute table
    file.read(attribute_table_->GetData(), record_number_ * primitive_offset_);

    // add int pk into set
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
            std::memcpy(&value, &(attribute_table_->GetData()[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int8_t));
            // do not check existance to avoid overhead
            primary_key_.addKeyIfNotExist(value, rIdx);
            break;
          }
          case meta::FieldType::INT2: {
            int16_t value = 0;
            std::memcpy(&value, &(attribute_table_->GetData()[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int16_t));
            // do not check existance to avoid overhead
            primary_key_.addKeyIfNotExist(value, rIdx);
            break;
          }
          case meta::FieldType::INT4: {
            int32_t value = 0;
            std::memcpy(&value, &(attribute_table_->GetData()[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int32_t));
            // do not check existance to avoid overhead
            primary_key_.addKeyIfNotExist(value, rIdx);
            break;
          }
          case meta::FieldType::INT8: {
            int64_t value = 0;
            std::memcpy(&value, &(attribute_table_->GetData()[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int64_t));
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

    // Handle the geospatial indices
    for (auto& field : table_schema.fields_) {
      if (field.field_type_ == meta::FieldType::GEO_POINT) {
        auto index = geospatial_indices_[field.name_];
        for (auto rIdx = 0; rIdx < record_number_; rIdx++) {
          // skip deleted entry
          if (deleted_->test(rIdx)) {
            continue;
          }
          double lat = 0;
          std::memcpy(&lat, &(attribute_table_->GetData()[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(double));
          double lon = 0;
          std::memcpy(&lon, &(attribute_table_->GetData()[rIdx * primitive_offset_ + field_id_mem_offset_map_[field.id_] + sizeof(double)]), sizeof(double));
          index->insertPoint(lat, lon, rIdx);
        }
      }
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
            var_len_attr_table_[attrIdx][recordIdx] = str;
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
            auto v = std::make_shared<SparseVector>(dataLen / sizeof(SparseVectorElement));
            file.read(reinterpret_cast<char*>(v->data()), dataLen);
            var_len_attr_table_[attrIdx][recordIdx] = std::move(v);
            break;
        }
      }
    }

    // Read the vector table
    for (auto i = 0; i < dense_vector_num_; ++i) {
      file.read(reinterpret_cast<char*>(vector_tables_[i]->GetData()), sizeof(float) * record_number_ * vector_dims_[i]);
    }

    // Last, read the global wal id.
    file.read(reinterpret_cast<char*>(&wal_global_id_), sizeof(wal_global_id_));

    // Close the file
    file.close();

    // CRITICAL FIX: After loading data from disk, reset capacity_manager_ to match loaded state
    // The capacity_manager_ was initialized in constructor with init_table_scale and current_size=0
    // But we just loaded record_number_ records, so we need to update it
    capacity_manager_ = std::make_unique<AtomicCapacityManager>(capacity_);
    size_t dummy_pos;
    if (record_number_ > 0) {
      capacity_manager_->TryReserve(record_number_, dummy_pos);
    }
    logger_.Info("Loaded table segment: capacity=" + std::to_string(capacity_) +
                 ", record_number=" + std::to_string(record_number_) +
                 ", capacity_manager reset");
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

bool TableSegment::isStringPK() const {
  return string_pk_offset_ != nullptr;
}

bool TableSegment::isIntPK() const {
  return pk_field_idx_ && !string_pk_offset_;
}

int64_t TableSegment::pkFieldIdx() const {
  return *pk_field_idx_;
}

meta::FieldSchema TableSegment::pkField() const {
  return schema.fields_[*pk_field_idx_];
}

meta::FieldType TableSegment::pkType() const {
  return schema.fields_[*pk_field_idx_].field_type_;
}

bool TableSegment::isEntryDeleted(int64_t id) const {
  return deleted_->test(id);
}

Status TableSegment::Delete(Json& records, std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes, int64_t wal_id) {
  // Check if hard delete is enabled via configuration
  bool use_soft_delete = vectordb::globalConfig.SoftDelete.load(std::memory_order_acquire);
  
  if (!use_soft_delete) {
#ifdef VECTORDB_DEBUG_BUILD
    logger_.Debug("[TableSegment] Hard delete mode enabled, using HardDelete method");
#endif
    return HardDelete(records, filter_nodes, wal_id);
  }

#ifdef VECTORDB_DEBUG_BUILD
  logger_.Debug("[TableSegment] Soft delete mode enabled, using standard delete (mark as deleted)");
#endif
  std::unique_lock<std::shared_mutex> lock(data_rw_mutex_);

  // Log deletion request at segment level
  size_t pk_list_size = records.GetSize();
  bool has_filter = !filter_nodes.empty();

  // Only log deletion summary for large batches or in debug mode
#ifdef VECTORDB_DEBUG_BUILD
  logger_.Debug("[TableSegment] Starting deletion in segment, wal_id=" + std::to_string(wal_id) +
               ", primaryKeys=" + std::to_string(pk_list_size) + " items" +
               (has_filter ? ", with_filter=true" : ", with_filter=false"));
#else
  // In production, only log significant batches
  if (pk_list_size > 100) {
    logger_.Info("[TableSegment] Batch deletion: wal_id=" + std::to_string(wal_id) +
                ", count=" + std::to_string(pk_list_size));
  }
#endif

  wal_global_id_ = wal_id;
  size_t deleted_record = 0;
  vectordb::query::expr::ExprEvaluator expr_evaluator(
      filter_nodes,
      field_name_mem_offset_map_,
      primitive_offset_,
      var_len_attr_num_,
      attribute_table_->GetData(),
      var_len_attr_table_);
  int filter_root_index = filter_nodes.size() - 1;

  if (pk_list_size > 0) {
    // Delete by the pk list.
#ifdef VECTORDB_DEBUG_BUILD
    logger_.Debug("[TableSegment] Deleting by primary key list (" + std::to_string(pk_list_size) + " keys)");
#endif
    
    // Track deletion statistics for batch logging
    size_t successfully_deleted = 0;
    size_t failed_deletions = 0;

    for (auto i = 0; i < pk_list_size; ++i) {
      auto pkField = records.GetArrayElement(i);
      bool item_deleted = false;

      if (isIntPK()) {
        auto pk = pkField.GetInt();
        switch (pkType()) {
          case meta::FieldType::INT1:
            item_deleted = DeleteByIntPK(static_cast<int8_t>(pk), expr_evaluator, filter_root_index).ok();
#ifdef VECTORDB_DEBUG_BUILD
            if (item_deleted) {
              logger_.Debug("[TableSegment] Deleted record with INT1 pk=" + std::to_string(pk));
            } else {
              logger_.Debug("[TableSegment] Record with INT1 pk=" + std::to_string(pk) + " not found or skipped by filter");
            }
#endif
            break;
          case meta::FieldType::INT2:
            item_deleted = DeleteByIntPK(static_cast<int16_t>(pk), expr_evaluator, filter_root_index).ok();
#ifdef VECTORDB_DEBUG_BUILD
            if (item_deleted) {
              logger_.Debug("[TableSegment] Deleted record with INT2 pk=" + std::to_string(pk));
            } else {
              logger_.Debug("[TableSegment] Record with INT2 pk=" + std::to_string(pk) + " not found or skipped by filter");
            }
#endif
            break;
          case meta::FieldType::INT4:
            item_deleted = DeleteByIntPK(static_cast<int32_t>(pk), expr_evaluator, filter_root_index).ok();
#ifdef VECTORDB_DEBUG_BUILD
            if (item_deleted) {
              logger_.Debug("[TableSegment] Deleted record with INT4 pk=" + std::to_string(pk));
            } else {
              logger_.Debug("[TableSegment] Record with INT4 pk=" + std::to_string(pk) + " not found or skipped by filter");
            }
#endif
            break;
          case meta::FieldType::INT8:
            item_deleted = DeleteByIntPK(static_cast<int64_t>(pk), expr_evaluator, filter_root_index).ok();
#ifdef VECTORDB_DEBUG_BUILD
            if (item_deleted) {
              logger_.Debug("[TableSegment] Deleted record with INT8 pk=" + std::to_string(pk));
            } else {
              logger_.Debug("[TableSegment] Record with INT8 pk=" + std::to_string(pk) + " not found or skipped by filter");
            }
#endif
            break;
        }
      } else if (isStringPK()) {
        auto pk = pkField.GetString();
        item_deleted = DeleteByStringPK(pk, expr_evaluator, filter_root_index).ok();
#ifdef VECTORDB_DEBUG_BUILD
        if (item_deleted) {
          logger_.Debug("[TableSegment] Deleted record with STRING pk=" + pk);
        } else {
          logger_.Debug("[TableSegment] Record with STRING pk=" + pk + " not found or skipped by filter");
        }
#endif
      }

      if (item_deleted) {
        deleted_record++;
        successfully_deleted++;
      } else {
        failed_deletions++;
      }
    }

    // Log batch summary instead of individual operations in production
#ifndef DEBUG
    if (pk_list_size > 0) {
      logger_.Info("[TableSegment] Batch deletion completed: requested=" + std::to_string(pk_list_size) +
                  ", deleted=" + std::to_string(successfully_deleted) +
                  ", not_found=" + std::to_string(failed_deletions));
    }
#endif
  } else {
    // Delete by scanning the whole segment.
    logger_.Debug("[TableSegment] Deleting by scanning entire segment (filter-only deletion), total_records=" + 
                 std::to_string(record_number_));
    
    for (auto id = 0; id < record_number_; ++id) {
      bool item_deleted = false;
      
      if (isIntPK()) {
        auto offset = field_id_mem_offset_map_[pkFieldIdx()] + id * primitive_offset_;
        switch (pkType()) {
          case meta::FieldType::INT1: {
            int8_t pk;
            std::memcpy(&pk, &(attribute_table_->GetData()[offset]), sizeof(int8_t));
            item_deleted = DeleteByIntPK(pk, expr_evaluator, filter_root_index).ok();
            break;
          }
          case meta::FieldType::INT2: {
            int16_t pk;
            std::memcpy(&pk, &(attribute_table_->GetData()[offset]), sizeof(int16_t));
            item_deleted = DeleteByIntPK(pk, expr_evaluator, filter_root_index).ok();
            break;
          }
          case meta::FieldType::INT4: {
            int32_t pk;
            std::memcpy(&pk, &(attribute_table_->GetData()[offset]), sizeof(int32_t));
            item_deleted = DeleteByIntPK(pk, expr_evaluator, filter_root_index).ok();
            break;
          }
          case meta::FieldType::INT8: {
            int64_t pk;
            std::memcpy(&pk, &(attribute_table_->GetData()[offset]), sizeof(int64_t));
            item_deleted = DeleteByIntPK(pk, expr_evaluator, filter_root_index).ok();
            break;
          }
        }
      } else if (isStringPK()) {
        auto pk = std::get<std::string>(var_len_attr_table_[field_id_mem_offset_map_[pkFieldIdx()]][id]);
        item_deleted = DeleteByStringPK(pk, expr_evaluator, filter_root_index).ok();
      } else {
        item_deleted = DeleteByID(id, expr_evaluator, filter_root_index).ok();
      }
      
      if (item_deleted) {
        deleted_record++;
      }
    }
  }
  
  // Log deletion summary
  logger_.Debug("[TableSegment] Deletion completed: deleted=" + std::to_string(deleted_record) + 
               " records, current_deletion_ratio=" + std::to_string(GetDeletedRatio()));
  
  // Segment is modified.
  skip_sync_disk_.store(false);
  return Status(DB_SUCCESS, "{\"deleted\": " + std::to_string(deleted_record) + "}");
}

// Convert a primary key to an internal id
bool TableSegment::PK2ID(Json& record, size_t& id) {
  if (isIntPK()) {
    auto fieldType = pkType();
    auto pk = record.GetInt();
    switch (pkType()) {
      case meta::FieldType::INT1:
        return primary_key_.getKeyWithLock(static_cast<int8_t>(pk), id);
      case meta::FieldType::INT2:
        return primary_key_.getKeyWithLock(static_cast<int16_t>(pk), id);
      case meta::FieldType::INT4:
        return primary_key_.getKeyWithLock(static_cast<int32_t>(pk), id);
      case meta::FieldType::INT8:
        return primary_key_.getKeyWithLock(static_cast<int64_t>(pk), id);
    }
  } else if (isStringPK()) {
    auto pk = record.GetString();
    return primary_key_.getKeyWithLock(pk, id);
  }
  return false;
}

Status TableSegment::DeleteByStringPK(
    const std::string& pk,
    vectordb::query::expr::ExprEvaluator& evaluator,
    int filter_root_index) {
  size_t result = 0;
  auto found = primary_key_.getKey(pk, result);
  if (found) {
    if (evaluator.LogicalEvaluate(filter_root_index, result)) {
      deleted_->set(result);
      primary_key_.removeKey(pk);
      logger_.Debug("[TableSegment] Marked record id=" + std::to_string(result) + 
                   " as deleted in bitset and removed STRING pk=" + pk + " from index");
      return Status::OK();
    } else {
      logger_.Debug("[TableSegment] Record with STRING pk=" + pk + 
                   " found at id=" + std::to_string(result) + " but skipped by filter");
    }
  } else {
    logger_.Debug("[TableSegment] Record with STRING pk=" + pk + " not found in primary key index");
  }
  return Status(RECORD_NOT_FOUND, "Record with primary key not exist or skipped by filter: " + pk);
}

Status TableSegment::DeleteByID(
    const size_t id,
    vectordb::query::expr::ExprEvaluator& evaluator,
    int filter_root_index) {
  // Caller needs to guarantee the id is within record range.
  if (!deleted_->test(id) && evaluator.LogicalEvaluate(filter_root_index, id)) {
    deleted_->set(id);
    return Status::OK();
  }
  return Status(RECORD_NOT_FOUND, "Record skipped by filter: " + std::to_string(id));
}

// Concurrency-Safe Hard Delete Implementation
Status TableSegment::HardDelete(Json& records, std::vector<vectordb::query::expr::ExprNodePtr>& filter_nodes, int64_t wal_id) {
  // Use exclusive lock to prevent any concurrent access during hard delete
  std::unique_lock<std::shared_mutex> lock(data_rw_mutex_);

  // Log deletion request at segment level
  size_t pk_list_size = records.GetSize();
  bool has_filter = !filter_nodes.empty();
  
  logger_.Debug("[TableSegment] Starting CONCURRENT-SAFE HARD deletion in segment, wal_id=" + std::to_string(wal_id) + 
               ", primaryKeys=" + std::to_string(pk_list_size) + " items" +
               (has_filter ? ", with_filter=true" : ", with_filter=false"));

  wal_global_id_ = wal_id;
  std::vector<size_t> records_to_delete; // Collect IDs to delete
  
  vectordb::query::expr::ExprEvaluator expr_evaluator(
      filter_nodes,
      field_name_mem_offset_map_,
      primitive_offset_,
      var_len_attr_num_,
      attribute_table_->GetData(),
      var_len_attr_table_);
  int filter_root_index = filter_nodes.size() - 1;

  // Phase 1: Collect all record IDs to delete (no modifications yet)
  if (pk_list_size > 0) {
    logger_.Debug("[TableSegment] Collecting records to hard delete by primary key list");
    
    for (auto i = 0; i < pk_list_size; ++i) {
      auto pkField = records.GetArrayElement(i);
      size_t record_id = 0;
      bool found = false;
      
      if (isIntPK()) {
        auto pk = pkField.GetInt();
        switch (pkType()) {
          case meta::FieldType::INT1:
            found = primary_key_.getKey(static_cast<int8_t>(pk), record_id);
            break;
          case meta::FieldType::INT2:
            found = primary_key_.getKey(static_cast<int16_t>(pk), record_id);
            break;
          case meta::FieldType::INT4:
            found = primary_key_.getKey(static_cast<int32_t>(pk), record_id);
            break;
          case meta::FieldType::INT8:
            found = primary_key_.getKey(static_cast<int64_t>(pk), record_id);
            break;
        }
      } else if (isStringPK()) {
        auto pk = pkField.GetString();
        found = primary_key_.getKey(pk, record_id);
      }
      
      if (found && !deleted_->test(record_id)) {
        if (expr_evaluator.LogicalEvaluate(filter_root_index, record_id)) {
          records_to_delete.push_back(record_id);
        }
      }
    }
  } else {
    logger_.Debug("[TableSegment] Collecting records to hard delete by filter");
    
    // Collect record IDs to delete by filter
    for (size_t id = 0; id < record_number_.load(); ++id) {
      if (!deleted_->test(id)) { // Skip already deleted records
        if (expr_evaluator.LogicalEvaluate(filter_root_index, id)) {
          records_to_delete.push_back(id);
        }
      }
    }
  }
  
  if (records_to_delete.empty()) {
    logger_.Debug("[TableSegment] No records found to delete");
    return Status::OK();
  }
  
  // Phase 2: Sort IDs in descending order to avoid index shifting issues
  std::sort(records_to_delete.begin(), records_to_delete.end(), std::greater<size_t>());
  
  logger_.Debug("[TableSegment] Hard deleting " + std::to_string(records_to_delete.size()) + 
               " records in batch mode for concurrency safety");
  
  // Phase 3: Perform batch hard delete with proper concurrency control
  return BatchHardDelete(records_to_delete);
}

// Batch Hard Delete - Concurrent-safe implementation 
Status TableSegment::BatchHardDelete(const std::vector<size_t>& sorted_ids) {
  // sorted_ids should be in descending order to avoid index shifting issues
  
  if (sorted_ids.empty()) {
    return Status::OK();
  }
  
  logger_.Debug("[TableSegment] Starting batch hard delete of " + std::to_string(sorted_ids.size()) + " records");
  
  // Phase 1: Mark records as deleted first (provides atomicity with soft delete fallback)
  for (size_t id : sorted_ids) {
    deleted_->set(id);
  }
  
  // Phase 2: Remove from primary key index (do this before data movement)
  for (size_t id : sorted_ids) {
    // Remove primary key mapping
    if (isStringPK()) {
      auto pk = std::get<std::string>(var_len_attr_table_[field_id_mem_offset_map_[pkFieldIdx()]][id]);
      primary_key_.removeKey(pk);
      logger_.Debug("[TableSegment] Removed STRING pk=" + pk + " from index for record id=" + std::to_string(id));
    } else if (isIntPK()) {
      size_t pk_offset = primitive_offset_ + field_id_mem_offset_map_[pkFieldIdx()];
      switch (pkType()) {
        case meta::FieldType::INT1: {
          int8_t pk;
          std::memcpy(&pk, &(attribute_table_->GetData()[pk_offset + id * primitive_offset_]), sizeof(int8_t));
          primary_key_.removeKey(pk);
          logger_.Debug("[TableSegment] Removed INT8 pk=" + std::to_string(pk) + " from index for record id=" + std::to_string(id));
          break;
        }
        case meta::FieldType::INT2: {
          int16_t pk;
          std::memcpy(&pk, &(attribute_table_->GetData()[pk_offset + id * primitive_offset_]), sizeof(int16_t));
          primary_key_.removeKey(pk);
          logger_.Debug("[TableSegment] Removed INT16 pk=" + std::to_string(pk) + " from index for record id=" + std::to_string(id));
          break;
        }
        case meta::FieldType::INT4: {
          int32_t pk;
          std::memcpy(&pk, &(attribute_table_->GetData()[pk_offset + id * primitive_offset_]), sizeof(int32_t));
          primary_key_.removeKey(pk);
          logger_.Debug("[TableSegment] Removed INT32 pk=" + std::to_string(pk) + " from index for record id=" + std::to_string(id));
          break;
        }
        case meta::FieldType::INT8: {
          int64_t pk;
          std::memcpy(&pk, &(attribute_table_->GetData()[pk_offset + id * primitive_offset_]), sizeof(int64_t));
          primary_key_.removeKey(pk);
          logger_.Debug("[TableSegment] Removed INT64 pk=" + std::to_string(pk) + " from index for record id=" + std::to_string(id));
          break;
        }
      }
    }
  }
  
  // Phase 3: Compact data structures in one pass to minimize data movement
  Status compact_status = CompactDataStructures(sorted_ids);
  if (!compact_status.ok()) {
    logger_.Error("[TableSegment] Data compaction failed: " + compact_status.message());
    return compact_status;
  }
  
  // Phase 4: Update record count atomically
  record_number_.fetch_sub(sorted_ids.size(), std::memory_order_release);
  
  // Phase 5: Rebuild primary key index with new IDs
  RebuildPrimaryKeyIndex();
  
  logger_.Debug("[TableSegment] Batch hard delete completed successfully. New record count: " + 
               std::to_string(record_number_.load()));
  
  return Status::OK();
}

// Efficiently compact data structures by removing multiple records in one pass
Status TableSegment::CompactDataStructures(const std::vector<size_t>& sorted_ids) {
  // sorted_ids are in descending order
  size_t total_records = record_number_.load();
  
  logger_.Debug("[TableSegment] Compacting data structures, removing " + std::to_string(sorted_ids.size()) + 
               " records from " + std::to_string(total_records) + " total records");
  
  // Convert to ascending order for easier processing
  std::vector<size_t> ascending_ids = sorted_ids;
  std::sort(ascending_ids.begin(), ascending_ids.end());
  
  // Calculate new positions for each record after compaction
  std::vector<size_t> new_positions(total_records);
  size_t del_idx = 0;
  size_t new_pos = 0;
  
  for (size_t old_pos = 0; old_pos < total_records; ++old_pos) {
    if (del_idx < ascending_ids.size() && old_pos == ascending_ids[del_idx]) {
      // This record will be deleted
      del_idx++;
      new_positions[old_pos] = SIZE_MAX; // Mark as deleted
    } else {
      new_positions[old_pos] = new_pos++;
    }
  }
  
  // Compact attribute data
  size_t record_size = primitive_offset_;
  char* attr_data = attribute_table_->GetData();
  size_t write_pos = 0;
  
  for (size_t read_pos = 0; read_pos < total_records; ++read_pos) {
    if (new_positions[read_pos] != SIZE_MAX) {
      if (write_pos != read_pos) {
        std::memmove(attr_data + write_pos * record_size,
                     attr_data + read_pos * record_size,
                     record_size);
      }
      write_pos++;
    }
  }
  
  // Compact vector data for each vector field
  for (size_t v = 0; v < vector_tables_.size() && v < vector_dims_.size(); ++v) {
    if (vector_tables_[v]) {
      size_t vec_dim = vector_dims_[v];
      float* vec_data = vector_tables_[v]->GetData();
      size_t write_pos = 0;
      
      for (size_t read_pos = 0; read_pos < total_records; ++read_pos) {
        if (new_positions[read_pos] != SIZE_MAX) {
          if (write_pos != read_pos) {
            std::memmove(vec_data + write_pos * vec_dim,
                         vec_data + read_pos * vec_dim,
                         vec_dim * sizeof(float));
          }
          write_pos++;
        }
      }
    }
  }
  
  // Compact variable length attributes - simplified approach
  for (auto& var_len_table : var_len_attr_table_) {
    // Process in reverse order to maintain indices
    for (auto it = ascending_ids.rbegin(); it != ascending_ids.rend(); ++it) {
      size_t id_to_remove = *it;
      if (id_to_remove < var_len_table.size()) {
        var_len_table.erase(var_len_table.begin() + id_to_remove);
      }
    }
  }
  
  logger_.Debug("[TableSegment] Data compaction completed successfully");
  return Status::OK();
}

// Rebuild primary key index after data compaction
void TableSegment::RebuildPrimaryKeyIndex() {
  logger_.Debug("[TableSegment] Rebuilding primary key index after hard delete");

  // Set flag to prevent resize during index rebuild
  index_rebuild_in_progress_.store(true);
  
  // Clear existing index
  primary_key_.clear();
  
  size_t current_records = record_number_.load();
  
  // Rebuild index with new record IDs
  for (size_t id = 0; id < current_records; ++id) {
    if (!deleted_->test(id)) {
      if (isStringPK()) {
        auto pk = std::get<std::string>(var_len_attr_table_[field_id_mem_offset_map_[pkFieldIdx()]][id]);
        primary_key_.addKeyIfNotExist(pk, id);
      } else if (isIntPK()) {
        size_t pk_offset = primitive_offset_ + field_id_mem_offset_map_[pkFieldIdx()];
        switch (pkType()) {
          case meta::FieldType::INT1: {
            int8_t pk;
            std::memcpy(&pk, &(attribute_table_->GetData()[pk_offset + id * primitive_offset_]), sizeof(int8_t));
            primary_key_.addKeyIfNotExist(pk, id);
            break;
          }
          case meta::FieldType::INT2: {
            int16_t pk;
            std::memcpy(&pk, &(attribute_table_->GetData()[pk_offset + id * primitive_offset_]), sizeof(int16_t));
            primary_key_.addKeyIfNotExist(pk, id);
            break;
          }
          case meta::FieldType::INT4: {
            int32_t pk;
            std::memcpy(&pk, &(attribute_table_->GetData()[pk_offset + id * primitive_offset_]), sizeof(int32_t));
            primary_key_.addKeyIfNotExist(pk, id);
            break;
          }
          case meta::FieldType::INT8: {
            int64_t pk;
            std::memcpy(&pk, &(attribute_table_->GetData()[pk_offset + id * primitive_offset_]), sizeof(int64_t));
            primary_key_.addKeyIfNotExist(pk, id);
            break;
          }
        }
      }
    }
  }
  
  logger_.Debug("[TableSegment] Primary key index rebuilt successfully");

  // Clear index rebuild flag
  index_rebuild_in_progress_.store(false);
}

// Legacy method - now redirects to safe batch version
Status TableSegment::SafeHardDeleteByID(size_t id) {
  if (id >= record_number_.load() || deleted_->test(id)) {
    return Status(RECORD_NOT_FOUND, "Record not found or already deleted: " + std::to_string(id));
  }
  
  // Use batch delete for concurrency safety even for single record
  std::vector<size_t> single_id = {id};
  return BatchHardDelete(single_id);
}

Status TableSegment::HardDeleteByStringPK(
    const std::string& pk,
    vectordb::query::expr::ExprEvaluator& evaluator,
    int filter_root_index) {
  size_t result = 0;
  auto found = primary_key_.getKey(pk, result);
  if (found) {
    if (evaluator.LogicalEvaluate(filter_root_index, result)) {
      return SafeHardDeleteByID(result); // Use safe concurrent version
    } else {
      logger_.Debug("[TableSegment] Record with STRING pk=" + pk + 
                   " found at id=" + std::to_string(result) + " but skipped by filter");
    }
  } else {
    logger_.Debug("[TableSegment] Record with STRING pk=" + pk + " not found in primary key index");
  }
  return Status(RECORD_NOT_FOUND, "Record with primary key not exist or skipped by filter: " + pk);
}

Status TableSegment::Insert(meta::TableSchema& table_schema, Json& records, int64_t wal_id, std::unordered_map<std::string, std::string> &headers, bool upsert) {
  // === INSERTION OPERATION START - DETAILED LOGGING ===
  auto insert_start_time = std::chrono::steady_clock::now();
  std::thread::id thread_id = std::this_thread::get_id();

  logger_.Info("INSERT_START: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
               ", wal_id=" + std::to_string(wal_id) +
               ", requested_records=" + std::to_string(records.GetSize()) +
               ", current_capacity=" + std::to_string(capacity_) +
               ", current_records=" + std::to_string(record_number_.load()) +
               ", compaction_in_progress=" + (compaction_in_progress_.load() ? "true" : "false"));

  // Use write lock for insert operations
  std::unique_lock<std::shared_mutex> lock(data_rw_mutex_);
  auto lock_acquired_time = std::chrono::steady_clock::now();
  auto lock_wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(lock_acquired_time - insert_start_time).count();

  logger_.Info("INSERT_LOCK_ACQUIRED: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
               ", lock_wait_ms=" + std::to_string(lock_wait_ms));

  size_t new_record_size = records.GetSize();
  if (new_record_size == 0) {
    logger_.Debug("INSERT_EMPTY: No records to insert.");
    return Status::OK();
  }

  // Record insertion for rate monitoring
  if (insertion_monitor_) {
    insertion_monitor_->RecordInsertion(new_record_size);

    // Check if predictive expansion is recommended
    // Also check cooldown period to prevent immediate expansion after restart
    auto now = std::chrono::steady_clock::now();
    if (predictive_expansion_enabled_.load() &&
        now > expansion_cooldown_until_ &&
        insertion_monitor_->ShouldPreExpand(record_number_.load(), capacity_, 30.0)) {

      // Calculate recommended capacity based on predicted growth
      size_t recommended_capacity = insertion_monitor_->GetRecommendedCapacity(capacity_);

      logger_.Info("Predictive expansion triggered: expanding from " +
                  std::to_string(capacity_) + " to " +
                  std::to_string(recommended_capacity) +
                  " based on insertion rate of " +
                  std::to_string(insertion_monitor_->GetCurrentRate()) + " records/sec");

      // Perform pre-emptive resize
      auto resize_status = Resize(recommended_capacity);
      if (!resize_status.ok()) {
        logger_.Warning("Predictive expansion failed: " + resize_status.message());
        // Continue with normal operation even if predictive expansion fails
      }
    }
  }

  // Check if the records are valid.
  for (auto i = 0; i < new_record_size; ++i) {
    auto record = records.GetArrayElement(i);
    for (auto& field : table_schema.fields_) {
      // Index fields are generated by calling embedding service.
      if (!field.is_index_field_ && !record.HasMember(field.name_)) {
        return Status(INVALID_RECORD, "Record " + std::to_string(i) + " missing field: " + field.name_);
      }
    }
  }

  // Check if we need to resize
  size_t reserved_position;
  logger_.Info("INSERT_CAPACITY_CHECK: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
               ", trying_to_reserve=" + std::to_string(new_record_size) +
               ", current_capacity=" + std::to_string(capacity_) +
               ", current_records=" + std::to_string(record_number_.load()));

  if (!capacity_manager_->TryReserve(new_record_size, reserved_position)) {
    logger_.Info("INSERT_RESIZE_NEEDED: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                 ", reservation_failed, calculating_new_capacity");

    // Need to resize - calculate new capacity
    size_t required_capacity = record_number_ + new_record_size;
    size_t new_capacity = capacity_;

    // Use adaptive growth strategy
    while (new_capacity < required_capacity) {
      if (new_capacity < 10000) {
        new_capacity *= 2;  // Double for small sizes
      } else if (new_capacity < 100000) {
        new_capacity = static_cast<size_t>(new_capacity * 1.5);  // 1.5x for medium
      } else {
        new_capacity += 100000;  // Linear for large sizes
      }
    }

    logger_.Info("INSERT_RESIZE_ATTEMPT: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                 ", old_capacity=" + std::to_string(capacity_) +
                 ", new_capacity=" + std::to_string(new_capacity));

    // Attempt resize
    auto resize_status = Resize(new_capacity);
    if (!resize_status.ok()) {
      logger_.Error("INSERT_RESIZE_FAILED: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                    ", error=" + resize_status.message());
      return Status(DB_UNEXPECTED_ERROR,
                   "Failed to dynamically resize table segment: " + resize_status.message());
    }

    logger_.Info("INSERT_RESIZE_SUCCESS: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                 ", new_capacity=" + std::to_string(new_capacity));

    // Try reserve again after resize
    if (!capacity_manager_->TryReserve(new_record_size, reserved_position)) {
      logger_.Error("INSERT_RESERVE_FAILED_AFTER_RESIZE: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)));
      return Status(DB_UNEXPECTED_ERROR,
                   "Failed to reserve capacity even after resize");
    }
  }

  logger_.Info("INSERT_CAPACITY_RESERVED: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
               ", reserved_position=" + std::to_string(reserved_position) +
               ", reserved_count=" + std::to_string(new_record_size));

  // CONSERVATIVE FIX: Only resize vector tables when it's safe to do so
  // This fixes the race condition while respecting compaction operations
  if (!compaction_in_progress_.load()) {
    size_t required_vectors = reserved_position + new_record_size;

    logger_.Info("INSERT_VECTOR_TABLE_CHECK: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                 ", required_vectors=" + std::to_string(required_vectors) +
                 ", reserved_position=" + std::to_string(reserved_position) +
                 ", new_record_size=" + std::to_string(new_record_size));

    // CRITICAL: Check for capacity tracking inconsistency
    // Only cap when required_vectors is unreasonably large compared to current records
    // This prevents issues after compaction while allowing legitimate expansion
    size_t current_records = record_number_.load();
    if (required_vectors > capacity_ && required_vectors > current_records * 10) {
      logger_.Error("INSERT_VECTOR_TABLE_INCONSISTENCY: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                   ", required_vectors=" + std::to_string(required_vectors) +
                   ", capacity=" + std::to_string(capacity_) +
                   ", current_records=" + std::to_string(current_records) +
                   " - This indicates a capacity tracking inconsistency.");
      required_vectors = std::max(capacity_, current_records + new_record_size);  // Use reasonable size
      logger_.Info("INSERT_VECTOR_TABLE_CORRECTED: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                   ", corrected_required_vectors=" + std::to_string(required_vectors));
    }

    for (size_t i = 0; i < vector_tables_.size(); ++i) {
      if (!vector_tables_[i]) {
        logger_.Error("INSERT_VECTOR_TABLE_NULL: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                     ", table_index=" + std::to_string(i) + " - Vector table is null!");
        continue;
      }

      // CRITICAL FIX: Use GetCapacity() to get physical capacity, not GetNumVectors()
      // After compaction, num_vectors_ may be set to actual data count (e.g., 24)
      // while physical capacity is much larger (e.g., 500) for growth headroom
      // GetNumVectors() returns num_vectors_ (logical count), not physical capacity
      size_t physical_capacity_floats = 0;
      size_t dimension = 0;

      logger_.Info("INSERT_ACCESSING_VECTOR_TABLE: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                   ", table_index=" + std::to_string(i) + ", about to call GetCapacity()...");

      try {
        physical_capacity_floats = vector_tables_[i]->GetCapacity();
        logger_.Info("INSERT_GOT_CAPACITY: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                     ", table_index=" + std::to_string(i) + ", capacity=" + std::to_string(physical_capacity_floats) +
                     ", about to call GetDimension()...");

        dimension = vector_tables_[i]->GetDimension();
        logger_.Info("INSERT_GOT_DIMENSION: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                     ", table_index=" + std::to_string(i) + ", dimension=" + std::to_string(dimension));
      } catch (const std::exception& e) {
        logger_.Error("INSERT_VECTOR_TABLE_ACCESS_ERROR: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                     ", table_index=" + std::to_string(i) + ", error=" + std::string(e.what()));
        return Status(DB_UNEXPECTED_ERROR, "Failed to access vector table: " + std::string(e.what()));
      }

      if (dimension == 0) {
        logger_.Error("INSERT_VECTOR_TABLE_ZERO_DIM: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                     ", table_index=" + std::to_string(i) + " - Dimension is zero!");
        return Status(DB_UNEXPECTED_ERROR, "Vector table has zero dimension");
      }

      size_t current_vector_capacity = physical_capacity_floats / dimension;

      if (current_vector_capacity < required_vectors) {
        logger_.Info("INSERT_VECTOR_TABLE_RESIZE_ATTEMPT: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                     ", table_index=" + std::to_string(i) +
                     ", current_capacity=" + std::to_string(current_vector_capacity) +
                     ", required=" + std::to_string(required_vectors));

        auto resize_start = std::chrono::steady_clock::now();
        auto resize_status = vector_tables_[i]->ResizeVectors(required_vectors);
        auto resize_end = std::chrono::steady_clock::now();
        auto resize_ms = std::chrono::duration_cast<std::chrono::milliseconds>(resize_end - resize_start).count();

        if (!resize_status.ok()) {
          logger_.Error("INSERT_VECTOR_TABLE_RESIZE_FAILED: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                       ", table_index=" + std::to_string(i) +
                       ", error=" + resize_status.message() +
                       ", resize_duration_ms=" + std::to_string(resize_ms));
          return Status(DB_UNEXPECTED_ERROR,
                       "Vector table resize failed: " + resize_status.message());
        }

        logger_.Info("INSERT_VECTOR_TABLE_RESIZE_SUCCESS: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                     ", table_index=" + std::to_string(i) +
                     ", new_capacity=" + std::to_string(required_vectors) +
                     ", resize_duration_ms=" + std::to_string(resize_ms));
      }
    }
  } else {
    logger_.Info("INSERT_VECTOR_TABLE_SKIPPED: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
                 " - Skipping vector table resize during compaction");
  }

  // Start WAL transaction
  auto wal_txn = wal_manager_->BeginTransaction(wal_id);
  wal_manager_->SetRollback(wal_txn.get(), [this, count = new_record_size]() {
    capacity_manager_->ReleaseReservation(count);
    logger_.Error("WAL transaction rolled back, released " + std::to_string(count) + " reserved slots");
  });

  wal_global_id_ = wal_id;
  size_t skipped_entry = 0;

  // Process the insert using reserved position
  size_t cursor = reserved_position;

  // Hold for the record id.
  size_t upsert_size = 0;
  std::vector<int64_t> updated_int_ids(new_record_size);
  std::vector<std::string> updated_string_ids(new_record_size);
  std::vector<size_t> updated_ids_old_idx(new_record_size);
  std::vector<size_t> updated_ids_new_idx(new_record_size);

  for (auto i = 0; i < new_record_size; ++i) {
    auto record = records.GetArrayElement(i);
    for (auto& field : table_schema.fields_) {
      // Index fields will be processed after the loop.
      if (field.is_index_field_) {
        continue;
      }
      if (field.field_type_ == meta::FieldType::STRING) {
        // Insert string attribute.
        auto value = record.GetString(field.name_);
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

        auto size = indices.GetSize();
        auto vecDim = size > 0 ? indices.GetArrayElement(size - 1).GetInt() : field.vector_dimension_;  // if size is 0, the vector is zero
        if (vecDim >= field.vector_dimension_) {
          std::cerr << "Record " + std::to_string(i) + " field " + field.name_ +
                           " has wrong dimension, expecting: " + std::to_string(field.vector_dimension_) + " actual: " + std::to_string(vecDim)
                    << std::endl;
          skipped_entry++;
          goto LOOP_END;
        }

        auto vec = std::make_shared<SparseVector>();

        float sum = 0;
        for (auto j = 0; j < indices.GetSize(); ++j) {
          auto index_signed = indices.GetArrayElement(j).GetInt();
          if (index_signed < 0) {
            std::cerr << "entry has negative index value" << index_signed << ", skipping." << std::endl;
            skipped_entry++;
            goto LOOP_END;
          }
          size_t index = static_cast<size_t>(index_signed);
          if (j > 0 && index <= vec->back().index) {
            std::cerr << "the index is not increasing: [...," << vec->back().index << " , " << index << ",...], skipping." << std::endl;
            skipped_entry++;
            goto LOOP_END;
          }
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
        if (vector.GetSize() != field.vector_dimension_) {
          std::cerr << "Record " + std::to_string(i) + " field " + field.name_ + " has wrong dimension, expecting: " +
                           std::to_string(field.vector_dimension_) + " actual: " + std::to_string(record.GetArraySize(field.name_));
          skipped_entry++;
          goto LOOP_END;
        }
        float sum = 0;
        float* base_vec_ptr = vector_tables_[field_id_mem_offset_map_[field.id_]]->GetVector(cursor);
        if (base_vec_ptr == nullptr) {
          std::cerr << "ERROR: GetVector returned null for cursor " << cursor << std::endl;
          skipped_entry++;
          goto LOOP_END;
        }
        for (auto j = 0; j < field.vector_dimension_; ++j) {
          float value = static_cast<float>((float)(vector.GetArrayElement(j).GetDouble()));
          sum += value * value;
          std::memcpy(&base_vec_ptr[j], &value, sizeof(float));
        }
        // convert to length
        if (field.metric_type_ == meta::MetricType::COSINE && sum > 1e-10) {
          sum = std::sqrt(sum);
          // normalize value - reuse the same base_vec_ptr we already validated
          for (auto j = 0; j < field.vector_dimension_; ++j) {
            base_vec_ptr[j] /= sum;
          }
        }
      } else {
        // Insert primitive attribute.
        switch (field.field_type_) {
          case meta::FieldType::INT1: {
            int8_t value = static_cast<int8_t>((int8_t)(record.GetInt(field.name_)));
            std::memcpy(&(attribute_table_->GetData()[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int8_t));
            break;
          }
          case meta::FieldType::INT2: {
            int16_t value = static_cast<int16_t>((int16_t)(record.GetInt(field.name_)));
            std::memcpy(&(attribute_table_->GetData()[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int16_t));
            break;
          }
          case meta::FieldType::INT4: {
            int32_t value = static_cast<int32_t>((int32_t)(record.GetInt(field.name_)));
            std::memcpy(&(attribute_table_->GetData()[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int32_t));
            break;
          }
          case meta::FieldType::INT8: {
            int64_t value = static_cast<int64_t>((int64_t)(record.GetInt(field.name_)));
            std::memcpy(&(attribute_table_->GetData()[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(int64_t));
            break;
          }
          case meta::FieldType::FLOAT: {
            float value = static_cast<float>((float)(record.GetDouble(field.name_)));
            std::memcpy(&(attribute_table_->GetData()[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(float));
            break;
          }
          case meta::FieldType::DOUBLE: {
            double value = record.GetDouble(field.name_);
            std::memcpy(&(attribute_table_->GetData()[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(double));
            break;
          }
          case meta::FieldType::BOOL: {
            bool value = record.GetBool(field.name_);
            std::memcpy(&(attribute_table_->GetData()[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &value, sizeof(bool));
            break;
          }
          case meta::FieldType::GEO_POINT: {
            double lat = record.Get(field.name_).GetDouble("latitude");
            if (lat < -90) {
              lat = -90;
            }
            if (lat > 90) {
              lat = 90;
            }
            double lon = record.Get(field.name_).GetDouble("longitude");
            if (lon < -180) {
              lon = -180;
            }
            if (lon > 180) {
              lon = 180;
            }
            std::memcpy(&(attribute_table_->GetData()[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), &lat, sizeof(double));
            std::memcpy(&(attribute_table_->GetData()[cursor * primitive_offset_ + field_id_mem_offset_map_[field.id_] + sizeof(double)]), &lon, sizeof(double));
            // Insert into the geospatial index.
            geospatial_indices_[field.name_]->insertPoint(lat, lon, cursor);
            break;
          }
          default:
            break;
        }
      }
    }
    // Handle pk.
    if (pk_field_idx_) {
      auto& field = schema.fields_[*pk_field_idx_];
      if (field.field_type_ == meta::FieldType::STRING) {
        auto value = record.GetString(field.name_);
        auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
        if (exist) {
          if (upsert) {
            updated_string_ids[upsert_size] = value;
            primary_key_.getKey(value, updated_ids_old_idx[upsert_size]);
            updated_ids_new_idx[upsert_size++] = cursor;
          } else {
            // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
            skipped_entry++;
            goto LOOP_END; 
          }
        }
      } else {
        switch (field.field_type_) {
          case meta::FieldType::INT1: {
            int8_t value = static_cast<int8_t>((int8_t)(record.GetInt(field.name_)));
            auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
            if (exist) {
              if (upsert) {
                updated_int_ids[upsert_size] = value;
                primary_key_.getKey(static_cast<int8_t>(value), updated_ids_old_idx[upsert_size]);
                updated_ids_new_idx[upsert_size++] = cursor;
              } else {
                // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
                skipped_entry++;
                goto LOOP_END; 
              }
            }
            break;
          }
          case meta::FieldType::INT2: {
            int16_t value = static_cast<int16_t>((int16_t)(record.GetInt(field.name_)));
            auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
            if (exist) {
              if (upsert) {
                updated_int_ids[upsert_size] = value;
                primary_key_.getKey(static_cast<int16_t>(value), updated_ids_old_idx[upsert_size]);
                updated_ids_new_idx[upsert_size++] = cursor;
              } else {
                // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
                skipped_entry++;
                goto LOOP_END; 
              }
            }
            break;
          }
          case meta::FieldType::INT4: {
            int32_t value = static_cast<int32_t>((int32_t)(record.GetInt(field.name_)));
            auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
            if (exist) {
              if (upsert) {
                updated_int_ids[upsert_size] = value;
                primary_key_.getKey(static_cast<int32_t>(value), updated_ids_old_idx[upsert_size]);
                updated_ids_new_idx[upsert_size++] = cursor;
              } else {
                // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
                skipped_entry++;
                goto LOOP_END; 
              }
            }
            break;
          }
          case meta::FieldType::INT8: {
            int64_t value = static_cast<int64_t>((int64_t)(record.GetInt(field.name_)));
            auto exist = !primary_key_.addKeyIfNotExist(value, cursor);
            if (exist) {
              if (upsert) {
                updated_int_ids[upsert_size] = value;
                primary_key_.getKey(static_cast<int64_t>(value), updated_ids_old_idx[upsert_size]);
                updated_ids_new_idx[upsert_size++] = cursor;
              } else {
                // std::cerr << "primary key [" << value << "] already exists, skipping." << std::endl;
                skipped_entry++;
                goto LOOP_END; 
              }
            }
            break;
          }
        }
      }
    }
    ++cursor;
  LOOP_END: {}
    // nothing should be done at the end of block
  }

  // Now process the index fields.
  // The record rows needs to be processed are [record_number_, cursor).
  // Assume the embedding service already normalize the vectors for dense vectors.
  for (auto& index: table_schema.indices_) {
    // TODO: support sparse embedding.
    auto status = embedding_service_->denseEmbedDocuments(
      index.embedding_model_name_,
      var_len_attr_table_[field_id_mem_offset_map_[index.src_field_id_]],
      vector_tables_[field_id_mem_offset_map_[index.tgt_field_id_]]->GetData(),
      record_number_,
      cursor,
      table_schema.fields_[index.tgt_field_id_].vector_dimension_,
      headers,
      index.dimensions > 0
    );
    if (!status.ok()) {
      std::cerr << "embedding service error: " << status.message() << std::endl;
      return status;
    }
  }

  size_t old_record_number = record_number_;

  // update the vector size  
  record_number_.store(reserved_position + new_record_size - skipped_entry);

  // For upsert, need to update the pk map, and delete the older version of the records.
  if (upsert) {
    for (auto idx = 0; idx < upsert_size; ++idx) {
      // Update the pk map.
      if (isIntPK()) {
        auto fieldType = pkType();
        switch (pkType()) {
          case meta::FieldType::INT1:
            primary_key_.updateKey(static_cast<int8_t>(updated_int_ids[idx]), updated_ids_new_idx[idx]);
            break;
          case meta::FieldType::INT2:
            primary_key_.updateKey(static_cast<int16_t>(updated_int_ids[idx]), updated_ids_new_idx[idx]);
            break;
          case meta::FieldType::INT4:
            primary_key_.updateKey(static_cast<int32_t>(updated_int_ids[idx]), updated_ids_new_idx[idx]);
            break;
          case meta::FieldType::INT8:
            primary_key_.updateKey(static_cast<int64_t>(updated_int_ids[idx]), updated_ids_new_idx[idx]);
            break;
        }
      } else if (isStringPK()) {
        primary_key_.updateKey(updated_string_ids[idx], updated_ids_new_idx[idx]);
      }
      // Delete the old version record.
      deleted_->set(updated_ids_old_idx[idx]);
    }
  }

  // Segment is modified.
  skip_sync_disk_.store(false);
  
  // Commit the WAL transaction on success
  wal_txn->Commit();
  
  // Update snapshot version for queries
  snapshot_manager_->IncrementVersion();

  // === INSERT OPERATION COMPLETE - FINAL LOGGING ===
  auto insert_end_time = std::chrono::steady_clock::now();
  auto total_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(insert_end_time - insert_start_time).count();

  logger_.Info("INSERT_COMPLETE: thread_id=" + std::to_string(std::hash<std::thread::id>{}(thread_id)) +
               ", wal_id=" + std::to_string(wal_id) +
               ", records_inserted=" + std::to_string(new_record_size - skipped_entry) +
               ", records_skipped=" + std::to_string(skipped_entry) +
               ", total_duration_ms=" + std::to_string(total_duration_ms) +
               ", final_capacity=" + std::to_string(capacity_) +
               ", final_record_count=" + std::to_string(record_number_.load()));

  auto msg = "{\"inserted\": " + std::to_string(new_record_size - skipped_entry) + ", \"skipped\": " + std::to_string(skipped_entry) + "}";
  auto statusCode = DB_SUCCESS;
  return Status(statusCode, msg);
  // auto msg = std::string("successfully inserted " +
  //                       std::to_string(new_record_size - skipped_entry) +
  //                       " records. ");
  // if (skipped_entry > 0) {
  //   msg += "skipped " +
  //          std::to_string(skipped_entry) + " records with primary key values that already exist, or invalid fields.";
  //   std::cerr << msg << std::endl;
  // }
  // if (skipped_entry == new_record_size) {
  //   statusCode = INVALID_RECORD;
  // }
}

Status TableSegment::InsertPrepare(meta::TableSchema& table_schema, Json& pks, Json& result) {
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

// Status TableSegment::SaveTableSegment(meta::TableSchema& table_schema, const std::string& db_catalog_path) {
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

Status TableSegment::SaveTableSegment(meta::TableSchema& table_schema, const std::string& db_catalog_path, bool force) {
  if (skip_sync_disk_ && !force) {
    return Status::OK();
  }

  // CRITICAL FIX: Acquire shared lock to protect data structures during save
  // This prevents concurrent modifications (e.g., from Insert) while reading vector_tables_
  // Shared lock allows multiple concurrent saves but blocks exclusive locks (Insert/Delete/Compact)
  std::shared_lock<std::shared_mutex> lock(data_rw_mutex_);

  // Construct the file path
  std::string path = db_catalog_path + "/" + std::to_string(table_schema.id_) + "/data_mvp.bin";
  std::string tmp_path = path + ".tmp";

  FILE* file = fopen(tmp_path.c_str(), "wb");
  if (!file) {
    return Status(DB_UNEXPECTED_ERROR, "Cannot open file: " + path);
  }
  // Write the number of records and the first record id
  int64_t current_wal_global_id = wal_global_id_.load();
  size_t current_record_number = record_number_.load();
  fwrite(&current_record_number, sizeof(current_record_number), 1, file);
  fwrite(&first_record_id_, sizeof(first_record_id_), 1, file);

  // Write the bitset
  int64_t bitset_size = deleted_->size();
  const uint8_t* bitset_data = deleted_->data();
  fwrite(&bitset_size, sizeof(bitset_size), 1, file);
  fwrite(bitset_data, bitset_size, 1, file);

  // Write the attribute table.
  fwrite(attribute_table_->GetData(), current_record_number * primitive_offset_, 1, file);

  // Write the variable length table.
  for (auto recordIdx = 0; recordIdx < current_record_number; ++recordIdx) {
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
  // NOTE: Caller must hold appropriate lock (shared or exclusive) to protect vector_tables_
  for (auto i = 0; i < dense_vector_num_; ++i) {
    if (!vector_tables_[i]) {
      logger_.Error("SaveTableSegment: vector_tables_[" + std::to_string(i) + "] is NULL!");
      fclose(file);
      return Status(DB_UNEXPECTED_ERROR, "Vector table is NULL during save");
    }
    // Save only the valid data portion (current_record_number records)
    size_t bytes_to_write = sizeof(float) * current_record_number * vector_dims_[i];
    fwrite(vector_tables_[i]->GetData(), bytes_to_write, 1, file);
  }

  // Last, write the global wal id.
  fwrite(&current_wal_global_id, sizeof(current_wal_global_id), 1, file);

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

size_t TableSegment::GetRecordCount() {
  return record_number_ - deleted_->count(record_number_);
}

double TableSegment::GetDeletedRatio() const {
  if (record_number_ == 0) return 0.0;
  return static_cast<double>(deleted_->count(record_number_)) / static_cast<double>(record_number_);
}

bool TableSegment::NeedsCompaction(double threshold) const {
  return GetDeletedRatio() > threshold;
}

Status TableSegment::CompactSegment() {
  // Set compaction flag to prevent resize during compaction
  compaction_in_progress_.store(true);

  std::unique_lock<std::shared_mutex> lock(data_rw_mutex_);
  
  size_t deleted_count = deleted_->count(record_number_);
  if (deleted_count == 0) {
    compaction_in_progress_.store(false);  // Clear flag before early return
    return Status(DB_SUCCESS, "No deleted records to compact");
  }
  
  size_t new_size = record_number_ - deleted_count;
  if (new_size == 0) {
    // All records are deleted, just reset everything
    record_number_ = 0;
    deleted_ = std::make_unique<ConcurrentBitset>(size_limit_);
    primary_key_.clear();
    skip_sync_disk_.store(false);
    compaction_in_progress_.store(false);  // Clear flag before return
    return Status(DB_SUCCESS, "All records deleted, segment reset");
  }
  
  // Create new dynamic data structures - CRITICAL: Use same capacity as vector tables
  // Calculate capacity with substantial growth headroom for concurrent inserts
  // This prevents frequent resizes and supports lock-free concurrent operations
  // Use large headroom (10x or min 500) to avoid triggering predictive expansion
  size_t actual_vector_capacity = std::max(static_cast<size_t>(500), static_cast<size_t>(new_size * 10));

  std::unique_ptr<DynamicMemoryBlock<char>> new_attribute_table;
  if (primitive_offset_ > 0) {
    // Use the same capacity as vector tables for consistency
    size_t total_size = 0;
    if (__builtin_mul_overflow(actual_vector_capacity, primitive_offset_, &total_size)) {
      return Status(DB_UNEXPECTED_ERROR, "Integer overflow in compaction attribute table size");
    }
    new_attribute_table = std::make_unique<DynamicMemoryBlock<char>>(total_size, GrowthStrategy::ADAPTIVE);
    std::memset(new_attribute_table->GetData(), 0, total_size);
  }

  std::vector<std::unique_ptr<DynamicVectorBlock>> new_vector_tables;

  if (dense_vector_num_ > 0) {
    new_vector_tables.resize(dense_vector_num_);

    for (int i = 0; i < dense_vector_num_; i++) {
      // Create vector table with physical capacity for growth
      // This allows concurrent inserts to reserve positions without triggering resize
      new_vector_tables[i] = std::make_unique<DynamicVectorBlock>(
          actual_vector_capacity, vector_dims_[i], GrowthStrategy::ADAPTIVE
      );
      logger_.Info("Compaction: Created vector table " + std::to_string(i) +
                   " with capacity=" + std::to_string(actual_vector_capacity));
    }
  }
  
  std::vector<VariableLenAttrColumnContainer> new_var_len_attr_table;
  if (var_len_attr_num_ > 0) {
    new_var_len_attr_table.resize(var_len_attr_num_);
    // CRITICAL FIX: Reserve capacity for future inserts
    // Each column needs capacity to match the vector/attribute tables
    for (int v = 0; v < var_len_attr_num_; v++) {
      new_var_len_attr_table[v].resize(actual_vector_capacity);
    }
    logger_.Info("Compaction: Resized " + std::to_string(var_len_attr_num_) +
                 " var-length columns to capacity=" + std::to_string(actual_vector_capacity));
  }

  // Rebuild primary key index with new positions
  UniqueKey new_primary_key;

  // Copy non-deleted records to new structures
  size_t new_idx = 0;
  for (size_t old_idx = 0; old_idx < record_number_; old_idx++) {
    if (!deleted_->test(old_idx)) {
      // Copy primitive attributes
      if (primitive_offset_ > 0 && attribute_table_) {
        std::memcpy(new_attribute_table->GetData() + new_idx * primitive_offset_,
                    attribute_table_->GetData() + old_idx * primitive_offset_,
                    primitive_offset_);
      }

      // Copy vector data
      for (int v = 0; v < dense_vector_num_; v++) {
        std::memcpy(new_vector_tables[v]->GetVector(new_idx),
                    vector_tables_[v]->GetVector(old_idx),
                    vector_dims_[v] * sizeof(float));
      }

      // Copy variable length attributes - direct assignment, not push_back
      for (int v = 0; v < var_len_attr_num_; v++) {
        new_var_len_attr_table[v][new_idx] = var_len_attr_table_[v][old_idx];
      }
      
      // Rebuild primary key mapping
      if (pk_field_idx_ != nullptr) {
        if (isIntPK()) {
          auto offset = field_id_mem_offset_map_[*pk_field_idx_] + old_idx * primitive_offset_;
          switch (pkType()) {
            case meta::FieldType::INT1: {
              int8_t pk;
              std::memcpy(&pk, &attribute_table_->GetData()[offset], sizeof(int8_t));
              new_primary_key.putKey(pk, new_idx);
              break;
            }
            case meta::FieldType::INT2: {
              int16_t pk;
              std::memcpy(&pk, &attribute_table_->GetData()[offset], sizeof(int16_t));
              new_primary_key.putKey(pk, new_idx);
              break;
            }
            case meta::FieldType::INT4: {
              int32_t pk;
              std::memcpy(&pk, &attribute_table_->GetData()[offset], sizeof(int32_t));
              new_primary_key.putKey(pk, new_idx);
              break;
            }
            case meta::FieldType::INT8: {
              int64_t pk;
              std::memcpy(&pk, &attribute_table_->GetData()[offset], sizeof(int64_t));
              new_primary_key.putKey(pk, new_idx);
              break;
            }
            default:
              break;
          }
        } else if (isStringPK()) {
          auto& pk = std::get<std::string>(var_len_attr_table_[*string_pk_offset_][old_idx]);
          new_primary_key.putKey(pk, new_idx);
        }
      }
      
      // Update geospatial indices if present
      for (auto& [field_name, geo_index] : geospatial_indices_) {
        // Note: This would need proper implementation based on the actual geospatial index API
        // For now, we'll need to rebuild the index after compaction
      }
      
      new_idx++;
    }
  }

  // IMPORTANT: Do NOT call ResizeVectors here!
  // Vector tables were created with DynamicVectorBlock(actual_vector_capacity, dim)
  // This sets num_vectors_ = actual_vector_capacity (e.g., 500)
  // We need num_vectors_ to remain at full capacity so GetVector(reserved_position) works
  // Only new_size (e.g., 24) records contain valid data, but positions [24, 500) must be accessible
  // for future inserts that use capacity_manager_->TryReserve()

  logger_.Info("Compaction: Vector tables created with num_vectors=" + std::to_string(actual_vector_capacity) +
               " (actual valid data: " + std::to_string(new_size) + " records)");

  // Replace old structures with new ones
  attribute_table_ = std::move(new_attribute_table);
  vector_tables_ = std::move(new_vector_tables);
  var_len_attr_table_ = std::move(new_var_len_attr_table);

  // Validate vector tables after move
  for (size_t i = 0; i < vector_tables_.size(); i++) {
    if (vector_tables_[i]) {
      size_t cap = vector_tables_[i]->GetCapacity();
      size_t dim = vector_tables_[i]->GetDimension();
      logger_.Info("Compaction: Validated vector_tables_[" + std::to_string(i) +
                   "] after move: capacity=" + std::to_string(cap) +
                   ", dimension=" + std::to_string(dim));
    } else {
      logger_.Error("Compaction: vector_tables_[" + std::to_string(i) + "] is NULL after move!");
    }
  }

  // For UniqueKey, we need to swap since it can't be moved due to mutex
  primary_key_.swap(new_primary_key);

  // Reset deleted bitset
  deleted_ = std::make_unique<ConcurrentBitset>(size_limit_);

  // Update record count and capacity to match compacted size
  size_t old_count = record_number_.load();
  record_number_ = new_size;

  // CRITICAL FIX: Set capacity to actual_vector_capacity (with headroom)
  // Vector tables were created with actual_vector_capacity for concurrent inserts
  // This prevents resize on every insert and supports lock-free TryReserve() operations
  capacity_ = actual_vector_capacity;

  // CRITICAL FIX: Reset capacity_manager_ to match the new capacity
  // Use actual_vector_capacity to allow concurrent inserts without resize
  capacity_manager_ = std::make_unique<AtomicCapacityManager>(actual_vector_capacity);
  // Reserve the already-used space (new_size positions)
  size_t dummy_pos;
  if (new_size > 0) {
    capacity_manager_->TryReserve(new_size, dummy_pos);
  }

  logger_.Info("Compaction reset capacity_manager_: capacity=" + std::to_string(actual_vector_capacity) +
               ", current_size=" + std::to_string(new_size));

  // Mark for sync to disk
  skip_sync_disk_.store(false);

  logger_.Info("Compaction completed: " + std::to_string(old_count) + " -> " +
               std::to_string(new_size) + " records (removed " +
               std::to_string(deleted_count) + " deleted records)");

  // Clear compaction flag
  compaction_in_progress_.store(false);

  return Status(DB_SUCCESS, "Compacted " + std::to_string(deleted_count) + " deleted records");
}

void TableSegment::Debug(meta::TableSchema& table_schema) {
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
      std::cout << ((attribute_table_->GetData()[i] >> j) & 1);
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
            std::memcpy(&value, &(attribute_table_->GetData()[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int8_t));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::INT2: {
            int16_t value;
            std::memcpy(&value, &(attribute_table_->GetData()[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int16_t));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::INT4: {
            int32_t value;
            std::memcpy(&value, &(attribute_table_->GetData()[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int32_t));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::INT8: {
            int64_t value;
            std::memcpy(&value, &(attribute_table_->GetData()[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(int64_t));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::FLOAT: {
            float value;
            std::memcpy(&value, &(attribute_table_->GetData()[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(float));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::DOUBLE: {
            double value;
            std::memcpy(&value, &(attribute_table_->GetData()[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(double));
            std::cout << value << " ";
            break;
          }
          case meta::FieldType::BOOL: {
            bool value;
            std::memcpy(&value, &(attribute_table_->GetData()[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(bool));
            std::cout << (value ? "true" : "false") << " ";
            break;
          }
          case meta::FieldType::GEO_POINT: {
            double value;
            std::memcpy(&value, &(attribute_table_->GetData()[i * primitive_offset_ + field_id_mem_offset_map_[field.id_]]), sizeof(double));
            std::cout << "Latitude: " << value << " ";
            std::memcpy(&value, &(attribute_table_->GetData()[i * primitive_offset_ + field_id_mem_offset_map_[field.id_] + sizeof(double)]), sizeof(double));
            std::cout << "Longitude: " << value << " ";
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
        std::cout << vector_tables_[i]->GetData()[offset] << " ";
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

TableSegment::~TableSegment() {
  Release();
}

Status TableSegment::Release() {
  // Smart pointers will automatically clean up
  attribute_table_.reset();
  vector_tables_.clear();
  deleted_.reset();
  return Status::OK();
}

// Factory method for safe loading from disk
std::pair<Status, std::shared_ptr<TableSegment>> TableSegment::CreateFromDisk(
    meta::TableSchema& table_schema,
    const std::string& db_catalog_path,
    int64_t init_table_scale,
    std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service) {
  
  try {
    auto segment = std::make_shared<TableSegmentMVP>(
        table_schema, db_catalog_path, init_table_scale, embedding_service);
    return {Status::OK(), segment};
  } catch (const std::exception& e) {
    return {Status(DB_UNEXPECTED_ERROR, "Failed to load table segment: " + std::string(e.what())), 
            nullptr};
  } catch (...) {
    return {Status(DB_UNEXPECTED_ERROR, "Failed to load table segment: unknown error"), 
            nullptr};
  }
}

}  // namespace engine
}  // namespace vectordb
