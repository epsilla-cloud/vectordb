// basic_meta_impl.hpp
#pragma once

#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include "db/catalog/meta.hpp"
#include "services/embedding_service.hpp"

namespace vectordb {
namespace engine {
namespace meta {

class BasicMetaImpl : public Meta {
 public:
  explicit BasicMetaImpl();
  ~BasicMetaImpl();

  Status LoadDatabase(const std::string& db_catalog_path, const std::string& db_name) override;

  Status HasDatabase(const std::string& db_name, bool& response) override;

  Status GetDatabase(const std::string& db_name, DatabaseSchema& response) override;

  Status UnloadDatabase(const std::string& db_name) override;

  Status DropDatabase(const std::string& db_name) override;

  Status CreateTable(const std::string& db_name, TableSchema& table_schema, size_t& table_id) override;

  Status HasTable(const std::string& db_name, const std::string& table_name, bool& response) override;

  Status GetTable(const std::string& db_name, const std::string& table_name, TableSchema& response) override;

  Status DropTable(const std::string& db_name, const std::string& table_name) override;

  Status SaveDBToFile(const DatabaseSchema& db, const std::string& file_path) override;

  void SetLeader(bool is_leader) override;

  void InjectEmbeddingService(std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service) override;

 private:
  std::unordered_map<std::string, std::mutex> db_mutexes_; // Map to hold a mutex for each database
  std::unordered_map<std::string, DatabaseSchema> databases_;
  std::unordered_set<std::string> loaded_databases_paths_;  // We cannot allow loading the same database twice
  // If the segment is leader (handle sync to storage) or follower (passively sync from storage)
  std::atomic<bool> is_leader_;

  std::shared_ptr<vectordb::engine::EmbeddingService> embedding_service_;
};  // BasicMetaImpl

}  // namespace meta
}  // namespace engine
}  // namespace vectordb

/**
 *
  // Test catalog
  vectordb::engine::meta::MetaPtr meta = std::make_shared<vectordb::engine::meta::BasicMetaImpl>();
  std::string db_name = "test_db";
  std::string db_catalog_path = "/tmp/epsilla-01/";
  meta->LoadDatabase(db_catalog_path, db_name);
  meta->DropTable(db_name, "test_table_2");
  vectordb::engine::meta::TableSchema table_schema;
  table_schema.name_ = "test_table_2";
  vectordb::engine::meta::FieldSchema id_field;
  id_field.name_ = "id";
  id_field.is_primary_key_ = true;
  id_field.field_type_ = vectordb::engine::meta::FieldType::INT4;
  table_schema.fields_.push_back(id_field);
  vectordb::engine::meta::FieldSchema doc_field;
  doc_field.name_ = "doc";
  doc_field.field_type_ = vectordb::engine::meta::FieldType::STRING;
  table_schema.fields_.push_back(doc_field);
  vectordb::engine::meta::FieldSchema vec_field;
  vec_field.name_ = "vec1";
  vec_field.field_type_ = vectordb::engine::meta::FieldType::VECTOR_FLOAT;
  vec_field.vector_dimension_ = 768;
  table_schema.fields_.push_back(vec_field);
  vectordb::engine::meta::FieldSchema vec2_field;
  vec2_field.name_ = "vec1";
  vec2_field.field_type_ = vectordb::engine::meta::FieldType::VECTOR_DOUBLE;
  vec2_field.vector_dimension_ = 768;
  vec2_field.metric_type_ = vectordb::engine::meta::MetricType::COSINE;
  table_schema.fields_.push_back(vec2_field);
  vectordb::engine::meta::AutoEmbedding auto_embed;
  auto_embed.src_field_id_ = 2;
  auto_embed.tgt_field_id_ = 3;
  auto_embed.model_name_ = "sentence-transformers/paraphrase-albert-small-v2";
  table_schema.auto_embeddings_.push_back(auto_embed);
  table_schema.auto_embeddings_.push_back({2, 4, "sentence-transformers/paraphrase-albert-small-v1"});

  auto status2 = meta->CreateTable(db_name, table_schema);
  if (!status2.ok()) {
    std::cout << status2.message() << std::endl;
  }

*/
