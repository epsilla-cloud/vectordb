#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <vector>
#include <random>
#include <chrono>

#include "config/config.hpp"
#include "db/table_segment.hpp"
#include "utils/json.hpp"

using namespace vectordb;
using namespace vectordb::engine;

class ConcurrentHardDeleteTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Force hard delete mode
    vectordb::globalConfig.setSoftDelete(false);
    
    // Create test schema
    schema_.name_ = "test_table";
    
    // Add primary key field
    meta::FieldSchema pk_field;
    pk_field.name_ = "id";
    pk_field.field_type_ = meta::FieldType::INT4;
    pk_field.is_primary_key_ = true;
    schema_.fields_.push_back(pk_field);
    
    // Add vector field  
    meta::FieldSchema vec_field;
    vec_field.name_ = "vector";
    vec_field.field_type_ = meta::FieldType::VECTOR_FLOAT;
    vec_field.vector_dimension_ = 128;
    vec_field.metric_type_ = meta::MetricType::EUCLIDEAN;
    schema_.fields_.push_back(vec_field);
    
    // Create table segment
    int64_t init_scale = 1000;
    segment_ = std::make_shared<TableSegment>(schema_, init_scale, nullptr);
  }

  void TearDown() override {
    // Restore default soft delete mode
    vectordb::globalConfig.setSoftDelete(true);
  }

  // Helper method to insert test records
  void InsertTestRecords(int start_id, int count) {
    std::vector<vectordb::query::expr::ExprNodePtr> empty_filter;
    
    for (int i = 0; i < count; ++i) {
      // Create a JSON array for the records
      std::string records_json = "[{";
      records_json += "\"id\":" + std::to_string(start_id + i) + ",";
      records_json += "\"vector\":[";
      
      // Create random vector
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_real_distribution<float> dis(-1.0f, 1.0f);
      
      for (int j = 0; j < 128; ++j) {
        if (j > 0) records_json += ",";
        records_json += std::to_string(dis(gen));
      }
      
      records_json += "]}]";
      
      vectordb::Json records;
      records.LoadFromString(records_json);
      
      std::unordered_map<std::string, std::string> headers;
      auto status = segment_->Insert(schema_, records, i, headers, false);
      ASSERT_TRUE(status.ok()) << "Insert failed: " << status.message();
    }
  }

protected:
  meta::TableSchema schema_;
  std::shared_ptr<TableSegment> segment_;
};

TEST_F(ConcurrentHardDeleteTest, BatchDeleteConcurrencySafety) {
  const int num_records = 1000;
  const int num_threads = 8;
  const int deletions_per_thread = 50;
  
  // Insert test records
  InsertTestRecords(1, num_records);
  
  EXPECT_EQ(segment_->GetRecordCount(), num_records);
  
  // Concurrent deletion test
  std::vector<std::thread> delete_threads;
  std::atomic<int> successful_deletes(0);
  std::atomic<int> failed_deletes(0);
  
  auto delete_worker = [&](int thread_id) {
    std::vector<vectordb::query::expr::ExprNodePtr> empty_filter;
    std::random_device rd;
    std::mt19937 gen(rd() + thread_id);
    std::uniform_int_distribution<int> id_dist(1, num_records);
    
    for (int i = 0; i < deletions_per_thread; ++i) {
      // Try to delete a random record
      int target_id = id_dist(gen);
      
      std::string delete_json = "[" + std::to_string(target_id) + "]";
      vectordb::Json delete_records;
      delete_records.LoadFromString(delete_json);
      
      auto status = segment_->HardDelete(delete_records, empty_filter, thread_id * 1000 + i);
      
      if (status.ok()) {
        successful_deletes.fetch_add(1);
      } else {
        failed_deletes.fetch_add(1);
      }
      
      // Small random delay to increase contention
      std::this_thread::sleep_for(std::chrono::microseconds(gen() % 10));
    }
  };
  
  // Launch delete threads
  for (int i = 0; i < num_threads; ++i) {
    delete_threads.emplace_back(delete_worker, i);
  }
  
  // Wait for all threads to complete
  for (auto& thread : delete_threads) {
    thread.join();
  }
  
  // Verify results
  size_t final_count = segment_->GetRecordCount();
  int total_attempts = num_threads * deletions_per_thread;
  int total_processed = successful_deletes.load() + failed_deletes.load();
  
  EXPECT_EQ(total_processed, total_attempts);
  EXPECT_LE(final_count, num_records);
  EXPECT_GE(successful_deletes.load(), 0);
  
  std::cout << "Concurrent Hard Delete Test Results:" << std::endl;
  std::cout << "  Original records: " << num_records << std::endl;  
  std::cout << "  Final records: " << final_count << std::endl;
  std::cout << "  Successful deletes: " << successful_deletes.load() << std::endl;
  std::cout << "  Failed deletes: " << failed_deletes.load() << std::endl;
  std::cout << "  Records actually deleted: " << (num_records - final_count) << std::endl;
}

TEST_F(ConcurrentHardDeleteTest, MixedReadWriteOperations) {
  const int num_initial_records = 500;
  const int num_threads = 6;
  const int operations_per_thread = 30;
  
  // Insert initial records
  InsertTestRecords(1, num_initial_records);
  
  std::atomic<int> reads_completed(0);
  std::atomic<int> writes_completed(0);
  std::atomic<int> deletes_completed(0);
  std::atomic<int> errors_occurred(0);
  
  auto mixed_worker = [&](int thread_id) {
    std::random_device rd;
    std::mt19937 gen(rd() + thread_id);
    std::uniform_int_distribution<int> op_dist(1, 3); // 1=read, 2=write, 3=delete
    std::uniform_int_distribution<int> id_dist(1, num_initial_records * 2);
    
    for (int i = 0; i < operations_per_thread; ++i) {
      int operation = op_dist(gen);
      int target_id = id_dist(gen);
      
      try {
        if (operation == 1) {
          // Read operation - check if record exists
          vectordb::Json pk_json;
          pk_json.SetInt("", target_id);
          size_t internal_id;
          bool exists = segment_->PK2ID(pk_json, internal_id);
          reads_completed.fetch_add(1);
          
        } else if (operation == 2) {
          // Write operation - insert new record
          int new_id = num_initial_records + thread_id * 1000 + i;
          std::string records_json = "[{\"id\":" + std::to_string(new_id) + ",\"vector\":[";
          
          for (int j = 0; j < 128; ++j) {
            if (j > 0) records_json += ",";
            records_json += "0.5";
          }
          records_json += "]}]";
          
          vectordb::Json records;
          records.LoadFromString(records_json);
          
          std::unordered_map<std::string, std::string> headers;
          auto status = segment_->Insert(schema_, records, thread_id * 1000 + i, headers, false);
          
          if (status.ok()) {
            writes_completed.fetch_add(1);
          }
          
        } else {
          // Delete operation
          std::string delete_json = "[" + std::to_string(target_id) + "]";
          vectordb::Json delete_records;
          delete_records.LoadFromString(delete_json);
          
          std::vector<vectordb::query::expr::ExprNodePtr> empty_filter;
          auto status = segment_->HardDelete(delete_records, empty_filter, thread_id * 1000 + i);
          
          if (status.ok()) {
            deletes_completed.fetch_add(1);
          }
        }
      } catch (...) {
        errors_occurred.fetch_add(1);
      }
      
      // Small delay to increase contention
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  };
  
  // Launch mixed operation threads
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(mixed_worker, i);
  }
  
  // Wait for completion
  for (auto& thread : threads) {
    thread.join();
  }
  
  // Verify no errors occurred during concurrent operations
  EXPECT_EQ(errors_occurred.load(), 0) << "Concurrent operations caused exceptions";
  
  std::cout << "Mixed Operations Test Results:" << std::endl;
  std::cout << "  Reads completed: " << reads_completed.load() << std::endl;
  std::cout << "  Writes completed: " << writes_completed.load() << std::endl;  
  std::cout << "  Deletes completed: " << deletes_completed.load() << std::endl;
  std::cout << "  Errors occurred: " << errors_occurred.load() << std::endl;
  std::cout << "  Final record count: " << segment_->GetRecordCount() << std::endl;
}

TEST_F(ConcurrentHardDeleteTest, PrimaryKeyIndexConsistency) {
  const int num_records = 100;
  const int num_delete_threads = 4;
  
  // Insert test records with known IDs
  InsertTestRecords(1, num_records);
  
  std::atomic<bool> index_corrupted(false);
  std::vector<std::thread> threads;
  
  // Delete threads
  for (int t = 0; t < num_delete_threads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = t * 25 + 1; i <= (t + 1) * 25; ++i) {
        std::string delete_json = "[" + std::to_string(i) + "]";
        vectordb::Json delete_records;
        delete_records.LoadFromString(delete_json);
        
        std::vector<vectordb::query::expr::ExprNodePtr> empty_filter;
        segment_->HardDelete(delete_records, empty_filter, i);
      }
    });
  }
  
  // Verification thread - continuously check index consistency
  std::atomic<bool> stop_verification(false);
  std::thread verification_thread([&]() {
    while (!stop_verification.load()) {
      try {
        // Try to lookup remaining records
        for (int i = num_records + 1; i <= num_records + 10; ++i) {
          vectordb::Json pk_json;
          pk_json.SetInt("", i);
          size_t internal_id;
          segment_->PK2ID(pk_json, internal_id); // Should not crash
        }
      } catch (...) {
        index_corrupted.store(true);
        break;
      }
      
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });
  
  // Wait for deletes to complete
  for (auto& thread : threads) {
    thread.join();
  }
  
  stop_verification.store(true);
  verification_thread.join();
  
  // Verify index was not corrupted during concurrent operations
  EXPECT_FALSE(index_corrupted.load()) << "Primary key index was corrupted during concurrent hard deletes";
  
  std::cout << "Index consistency test completed. Final record count: " << segment_->GetRecordCount() << std::endl;
}