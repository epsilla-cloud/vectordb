#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <filesystem>

#include "db/db_server.hpp"
#include "db/database.hpp"
#include "config/config.hpp"
#include "utils/json.hpp"

using namespace vectordb;
using namespace vectordb::engine;

class DataSafetyTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directory
        test_dir_ = "/tmp/vectordb_safety_test_" + std::to_string(std::time(nullptr));
        std::filesystem::create_directories(test_dir_);
        
        // Initialize DB server
        db_server_ = std::make_shared<DBServer>();
        db_server_->SetLeader(true);
    }
    
    void TearDown() override {
        // Clean up test directory
        if (std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
    }
    
    // Helper method to create test database
    Status CreateTestDatabase(const std::string& db_name) {
        std::string db_path = test_dir_ + "/" + db_name;
        std::filesystem::create_directories(db_path);
        
        std::unordered_map<std::string, std::string> headers;
        return db_server_->LoadDB(db_name, db_path, 1000, true, headers);
    }
    
    // Helper method to insert test data
    Status InsertTestData(const std::string& db_name, const std::string& table_name, int record_count) {
        auto db = db_server_->GetDatabase(db_name);
        if (!db) {
            return Status(DB_NOT_FOUND, "Database not found");
        }
        
        // Create table schema
        meta::TableSchema table_schema;
        table_schema.name_ = table_name;
        
        // Add primary key field
        meta::FieldSchema pk_field;
        pk_field.name_ = "id";
        pk_field.field_type_ = meta::FieldType::INT4;
        pk_field.is_primary_key_ = true;
        table_schema.fields_.push_back(pk_field);
        
        // Add vector field
        meta::FieldSchema vec_field;
        vec_field.name_ = "vector";
        vec_field.field_type_ = meta::FieldType::VECTOR_FLOAT;
        vec_field.vector_dimension_ = 128;
        vec_field.metric_type_ = meta::MetricType::EUCLIDEAN;
        table_schema.fields_.push_back(vec_field);
        
        // Create table
        auto status = db->CreateTable(table_schema);
        if (!status.ok()) {
            return status;
        }
        
        // Insert records
        auto table = db->GetTable(table_name);
        if (!table) {
            return Status(TABLE_NOT_FOUND, "Table not found");
        }
        
        for (int i = 0; i < record_count; ++i) {
            std::string records_json = "[{\"id\":" + std::to_string(i) + ",\"vector\":[";
            for (int j = 0; j < 128; ++j) {
                if (j > 0) records_json += ",";
                records_json += "0.5";
            }
            records_json += "]}]";
            
            vectordb::Json records;
            records.LoadFromString(records_json);
            
            std::unordered_map<std::string, std::string> headers;
            status = table->Insert(records, false, headers);
            if (!status.ok()) {
                return status;
            }
        }
        
        return Status::OK();
    }
    
    // Helper method to verify data exists
    bool VerifyDataExists(const std::string& db_name, const std::string& table_name, int expected_count) {
        auto db = db_server_->GetDatabase(db_name);
        if (!db) return false;
        
        auto table = db->GetTable(table_name);
        if (!table) return false;
        
        return table->GetRecordCount() == expected_count;
    }
    
protected:
    std::string test_dir_;
    std::shared_ptr<DBServer> db_server_;
};

TEST_F(DataSafetyTest, UnloadWithWALFlush) {
    const std::string db_name = "test_db";
    const std::string table_name = "test_table";
    const int record_count = 100;
    
    // Create database and insert data
    ASSERT_TRUE(CreateTestDatabase(db_name).ok());
    ASSERT_TRUE(InsertTestData(db_name, table_name, record_count).ok());
    
    // Verify data exists before unload
    EXPECT_TRUE(VerifyDataExists(db_name, table_name, record_count));
    
    // Unload database (should flush WAL)
    auto unload_status = db_server_->UnloadDB(db_name);
    ASSERT_TRUE(unload_status.ok()) << "Unload failed: " << unload_status.message();
    
    // Reload database
    std::string db_path = test_dir_ + "/" + db_name;
    std::unordered_map<std::string, std::string> headers;
    auto reload_status = db_server_->LoadDB(db_name, db_path, 1000, true, headers);
    ASSERT_TRUE(reload_status.ok()) << "Reload failed: " << reload_status.message();
    
    // Verify data persisted after reload
    EXPECT_TRUE(VerifyDataExists(db_name, table_name, record_count)) 
        << "Data was not persisted properly during unload/reload";
}

TEST_F(DataSafetyTest, ConcurrentWriteAndUnload) {
    const std::string db_name = "concurrent_db";
    const std::string table_name = "concurrent_table";
    const int initial_records = 50;
    const int concurrent_writes = 50;
    
    // Create database and insert initial data
    ASSERT_TRUE(CreateTestDatabase(db_name).ok());
    ASSERT_TRUE(InsertTestData(db_name, table_name, initial_records).ok());
    
    std::atomic<bool> writing_done(false);
    std::atomic<int> records_written(0);
    
    // Start concurrent write thread
    std::thread write_thread([&]() {
        auto db = db_server_->GetDatabase(db_name);
        if (!db) return;
        
        auto table = db->GetTable(table_name);
        if (!table) return;
        
        for (int i = initial_records; i < initial_records + concurrent_writes; ++i) {
            std::string records_json = "[{\"id\":" + std::to_string(i) + ",\"vector\":[";
            for (int j = 0; j < 128; ++j) {
                if (j > 0) records_json += ",";
                records_json += "0.5";
            }
            records_json += "]}]";
            
            vectordb::Json records;
            records.LoadFromString(records_json);
            
            std::unordered_map<std::string, std::string> headers;
            auto status = table->Insert(records, false, headers);
            if (status.ok()) {
                records_written++;
            }
            
            // Small delay to increase chance of concurrent unload
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        writing_done = true;
    });
    
    // Wait a bit for writes to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Unload while writes are happening
    auto unload_status = db_server_->UnloadDB(db_name);
    ASSERT_TRUE(unload_status.ok()) << "Unload failed: " << unload_status.message();
    
    // Wait for write thread to complete
    write_thread.join();
    
    // Reload database
    std::string db_path = test_dir_ + "/" + db_name;
    std::unordered_map<std::string, std::string> headers;
    auto reload_status = db_server_->LoadDB(db_name, db_path, 1000, true, headers);
    ASSERT_TRUE(reload_status.ok()) << "Reload failed: " << reload_status.message();
    
    // Verify at least initial records were persisted
    auto db = db_server_->GetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    
    auto table = db->GetTable(table_name);
    ASSERT_NE(table, nullptr);
    
    size_t final_count = table->GetRecordCount();
    EXPECT_GE(final_count, initial_records) 
        << "Initial records were not persisted. Expected at least " 
        << initial_records << " but got " << final_count;
    
    std::cout << "Data Safety Test Results:" << std::endl;
    std::cout << "  Initial records: " << initial_records << std::endl;
    std::cout << "  Concurrent writes attempted: " << concurrent_writes << std::endl;
    std::cout << "  Records written before unload: " << records_written.load() << std::endl;
    std::cout << "  Final persisted count: " << final_count << std::endl;
}

TEST_F(DataSafetyTest, WALRecoveryAfterCrash) {
    const std::string db_name = "crash_db";
    const std::string table_name = "crash_table";
    const int record_count = 75;
    
    // Create database and insert data
    ASSERT_TRUE(CreateTestDatabase(db_name).ok());
    ASSERT_TRUE(InsertTestData(db_name, table_name, record_count).ok());
    
    // Get database and force WAL flush
    auto db = db_server_->GetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    
    auto flush_status = db->FlushAllWAL();
    ASSERT_TRUE(flush_status.ok()) << "WAL flush failed: " << flush_status.message();
    
    // Simulate crash by resetting server without proper unload
    db_server_.reset();
    
    // Create new server and reload
    db_server_ = std::make_shared<DBServer>();
    db_server_->SetLeader(true);
    
    std::string db_path = test_dir_ + "/" + db_name;
    std::unordered_map<std::string, std::string> headers;
    auto reload_status = db_server_->LoadDB(db_name, db_path, 1000, true, headers);
    ASSERT_TRUE(reload_status.ok()) << "Reload after crash failed: " << reload_status.message();
    
    // Verify data recovered from WAL
    EXPECT_TRUE(VerifyDataExists(db_name, table_name, record_count))
        << "Data was not recovered from WAL after simulated crash";
}

TEST_F(DataSafetyTest, MetadataPersistence) {
    const std::string db_name = "metadata_db";
    const std::string table1 = "table1";
    const std::string table2 = "table2";
    
    // Create database with multiple tables
    ASSERT_TRUE(CreateTestDatabase(db_name).ok());
    ASSERT_TRUE(InsertTestData(db_name, table1, 25).ok());
    ASSERT_TRUE(InsertTestData(db_name, table2, 35).ok());
    
    // Get initial table list
    auto db = db_server_->GetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    auto initial_tables = db->GetTables();
    EXPECT_EQ(initial_tables.size(), 2);
    
    // Unload and reload
    ASSERT_TRUE(db_server_->UnloadDB(db_name).ok());
    
    std::string db_path = test_dir_ + "/" + db_name;
    std::unordered_map<std::string, std::string> headers;
    ASSERT_TRUE(db_server_->LoadDB(db_name, db_path, 1000, true, headers).ok());
    
    // Verify metadata persisted
    db = db_server_->GetDatabase(db_name);
    ASSERT_NE(db, nullptr);
    auto reloaded_tables = db->GetTables();
    EXPECT_EQ(reloaded_tables.size(), 2);
    EXPECT_TRUE(VerifyDataExists(db_name, table1, 25));
    EXPECT_TRUE(VerifyDataExists(db_name, table2, 35));
}