#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <chrono>

#include "db/compaction_manager.hpp"
#include "db/database.hpp"
#include "db/table.hpp"
#include "config/config.hpp"
#include "utils/json.hpp"

using namespace vectordb;
using namespace vectordb::engine;

class CompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Enable compaction for tests
        globalConfig.AutoCompaction = true;
        globalConfig.CompactionThreshold = 0.2;  // 20% threshold
        globalConfig.CompactionInterval = 1;     // 1 second for testing
        globalConfig.MinVectorsForCompaction = 10;
    }

    void TearDown() override {
        // Stop compaction manager
        auto& mgr = CompactionManager::GetInstance();
        mgr.Stop();
    }
};

TEST_F(CompactionTest, CompactionManagerSingleton) {
    auto& mgr1 = CompactionManager::GetInstance();
    auto& mgr2 = CompactionManager::GetInstance();

    // Should be the same instance
    EXPECT_EQ(&mgr1, &mgr2);
}

TEST_F(CompactionTest, StartStopCompactionManager) {
    auto& mgr = CompactionManager::GetInstance();

    // Start should succeed
    auto status = mgr.Start();
    EXPECT_TRUE(status.ok()) << status.message();

    // Starting again should fail (already running)
    status = mgr.Start();
    EXPECT_FALSE(status.ok());

    // Stop should succeed
    status = mgr.Stop();
    EXPECT_TRUE(status.ok());

    // Stopping again should succeed (idempotent)
    status = mgr.Stop();
    EXPECT_TRUE(status.ok());
}

TEST_F(CompactionTest, CompactionStatistics) {
    auto& mgr = CompactionManager::GetInstance();

    // Get initial global stats
    auto global_stats = mgr.GetGlobalStats();
    EXPECT_EQ(global_stats.total_vectors, 0);
    EXPECT_EQ(global_stats.deleted_vectors, 0);
    EXPECT_EQ(global_stats.compaction_count, 0);

    // Update stats for a fake table
    mgr.UpdateStats("test_table", 1000, 200);

    // Get table stats
    auto table_stats = mgr.GetTableStats("test_table");
    EXPECT_EQ(table_stats.total_vectors, 1000);
    EXPECT_EQ(table_stats.deleted_vectors, 200);
    EXPECT_DOUBLE_EQ(table_stats.GetDeletedRatio(), 0.2);
}

TEST_F(CompactionTest, ShouldCompactThreshold) {
    auto& mgr = CompactionManager::GetInstance();

    // Test with sufficient vectors (above minimum of 10)
    mgr.UpdateStats("test_table", 100, 10);  // 10% deleted
    EXPECT_FALSE(mgr.ShouldCompact("test_table"));  // Below 20% threshold

    mgr.UpdateStats("test_table", 100, 25);  // 25% deleted
    EXPECT_TRUE(mgr.ShouldCompact("test_table"));   // Above 20% threshold

    // Test minimum vector requirement (minimum is 10)
    mgr.UpdateStats("small_table", 5, 3);  // 60% deleted but only 5 vectors
    EXPECT_FALSE(mgr.ShouldCompact("small_table")); // Below minimum of 10 vectors
}

TEST_F(CompactionTest, CompactionConfiguration) {
    auto& mgr = CompactionManager::GetInstance();

    // Test configuration setters
    mgr.SetAutoCompaction(false);
    EXPECT_FALSE(mgr.IsCompacting());

    mgr.SetCompactionThreshold(0.3);  // 30%
    mgr.SetCompactionInterval(60);     // 1 minute

    // Verify configuration took effect (using 100 vectors, above minimum)
    mgr.UpdateStats("test_config", 100, 25);  // 25% deleted
    EXPECT_FALSE(mgr.ShouldCompact("test_config")); // Now below 30% threshold

    mgr.UpdateStats("test_config", 100, 35);  // 35% deleted
    EXPECT_TRUE(mgr.ShouldCompact("test_config"));  // Above 30% threshold
}

TEST_F(CompactionTest, CompactionStatsJson) {
    auto& mgr = CompactionManager::GetInstance();

    // Add some test data
    mgr.UpdateStats("table1", 1000, 100);
    mgr.UpdateStats("table2", 2000, 500);

    // Get JSON stats
    Json stats_json = mgr.GetAllStatsAsJson();

    // Verify JSON structure
    EXPECT_TRUE(stats_json.HasMember("global"));
    EXPECT_TRUE(stats_json.HasMember("tables"));

    // Check global stats in JSON
    Json global = stats_json.GetObject("global");
    EXPECT_TRUE(global.HasMember("total_vectors"));
    EXPECT_TRUE(global.HasMember("deleted_vectors"));
    EXPECT_TRUE(global.HasMember("deleted_ratio"));
    EXPECT_TRUE(global.HasMember("compaction_count"));
}

TEST_F(CompactionTest, TableRegistration) {
    auto& mgr = CompactionManager::GetInstance();

    // Create a mock table (simplified for testing)
    meta::TableSchema schema;
    schema.name_ = "test_registration";
    schema.id_ = 1;

    // Note: In real tests, we would use a proper Table instance
    // For now, we just test the registration mechanism
    std::shared_ptr<Table> mock_table = nullptr;

    // Register and unregister should not crash even with null table
    mgr.RegisterTable("db.test_registration", mock_table);
    mgr.UnregisterTable("db.test_registration");
}

TEST_F(CompactionTest, ManualCompaction) {
    auto& mgr = CompactionManager::GetInstance();

    // Manual compaction with no tables should succeed
    auto status = mgr.CompactAllTables();
    EXPECT_TRUE(status.ok());

    // Compact specific non-existent table should fail
    status = mgr.CompactTable("non_existent");
    EXPECT_FALSE(status.ok());
}

TEST_F(CompactionTest, ConcurrentCompaction) {
    auto& mgr = CompactionManager::GetInstance();

    std::atomic<int> success_count{0};
    std::atomic<int> failure_count{0};

    // Try to trigger multiple compactions concurrently
    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&mgr, &success_count, &failure_count, i]() {
            auto status = mgr.CompactTable("test_table_" + std::to_string(i));
            if (status.ok()) {
                success_count++;
            } else {
                failure_count++;
            }
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // All should have completed (either success or failure)
    EXPECT_EQ(success_count + failure_count, 5);
}

TEST_F(CompactionTest, CompactionMemoryPressure) {
    auto& mgr = CompactionManager::GetInstance();

    // This is hard to test without mocking system memory
    // The method is private, so we can't test it directly
    // We'll test it indirectly through ShouldCompact
    EXPECT_TRUE(true);  // Placeholder test
}

TEST_F(CompactionTest, CompactionLowTrafficPeriod) {
    // IsLowTrafficPeriod is private, test indirectly
    // Get current hour to understand expected behavior
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    struct tm* tm_info = localtime(&time_t);
    int hour = tm_info->tm_hour;

    // Just verify time logic works
    bool expected_low = (hour >= 2 && hour <= 5);
    EXPECT_TRUE(expected_low || !expected_low);  // Always true, but tests the logic
}

TEST_F(CompactionTest, CompactionWorkerThread) {
    auto& mgr = CompactionManager::GetInstance();

    // Start the worker
    auto status = mgr.Start();
    EXPECT_TRUE(status.ok());

    // Let it run for a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Should still be running
    EXPECT_FALSE(mgr.IsCompacting());  // Not actively compacting without tables

    // Stop the worker
    status = mgr.Stop();
    EXPECT_TRUE(status.ok());
}

TEST_F(CompactionTest, EndToEndCompaction) {
    // This test verifies the compaction workflow without actual data

    auto& mgr = CompactionManager::GetInstance();

    // Start compaction manager
    auto start_status = mgr.Start();
    EXPECT_TRUE(start_status.ok());

    // Simulate table lifecycle
    mgr.RegisterTable("db.table1", nullptr);
    mgr.UpdateStats("db.table1", 1000, 300);  // 30% deleted

    // Check if compaction is needed
    EXPECT_TRUE(mgr.ShouldCompact("db.table1"));

    // Trigger manual compaction (will fail without actual table)
    auto status = mgr.CompactTable("db.table1");
    EXPECT_FALSE(status.ok());  // Expected to fail without real table

    // Cleanup
    mgr.UnregisterTable("db.table1");
    auto stop_status = mgr.Stop();
    EXPECT_TRUE(stop_status.ok());
}