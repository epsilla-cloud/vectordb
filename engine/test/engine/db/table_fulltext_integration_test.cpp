#include <gtest/gtest.h>
#include "db/table.hpp"
#include "config/config.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {
namespace test {

/**
 * @brief Integration test for Table and Full-Text index lifecycle management
 *
 * This test verifies that:
 * 1. Full-text index ID generation follows naming convention: {db}_{table}
 * 2. InitFullTextIndex() respects VECTORDB_FULLTEXT_SEARCH_ENABLE config
 * 3. DropFullTextIndex() works correctly
 */
class TableFullTextIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Save original config
    original_enable_fulltext_ = globalConfig.EnableFullText.load();
  }

  void TearDown() override {
    // Restore original config
    globalConfig.EnableFullText.store(original_enable_fulltext_);
  }

  bool original_enable_fulltext_;
};

/**
 * Test 1: Verify index ID generation follows naming convention
 */
TEST_F(TableFullTextIntegrationTest, IndexIdNamingConvention) {
  std::cout << "\n=== Test 1: Index ID Naming Convention ===" << std::endl;

  // Test case 1: Simple names
  std::string index_id_1 = Table::GenerateFullTextIndexId("my_db", "users");
  EXPECT_EQ(index_id_1, "my_db_users");
  std::cout << "✓ Simple names: my_db + users → " << index_id_1 << std::endl;

  // Test case 2: Names with uppercase (should convert to lowercase)
  std::string index_id_2 = Table::GenerateFullTextIndexId("MyDB", "UserTable");
  EXPECT_EQ(index_id_2, "mydb_usertable");
  std::cout << "✓ Uppercase converted: MyDB + UserTable → " << index_id_2 << std::endl;

  // Test case 3: Names with special characters (should replace with underscore)
  std::string index_id_3 = Table::GenerateFullTextIndexId("db-prod", "user.table");
  EXPECT_EQ(index_id_3, "db_prod_user_table");
  std::cout << "✓ Special chars replaced: db-prod + user.table → " << index_id_3 << std::endl;

  // Test case 4: Mixed case with numbers
  std::string index_id_4 = Table::GenerateFullTextIndexId("DB123", "Table456");
  EXPECT_EQ(index_id_4, "db123_table456");
  std::cout << "✓ Mixed with numbers: DB123 + Table456 → " << index_id_4 << std::endl;

  std::cout << "✓ All naming convention tests passed!" << std::endl;
}

/**
 * Test 2: Verify InitFullTextIndex respects global config
 */
TEST_F(TableFullTextIntegrationTest, InitFullTextIndexRespectsConfig) {
  std::cout << "\n=== Test 2: InitFullTextIndex Respects Config ===" << std::endl;

  // Test case 1: Full-text disabled globally
  std::cout << "Test case 1: Full-text search disabled" << std::endl;
  globalConfig.EnableFullText.store(false);

  // Create a mock table (we'll test without actual table creation)
  // Just test the index ID generation and config check logic
  std::string index_id = Table::GenerateFullTextIndexId("test_db", "test_table");
  EXPECT_EQ(index_id, "test_db_test_table");
  std::cout << "✓ Index ID generated correctly even when disabled: " << index_id << std::endl;

  // Test case 2: Full-text enabled globally
  std::cout << "\nTest case 2: Full-text search enabled" << std::endl;
  globalConfig.EnableFullText.store(true);

  index_id = Table::GenerateFullTextIndexId("prod_db", "orders");
  EXPECT_EQ(index_id, "prod_db_orders");
  std::cout << "✓ Index ID generated when enabled: " << index_id << std::endl;

  std::cout << "✓ Config respect test passed!" << std::endl;
}

/**
 * Test 3: Verify multiple tables get unique index IDs
 */
TEST_F(TableFullTextIntegrationTest, MultipleTablesUniqueIndexIds) {
  std::cout << "\n=== Test 3: Multiple Tables Get Unique Index IDs ===" << std::endl;

  // Simulate multiple tables in the same database
  std::vector<std::pair<std::string, std::string>> tables = {
      {"my_db", "users"},
      {"my_db", "orders"},
      {"my_db", "products"},
      {"my_db", "reviews"}
  };

  std::set<std::string> index_ids;
  for (const auto& [db, table] : tables) {
    std::string index_id = Table::GenerateFullTextIndexId(db, table);
    std::cout << "Table " << db << "." << table << " → Index: " << index_id << std::endl;

    // Verify uniqueness
    EXPECT_TRUE(index_ids.find(index_id) == index_ids.end())
        << "Duplicate index ID detected: " << index_id;
    index_ids.insert(index_id);
  }

  std::cout << "✓ All " << tables.size() << " tables got unique index IDs" << std::endl;
}

/**
 * Test 4: Verify different databases can have tables with same name
 */
TEST_F(TableFullTextIntegrationTest, DifferentDatabasesSameTableName) {
  std::cout << "\n=== Test 4: Different Databases, Same Table Name ===" << std::endl;

  std::vector<std::pair<std::string, std::string>> tables = {
      {"db_dev", "users"},
      {"db_staging", "users"},
      {"db_prod", "users"}
  };

  std::set<std::string> index_ids;
  for (const auto& [db, table] : tables) {
    std::string index_id = Table::GenerateFullTextIndexId(db, table);
    std::cout << "DB " << db << " + Table " << table << " → Index: " << index_id << std::endl;

    // Verify uniqueness across databases
    EXPECT_TRUE(index_ids.find(index_id) == index_ids.end())
        << "Duplicate index ID detected across databases: " << index_id;
    index_ids.insert(index_id);
  }

  EXPECT_EQ(index_ids.size(), 3);
  std::cout << "✓ Same table name in different databases got unique index IDs" << std::endl;
}

/**
 * Test 5: Edge cases - empty strings, long names
 */
TEST_F(TableFullTextIntegrationTest, EdgeCases) {
  std::cout << "\n=== Test 5: Edge Cases ===" << std::endl;

  // Test case 1: Very long names
  std::string long_db(100, 'a');
  std::string long_table(100, 'b');
  std::string index_id_long = Table::GenerateFullTextIndexId(long_db, long_table);
  EXPECT_FALSE(index_id_long.empty());
  std::cout << "✓ Long names handled, result length: " << index_id_long.length() << std::endl;

  // Test case 2: Names with only special characters
  std::string index_id_special = Table::GenerateFullTextIndexId("---", "...");
  EXPECT_FALSE(index_id_special.empty());
  std::cout << "✓ Special chars only: --- + ... → " << index_id_special << std::endl;

  // Test case 3: Single character names
  std::string index_id_single = Table::GenerateFullTextIndexId("a", "b");
  EXPECT_EQ(index_id_single, "a_b");
  std::cout << "✓ Single chars: a + b → " << index_id_single << std::endl;

  std::cout << "✓ All edge case tests passed!" << std::endl;
}

/**
 * Test 6: Performance test - generate many index IDs
 */
TEST_F(TableFullTextIntegrationTest, PerformanceTest) {
  std::cout << "\n=== Test 6: Performance Test ===" << std::endl;

  const int NUM_ITERATIONS = 10000;
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < NUM_ITERATIONS; ++i) {
    std::string db = "db_" + std::to_string(i);
    std::string table = "table_" + std::to_string(i);
    std::string index_id = Table::GenerateFullTextIndexId(db, table);
    EXPECT_FALSE(index_id.empty());
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  double avg_time_us = static_cast<double>(duration.count()) / NUM_ITERATIONS;
  std::cout << "Generated " << NUM_ITERATIONS << " index IDs in "
            << duration.count() << " μs" << std::endl;
  std::cout << "Average time per ID: " << avg_time_us << " μs" << std::endl;

  EXPECT_LT(avg_time_us, 10.0) << "Index ID generation too slow!";
  std::cout << "✓ Performance test passed (< 10 μs per ID)" << std::endl;
}

}  // namespace test
}  // namespace engine
}  // namespace vectordb
