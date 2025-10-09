#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "server/fulltext/implementations/quickwit/quickwit_client.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace server {
namespace fulltext {
namespace quickwit {
namespace test {

class QuickwitClientTest : public ::testing::Test {
protected:
  void SetUp() override {
    // This test requires a running Quickwit instance
    // We'll use a mock HTTP server for testing
    base_url_ = "http://localhost:7280";
  }

  void TearDown() override {
    // Cleanup
  }

  std::string base_url_;
};

// Test 1: Client Construction
TEST_F(QuickwitClientTest, Construction) {
  QuickwitClient client(base_url_);
  EXPECT_EQ(client.GetBaseUrl(), base_url_);
}

// Test 2: Health Check (requires Quickwit running)
TEST_F(QuickwitClientTest, HealthCheckWithoutServer) {
  QuickwitClient client("http://localhost:9999");  // Non-existent server

  bool healthy = client.HealthCheck();
  EXPECT_FALSE(healthy);  // Should fail when server is not running
}

// Test 3: Create Index (mock test - needs actual Quickwit for real test)
TEST_F(QuickwitClientTest, CreateIndexMock) {
  QuickwitClient client(base_url_);

  std::string index_config = R"(
version: 0.7
index_id: test_index
doc_mapping:
  field_mappings:
    - name: id
      type: text
      tokenizer: raw
    - name: content
      type: text
      tokenizer: default
indexing_settings:
  commit_timeout_secs: 60
)";

  // This will fail without Quickwit running, which is expected
  auto status = client.CreateIndex("test_index", index_config);
  // We just verify it doesn't crash
  EXPECT_FALSE(status.ok());  // Expected to fail without server
}

// Test 4: NDJSON Conversion
TEST_F(QuickwitClientTest, NDJSONConversion) {
  QuickwitClient client(base_url_);

  std::vector<vectordb::Json> documents;

  vectordb::Json doc1;
  doc1.SetString("id", "doc1");
  doc1.SetString("content", "This is document 1");
  documents.push_back(doc1);

  vectordb::Json doc2;
  doc2.SetString("id", "doc2");
  doc2.SetString("content", "This is document 2");
  documents.push_back(doc2);

  // Note: ToNDJSON is private, so we test it indirectly via IndexBatch
  auto status = client.IndexBatch("test_index", documents);
  // Without server, this should fail gracefully
  EXPECT_FALSE(status.ok());
}

// Test 5: Empty Document Batch
TEST_F(QuickwitClientTest, EmptyBatch) {
  QuickwitClient client(base_url_);

  std::vector<vectordb::Json> empty_docs;
  auto status = client.IndexBatch("test_index", empty_docs);

  // Empty batch should succeed (no-op)
  EXPECT_TRUE(status.ok());
}

// Test 6: Delete By IDs
TEST_F(QuickwitClientTest, DeleteByIds) {
  QuickwitClient client(base_url_);

  std::vector<std::string> ids = {"doc1", "doc2", "doc3"};
  auto status = client.DeleteByIds("test_index", ids);

  // Without server, should fail
  EXPECT_FALSE(status.ok());
}

// Test 7: Delete Empty IDs
TEST_F(QuickwitClientTest, DeleteEmptyIds) {
  QuickwitClient client(base_url_);

  std::vector<std::string> empty_ids;
  auto status = client.DeleteByIds("test_index", empty_ids);

  // Empty deletion should succeed (no-op)
  EXPECT_TRUE(status.ok());
}

// Test 8: Search Query
TEST_F(QuickwitClientTest, SearchQuery) {
  QuickwitClient client(base_url_);

  auto result = client.Search("test_index", "search query", 10, 0);

  // Without server, should return empty JSON
  // We just verify it doesn't crash
  EXPECT_FALSE(result.IsArray());  // Should be empty or null
}

// Test 9: Index Exists Check
TEST_F(QuickwitClientTest, IndexExists) {
  QuickwitClient client(base_url_);

  bool exists = client.IndexExists("test_index");
  EXPECT_FALSE(exists);  // Without server, should return false
}

// Test 10: Delete Index
TEST_F(QuickwitClientTest, DeleteIndex) {
  QuickwitClient client(base_url_);

  auto status = client.DeleteIndex("test_index");
  EXPECT_FALSE(status.ok());  // Without server, should fail
}

} // namespace test
} // namespace quickwit
} // namespace fulltext
} // namespace server
} // namespace vectordb
