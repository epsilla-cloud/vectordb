#include <gtest/gtest.h>
#include "server/fulltext/fulltext_manager.hpp"
#include "server/fulltext/fulltext_engine.hpp"

namespace vectordb {
namespace server {
namespace fulltext {
namespace test {

class FullTextManagerTest : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}
};

// Test 1: Parse Engine Type - Quickwit
TEST_F(FullTextManagerTest, ParseEngineTypeQuickwit) {
  auto type = FullTextManager::ParseEngineType("quickwit");
  EXPECT_EQ(type, FullTextEngine::EngineType::QUICKWIT);

  type = FullTextManager::ParseEngineType("Quickwit");
  EXPECT_EQ(type, FullTextEngine::EngineType::QUICKWIT);

  type = FullTextManager::ParseEngineType("QUICKWIT");
  EXPECT_EQ(type, FullTextEngine::EngineType::QUICKWIT);
}

// Test 2: Parse Engine Type - Elasticsearch
TEST_F(FullTextManagerTest, ParseEngineTypeElasticsearch) {
  auto type = FullTextManager::ParseEngineType("elasticsearch");
  EXPECT_EQ(type, FullTextEngine::EngineType::ELASTICSEARCH);

  type = FullTextManager::ParseEngineType("es");
  EXPECT_EQ(type, FullTextEngine::EngineType::ELASTICSEARCH);
}

// Test 3: Parse Engine Type - Meilisearch
TEST_F(FullTextManagerTest, ParseEngineTypeMeilisearch) {
  auto type = FullTextManager::ParseEngineType("meilisearch");
  EXPECT_EQ(type, FullTextEngine::EngineType::MEILISEARCH);
}

// Test 4: Parse Engine Type - OpenSearch
TEST_F(FullTextManagerTest, ParseEngineTypeOpenSearch) {
  auto type = FullTextManager::ParseEngineType("opensearch");
  EXPECT_EQ(type, FullTextEngine::EngineType::OPENSEARCH);
}

// Test 5: Parse Engine Type - Unknown (defaults to Quickwit)
TEST_F(FullTextManagerTest, ParseEngineTypeUnknown) {
  auto type = FullTextManager::ParseEngineType("unknown_engine");
  EXPECT_EQ(type, FullTextEngine::EngineType::QUICKWIT);
}

// Test 6: Engine Type to String
TEST_F(FullTextManagerTest, EngineTypeToString) {
  EXPECT_EQ(FullTextManager::EngineTypeToString(FullTextEngine::EngineType::QUICKWIT), "Quickwit");
  EXPECT_EQ(FullTextManager::EngineTypeToString(FullTextEngine::EngineType::ELASTICSEARCH), "Elasticsearch");
  EXPECT_EQ(FullTextManager::EngineTypeToString(FullTextEngine::EngineType::MEILISEARCH), "Meilisearch");
  EXPECT_EQ(FullTextManager::EngineTypeToString(FullTextEngine::EngineType::OPENSEARCH), "OpenSearch");
}

// Test 7: Is Engine Supported
TEST_F(FullTextManagerTest, IsEngineSupported) {
  EXPECT_TRUE(FullTextManager::IsEngineSupported(FullTextEngine::EngineType::QUICKWIT));
  EXPECT_FALSE(FullTextManager::IsEngineSupported(FullTextEngine::EngineType::ELASTICSEARCH));
  EXPECT_FALSE(FullTextManager::IsEngineSupported(FullTextEngine::EngineType::MEILISEARCH));
  EXPECT_FALSE(FullTextManager::IsEngineSupported(FullTextEngine::EngineType::OPENSEARCH));
}

// Test 8: Get Supported Engines
TEST_F(FullTextManagerTest, GetSupportedEngines) {
  auto engines = FullTextManager::GetSupportedEngines();
  EXPECT_EQ(engines.size(), 1);
  EXPECT_EQ(engines[0], "Quickwit");
}

// Test 9: Create Quickwit Engine (will fail without actual binary)
TEST_F(FullTextManagerTest, CreateQuickwitEngine) {
  FullTextEngineConfig config;
  config.engine_type = FullTextEngine::EngineType::QUICKWIT;
  config.binary_path = "/nonexistent/quickwit";
  config.data_dir = "/tmp/test_fulltext";
  config.port = 7280;

  auto engine = FullTextManager::CreateEngine(config);

  ASSERT_NE(engine, nullptr);
  EXPECT_EQ(engine->GetEngineType(), FullTextEngine::EngineType::QUICKWIT);
  EXPECT_EQ(engine->GetEngineName(), "Quickwit");
  EXPECT_FALSE(engine->IsRunning());  // Not started yet
}

// Test 10: Create Unsupported Engine
TEST_F(FullTextManagerTest, CreateUnsupportedEngine) {
  FullTextEngineConfig config;
  config.engine_type = FullTextEngine::EngineType::ELASTICSEARCH;

  auto engine = FullTextManager::CreateEngine(config);

  EXPECT_EQ(engine, nullptr);  // Should fail for unsupported engine
}

// Test 11: Engine Configuration
TEST_F(FullTextManagerTest, EngineConfiguration) {
  FullTextEngineConfig config;
  config.engine_type = FullTextEngine::EngineType::QUICKWIT;
  config.binary_path = "/usr/local/bin/quickwit";
  config.config_path = "/path/to/config.yaml";
  config.data_dir = "./fulltext_data";
  config.port = 7280;
  config.endpoint = "http://localhost:9200";
  config.api_key = "test_key";

  EXPECT_EQ(config.engine_type, FullTextEngine::EngineType::QUICKWIT);
  EXPECT_EQ(config.binary_path, "/usr/local/bin/quickwit");
  EXPECT_EQ(config.port, 7280);
  EXPECT_EQ(config.endpoint, "http://localhost:9200");
  EXPECT_EQ(config.api_key, "test_key");
}

} // namespace test
} // namespace fulltext
} // namespace server
} // namespace vectordb
