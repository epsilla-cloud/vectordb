#include <gtest/gtest.h>
#include <fstream>
#include <thread>
#include <chrono>

#include "config/advanced_config.hpp"
//#include "config/config_integration.hpp"
#include "utils/enhanced_error.hpp"
#include "utils/error.hpp"

using namespace vectordb;
using namespace vectordb::config;
using namespace vectordb::error;
//using namespace vectordb::integration;

class AdvancedConfigTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a test configuration file
    createTestConfig();
  }
  
  void TearDown() override {
    // Clean up test files
    std::remove("test_config.yaml");
    std::remove("test_config.toml");
    std::remove("test_config.json");
    
    // Reset global config
    if (g_advanced_config) {
      delete g_advanced_config;
      g_advanced_config = nullptr;
    }
  }
  
  void createTestConfig() {
    // Create YAML test config
    std::ofstream yaml_file("test_config.yaml");
    yaml_file << "IntraQueryThreads: 8\n";
    yaml_file << "RebuildThreads: 2\n";
    yaml_file << "PreFilter: true\n";
    yaml_file << "SearchQueueSize: 2000\n";
    yaml_file.close();
    
    // Create TOML test config
    std::ofstream toml_file("test_config.toml");
    toml_file << "[config]\n";
    toml_file << "IntraQueryThreads = 12\n";
    toml_file << "RebuildThreads = 3\n";
    toml_file << "PreFilter = false\n";
    toml_file.close();
    
    // Create JSON test config
    std::ofstream json_file("test_config.json");
    json_file << R"({"IntraQueryThreads": 6, "RebuildThreads": 1, "PreFilter": true})";
    json_file.close();
  }
};

TEST_F(AdvancedConfigTest, BasicConfiguration) {
  AdvancedConfigManager config_manager;
  
  // Test basic setValue and getValue
  EXPECT_TRUE(config_manager.setValue("TestKey", "TestValue"));
  EXPECT_EQ(config_manager.getValue<std::string>("TestKey"), "TestValue");
  
  // Test type conversion
  EXPECT_TRUE(config_manager.setValue("IntKey", 42));
  EXPECT_EQ(config_manager.getValue<int>("IntKey"), 42);
  
  EXPECT_TRUE(config_manager.setValue("BoolKey", true));
  EXPECT_TRUE(config_manager.getValue<bool>("BoolKey"));
}

TEST_F(AdvancedConfigTest, Validation) {
  AdvancedConfigManager config_manager;
  
  // Test built-in validators
  EXPECT_TRUE(config_manager.setValue("IntraQueryThreads", 16));
  EXPECT_FALSE(config_manager.setValue("IntraQueryThreads", 0));    // Below range
  EXPECT_FALSE(config_manager.setValue("IntraQueryThreads", 200));  // Above range
  
  EXPECT_TRUE(config_manager.setValue("SearchQueueSize", 1000));
  EXPECT_FALSE(config_manager.setValue("SearchQueueSize", 50));     // Below range
}

TEST_F(AdvancedConfigTest, YamlConfiguration) {
  AdvancedConfigManager config_manager;
  
  EXPECT_TRUE(config_manager.loadFromFile("test_config.yaml"));
  
  EXPECT_EQ(config_manager.getValue<int>("IntraQueryThreads"), 8);
  EXPECT_EQ(config_manager.getValue<int>("RebuildThreads"), 2);
  EXPECT_TRUE(config_manager.getValue<bool>("PreFilter"));
  EXPECT_EQ(config_manager.getValue<int>("SearchQueueSize"), 2000);
}

TEST_F(AdvancedConfigTest, TomlConfiguration) {
  AdvancedConfigManager config_manager;
  
  EXPECT_TRUE(config_manager.loadFromFile("test_config.toml"));
  
  EXPECT_EQ(config_manager.getValue<int>("IntraQueryThreads"), 12);
  EXPECT_EQ(config_manager.getValue<int>("RebuildThreads"), 3);
  EXPECT_FALSE(config_manager.getValue<bool>("PreFilter"));
}

TEST_F(AdvancedConfigTest, JsonConfiguration) {
  AdvancedConfigManager config_manager;
  
  EXPECT_TRUE(config_manager.loadFromFile("test_config.json"));
  
  EXPECT_EQ(config_manager.getValue<int>("IntraQueryThreads"), 6);
  EXPECT_EQ(config_manager.getValue<int>("RebuildThreads"), 1);
  EXPECT_TRUE(config_manager.getValue<bool>("PreFilter"));
}

TEST_F(AdvancedConfigTest, ChangeCallbacks) {
  AdvancedConfigManager config_manager;
  
  std::string callback_key;
  std::string callback_new_value;
  bool callback_called = false;
  
  config_manager.registerCallback("TestCallback", 
    [&](const ConfigChangeEvent& event) {
      callback_key = event.key;
      callback_new_value = event.new_value;
      callback_called = true;
    });
  
  config_manager.setValue("TestCallback", "NewValue");
  
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_key, "TestCallback");
  EXPECT_EQ(callback_new_value, "NewValue");
}

TEST_F(AdvancedConfigTest, SaveConfiguration) {
  AdvancedConfigManager config_manager;
  
  config_manager.setValue("TestSave1", "Value1");
  config_manager.setValue("TestSave2", 42);
  config_manager.setValue("TestSave3", true);
  
  // Test YAML save
  EXPECT_TRUE(config_manager.saveToFile("test_save.yaml"));
  
  // Load and verify
  AdvancedConfigManager verify_manager;
  EXPECT_TRUE(verify_manager.loadFromFile("test_save.yaml"));
  EXPECT_EQ(verify_manager.getValue<std::string>("TestSave1"), "Value1");
  EXPECT_EQ(verify_manager.getValue<int>("TestSave2"), 42);
  EXPECT_TRUE(verify_manager.getValue<bool>("TestSave3"));
  
  std::remove("test_save.yaml");
}

class EnhancedErrorTest : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(EnhancedErrorTest, BasicErrorCreation) {
  auto status = EnhancedStatus::DatabaseError(1, "Test database error");
  
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code().category, ErrorCategory::DATABASE);
  EXPECT_EQ(status.code().specific_code, 1);
  EXPECT_EQ(status.message(), "Test database error");
}

TEST_F(EnhancedErrorTest, ErrorContext) {
  ErrorContext context("testFunction", "test.cpp", 42, "test_operation");
  context.addMetadata("key1", "value1")
         .addMetadata("key2", "value2");
  
  auto status = EnhancedStatus::ValidationError(1, "Test validation error", context);
  
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.contextStack().size(), 1);
  EXPECT_EQ(status.contextStack()[0].function_name, "testFunction");
  EXPECT_EQ(status.contextStack()[0].line_number, 42);
  EXPECT_EQ(status.contextStack()[0].metadata.at("key1"), "value1");
}

TEST_F(EnhancedErrorTest, ErrorRecovery) {
  bool recovery_attempted = false;
  
  auto retry_action = std::make_shared<RetryRecoveryAction>(
    3, 
    std::chrono::milliseconds(10), 
    [&recovery_attempted]() { 
      recovery_attempted = true; 
      return true; 
    });
  
  auto status = EnhancedStatus::NetworkError(1, "Network error");
  status.addRecoveryAction(retry_action);
  
  EXPECT_TRUE(status.attemptRecovery());
  EXPECT_TRUE(recovery_attempted);
}

TEST_F(EnhancedErrorTest, ErrorRegistry) {
  auto& registry = ErrorRegistry::instance();
  
  EnhancedErrorCode custom_code(ErrorCategory::DATABASE, 1, 999);
  registry.registerError(custom_code, "Custom database error");
  
  auto status = registry.createStatus(custom_code, "", ERROR_CONTEXT());
  
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.message(), "Custom database error");
}

/*
class ConfigIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Initialize with a clean state
  }
  
  void TearDown() override {
    if (g_advanced_config) {
      delete g_advanced_config;
      g_advanced_config = nullptr;
    }
  }
};

TEST_F(ConfigIntegrationTest, LegacyIntegration) {
  InitializeAdvancedConfig();
  Config legacy_config;
  ConfigIntegration integration(legacy_config);
  
  // Test that changes in advanced config propagate to legacy config
  auto& advanced = GetAdvancedConfig();
  advanced.setValue("IntraQueryThreads", 8);
  
  // Give some time for the callback to execute
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  
  EXPECT_EQ(legacy_config.IntraQueryThreads.load(), 8);
}

TEST_F(ConfigIntegrationTest, ErrorConversion) {
  // Test legacy to enhanced conversion
  vectordb::Status legacy_status(DB_NOT_FOUND, "Table not found");
  auto enhanced = ErrorIntegration::convertLegacyStatus(legacy_status);
  
  EXPECT_FALSE(enhanced.ok());
  EXPECT_EQ(enhanced.code().category, ErrorCategory::DATABASE);
  EXPECT_EQ(enhanced.message(), "Table not found");
  
  // Test enhanced to legacy conversion
  auto enhanced_status = EnhancedStatus::ValidationError(1, "Invalid input");
  auto converted_legacy = ErrorIntegration::convertToLegacyStatus(enhanced_status);
  
  EXPECT_FALSE(converted_legacy.ok());
  EXPECT_EQ(converted_legacy.message(), "Invalid input");
}

TEST_F(ConfigIntegrationTest, ExampleConfigGeneration) {
  InitializeAdvancedConfig();
  Config legacy_config;
  ConfigIntegration integration(legacy_config);
  
  // This should create example configuration files
  EXPECT_NO_THROW(integration.createExampleConfigs());
  
  // Check if files were created (they should exist)
  std::ifstream yaml_check("vectordb_example.yaml");
  std::ifstream toml_check("vectordb_example.toml");
  std::ifstream json_check("vectordb_example.json");
  
  EXPECT_TRUE(yaml_check.good());
  EXPECT_TRUE(toml_check.good());
  EXPECT_TRUE(json_check.good());
  
  yaml_check.close();
  toml_check.close();
  json_check.close();
  
  // Clean up
  std::remove("vectordb_example.yaml");
  std::remove("vectordb_example.toml");
  std::remove("vectordb_example.json");
}
*/

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}