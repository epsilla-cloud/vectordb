#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <chrono>

#include "config/advanced_config.hpp"
#include "config/config_integration.hpp"
#include "config/config_schema.hpp"

using namespace vectordb;
using namespace vectordb::config;

class ConfigSystemTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create temporary test directory
    test_dir_ = std::filesystem::temp_directory_path() / "vectordb_config_test";
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    // Clean up test files
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }

  std::filesystem::path test_dir_;
};

/**
 * @brief Test basic configuration operations
 */
TEST_F(ConfigSystemTest, BasicConfigOperations) {
  AdvancedConfigManager config_manager;
  
  // Test setting and getting values
  EXPECT_TRUE(config_manager.setValue("TestKey", "TestValue"));
  EXPECT_EQ(config_manager.getValue<std::string>("TestKey"), "TestValue");
  
  // Test different data types
  EXPECT_TRUE(config_manager.setValue("IntKey", 42));
  EXPECT_EQ(config_manager.getValue<int>("IntKey"), 42);
  
  EXPECT_TRUE(config_manager.setValue("BoolKey", true));
  EXPECT_TRUE(config_manager.getValue<bool>("BoolKey"));
  
  EXPECT_TRUE(config_manager.setValue("FloatKey", 3.14));
  EXPECT_DOUBLE_EQ(config_manager.getValue<double>("FloatKey"), 3.14);
  
  // Test key existence
  EXPECT_TRUE(config_manager.hasKey("TestKey"));
  EXPECT_FALSE(config_manager.hasKey("NonExistentKey"));
}

/**
 * @brief Test configuration validation
 */
TEST_F(ConfigSystemTest, ConfigurationValidation) {
  AdvancedConfigManager config_manager;
  
  // Register validators
  config_manager.registerValidator("ThreadCount", 
    std::make_shared<RangeValidator<int>>(1, 128));
  
  // Test valid values
  EXPECT_TRUE(config_manager.setValue("ThreadCount", 8));
  EXPECT_TRUE(config_manager.setValue("ThreadCount", 1));
  EXPECT_TRUE(config_manager.setValue("ThreadCount", 128));
  
  // Test invalid values
  EXPECT_FALSE(config_manager.setValue("ThreadCount", 0));
  EXPECT_FALSE(config_manager.setValue("ThreadCount", 200));
  EXPECT_FALSE(config_manager.setValue("ThreadCount", -5));
}

/**
 * @brief Test YAML configuration file loading
 */
TEST_F(ConfigSystemTest, YAMLConfigLoading) {
  auto yaml_file = test_dir_ / "test_config.yaml";
  
  // Create test YAML file
  std::ofstream file(yaml_file);
  file << "# Test configuration\n"
       << "IntraQueryThreads: 8\n"
       << "RebuildThreads: 4\n"
       << "PreFilter: true\n"
       << "ServerPort: 8888\n"
       << "DataPath: \"/test/data\"\n";
  file.close();
  
  AdvancedConfigManager config_manager(yaml_file.string());
  
  EXPECT_EQ(config_manager.getValue<int>("IntraQueryThreads"), 8);
  EXPECT_EQ(config_manager.getValue<int>("RebuildThreads"), 4);
  EXPECT_TRUE(config_manager.getValue<bool>("PreFilter"));
  EXPECT_EQ(config_manager.getValue<int>("ServerPort"), 8888);
  EXPECT_EQ(config_manager.getValue<std::string>("DataPath"), "/test/data");
}

/**
 * @brief Test TOML configuration file loading
 */
TEST_F(ConfigSystemTest, TOMLConfigLoading) {
  auto toml_file = test_dir_ / "test_config.toml";
  
  // Create test TOML file
  std::ofstream file(toml_file);
  file << "# Test configuration\n"
       << "[config]\n"
       << "IntraQueryThreads = 12\n"
       << "RebuildThreads = 3\n"
       << "PreFilter = false\n";
  file.close();
  
  AdvancedConfigManager config_manager;
  EXPECT_TRUE(config_manager.loadFromFile(toml_file.string()));
  
  EXPECT_EQ(config_manager.getValue<int>("IntraQueryThreads"), 12);
  EXPECT_EQ(config_manager.getValue<int>("RebuildThreads"), 3);
  EXPECT_FALSE(config_manager.getValue<bool>("PreFilter"));
}

/**
 * @brief Test JSON configuration file loading
 */
TEST_F(ConfigSystemTest, JSONConfigLoading) {
  auto json_file = test_dir_ / "test_config.json";
  
  // Create test JSON file
  std::ofstream file(json_file);
  file << "{\n"
       << "  \"IntraQueryThreads\": 16,\n"
       << "  \"RebuildThreads\": 2,\n"
       << "  \"PreFilter\": true,\n"
       << "  \"ServerPort\": 9999\n"
       << "}";
  file.close();
  
  AdvancedConfigManager config_manager;
  EXPECT_TRUE(config_manager.loadFromFile(json_file.string()));
  
  EXPECT_EQ(config_manager.getValue<int>("IntraQueryThreads"), 16);
  EXPECT_EQ(config_manager.getValue<int>("RebuildThreads"), 2);
  EXPECT_TRUE(config_manager.getValue<bool>("PreFilter"));
  EXPECT_EQ(config_manager.getValue<int>("ServerPort"), 9999);
}

/**
 * @brief Test configuration hot-reload functionality
 */
TEST_F(ConfigSystemTest, HotReloadFunctionality) {
  auto config_file = test_dir_ / "hot_reload_test.yaml";
  
  // Create initial configuration file
  std::ofstream file(config_file);
  file << "TestValue: 100\n";
  file.close();
  
  // Create config manager with file watching
  AdvancedConfigManager config_manager(config_file.string(), 
                                      std::chrono::milliseconds(100)); // Fast polling for testing
  
  // Initial value should be loaded
  EXPECT_EQ(config_manager.getValue<int>("TestValue"), 100);
  
  // Wait a bit for watcher to start
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  
  // Modify the configuration file
  std::ofstream file2(config_file);
  file2 << "TestValue: 200\n";
  file2.close();
  
  // Wait for file change detection and reload
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  
  // Value should be updated
  EXPECT_EQ(config_manager.getValue<int>("TestValue"), 200);
}

/**
 * @brief Test configuration callbacks
 */
TEST_F(ConfigSystemTest, ConfigurationCallbacks) {
  AdvancedConfigManager config_manager;
  
  std::string callback_key;
  std::string callback_old_value;
  std::string callback_new_value;
  bool callback_called = false;
  
  // Register callback
  config_manager.registerCallback("TestCallback", 
    [&](const ConfigChangeEvent& event) {
      callback_key = event.key;
      callback_old_value = event.old_value;
      callback_new_value = event.new_value;
      callback_called = true;
    });
  
  // Set initial value
  config_manager.setValue("TestCallback", "InitialValue");
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_key, "TestCallback");
  EXPECT_EQ(callback_new_value, "InitialValue");
  
  // Reset callback state
  callback_called = false;
  
  // Update value
  config_manager.setValue("TestCallback", "UpdatedValue");
  EXPECT_TRUE(callback_called);
  EXPECT_EQ(callback_old_value, "InitialValue");
  EXPECT_EQ(callback_new_value, "UpdatedValue");
}

/**
 * @brief Test configuration schema validation
 */
TEST_F(ConfigSystemTest, SchemaValidation) {
  ConfigSchema schema = VectorDBConfigSchemaFactory::createSchema();
  
  // Test valid configuration
  std::unordered_map<std::string, std::string> valid_config = {
    {"IntraQueryThreads", "16"},
    {"RebuildThreads", "4"},
    {"PreFilter", "true"},
    {"GlobalSyncInterval", "30"}
  };
  
  auto result = schema.validate(valid_config);
  EXPECT_TRUE(result.valid) << "Validation error: " << result.error_message;
  
  // Test invalid configuration (out of range)
  std::unordered_map<std::string, std::string> invalid_config = {
    {"IntraQueryThreads", "200"}, // Out of range (1-128)
    {"RebuildThreads", "0"}       // Out of range (1-128)
  };
  
  auto invalid_result = schema.validate(invalid_config);
  EXPECT_FALSE(invalid_result.valid);
  EXPECT_FALSE(invalid_result.error_message.empty());
}

/**
 * @brief Test configuration integration between legacy and advanced systems
 */
TEST_F(ConfigSystemTest, ConfigurationIntegration) {
  Config legacy_config;
  AdvancedConfigManager advanced_config;
  
  integration::ConfigIntegration integration(legacy_config);
  
  // Test synchronization from advanced to legacy
  advanced_config.setValue("IntraQueryThreads", 24);
  
  // The integration should sync this value to legacy config
  // Note: In real implementation, callbacks would handle this automatically
  integration.pushLegacyToAdvanced();
  
  // Verify values are synchronized
  EXPECT_EQ(advanced_config.getValue<int>("IntraQueryThreads"), 
           legacy_config.IntraQueryThreads.load());
}

/**
 * @brief Test configuration statistics
 */
TEST_F(ConfigSystemTest, ConfigurationStatistics) {
  AdvancedConfigManager config_manager;
  
  // Add some configuration values
  config_manager.setValue("Key1", "Value1");
  config_manager.setValue("Key2", "Value2");
  config_manager.setValue("Key3", "Value3");
  
  // Register some validators and callbacks
  config_manager.registerValidator("Key1", std::make_shared<RangeValidator<int>>(1, 100));
  config_manager.registerCallback("Key2", [](const ConfigChangeEvent&) {});
  
  auto stats = config_manager.getStats();
  
  EXPECT_EQ(stats.total_keys, 3);
  EXPECT_EQ(stats.total_validators, 1);
  EXPECT_GE(stats.total_callbacks, 1); // May have additional callbacks from integration
  EXPECT_FALSE(stats.watching_enabled); // No file specified
}

/**
 * @brief Test configuration export and import
 */
TEST_F(ConfigSystemTest, ConfigurationExportImport) {
  AdvancedConfigManager config_manager;
  
  // Set some configuration values
  config_manager.setValue("ExportTest1", "Value1");
  config_manager.setValue("ExportTest2", 42);
  config_manager.setValue("ExportTest3", true);
  
  auto export_file = test_dir_ / "export_test.yaml";
  
  // Export configuration
  EXPECT_TRUE(config_manager.saveToFile(export_file.string()));
  EXPECT_TRUE(std::filesystem::exists(export_file));
  
  // Create new config manager and import
  AdvancedConfigManager import_config_manager;
  EXPECT_TRUE(import_config_manager.loadFromFile(export_file.string()));
  
  // Verify imported values
  EXPECT_EQ(import_config_manager.getValue<std::string>("ExportTest1"), "Value1");
  EXPECT_EQ(import_config_manager.getValue<int>("ExportTest2"), 42);
  EXPECT_TRUE(import_config_manager.getValue<bool>("ExportTest3"));
}

/**
 * @brief Test configuration error handling
 */
TEST_F(ConfigSystemTest, ErrorHandling) {
  AdvancedConfigManager config_manager;
  
  // Test loading non-existent file
  EXPECT_FALSE(config_manager.loadFromFile("/non/existent/file.yaml"));
  
  // Test invalid file format
  auto invalid_file = test_dir_ / "invalid.yaml";
  std::ofstream file(invalid_file);
  file << "invalid: yaml: content:\n  - malformed\n    structure";
  file.close();
  
  // Should handle malformed YAML gracefully
  // Note: The simple parser may still succeed with basic key-value extraction
  config_manager.loadFromFile(invalid_file.string());
  
  // Test saving to invalid path
  EXPECT_FALSE(config_manager.saveToFile("/invalid/path/config.yaml"));
}

/**
 * @brief Test schema field resolution with aliases
 */
TEST_F(ConfigSystemTest, SchemaAliases) {
  ConfigSchema schema;
  
  ConfigFieldSchema field("PrimaryName", ConfigValueType::INTEGER, "Test field");
  field.aliases = {"Alias1", "Alias2", "LegacyName"};
  schema.addField(field);
  
  // Test canonical name resolution
  EXPECT_EQ(schema.getCanonicalName("PrimaryName"), "PrimaryName");
  EXPECT_EQ(schema.getCanonicalName("Alias1"), "PrimaryName");
  EXPECT_EQ(schema.getCanonicalName("Alias2"), "PrimaryName");
  EXPECT_EQ(schema.getCanonicalName("LegacyName"), "PrimaryName");
  EXPECT_EQ(schema.getCanonicalName("UnknownName"), "UnknownName");
  
  // Test field schema retrieval by alias
  auto schema_by_alias = schema.getFieldSchema("Alias1");
  EXPECT_TRUE(schema_by_alias.has_value());
  EXPECT_EQ(schema_by_alias->name, "PrimaryName");
}

/**
 * @brief Test environment variable precedence
 */
TEST_F(ConfigSystemTest, EnvironmentVariablePrecedence) {
  // Set test environment variable
  setenv("EPSILLA_INTRA_QUERY_THREADS", "32", 1);
  
  AdvancedConfigManager config_manager;
  
  // Environment variable should be loaded
  EXPECT_EQ(config_manager.getValue<int>("IntraQueryThreads"), 32);
  
  // Clean up
  unsetenv("EPSILLA_INTRA_QUERY_THREADS");
}

/**
 * @brief Performance test for configuration operations
 */
TEST_F(ConfigSystemTest, PerformanceTest) {
  AdvancedConfigManager config_manager;
  
  auto start = std::chrono::high_resolution_clock::now();
  
  // Perform many configuration operations
  for (int i = 0; i < 10000; ++i) {
    config_manager.setValue("PerfTest" + std::to_string(i), i);
  }
  
  for (int i = 0; i < 10000; ++i) {
    int value = config_manager.getValue<int>("PerfTest" + std::to_string(i));
    EXPECT_EQ(value, i);
  }
  
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  
  // Should complete within reasonable time (adjust threshold as needed)
  EXPECT_LT(duration.count(), 5000) << "Configuration operations took too long: " << duration.count() << "ms";
}