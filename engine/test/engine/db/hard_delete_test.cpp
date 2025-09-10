#include <gtest/gtest.h>
#include <cstdlib>
#include <string>

#include "config/config.hpp"
#include "db/db_server.hpp"
#include "utils/json.hpp"

using namespace vectordb;
using namespace vectordb::engine;

class HardDeleteTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Save current environment state
    const char* current_soft_delete = std::getenv("SOFT_DELETE");
    if (current_soft_delete) {
      original_soft_delete_ = std::string(current_soft_delete);
      has_original_soft_delete_ = true;
    } else {
      has_original_soft_delete_ = false;
    }
  }

  void TearDown() override {
    // Restore original environment state
    if (has_original_soft_delete_) {
      setenv("SOFT_DELETE", original_soft_delete_.c_str(), 1);
    } else {
      unsetenv("SOFT_DELETE");
    }
  }

  void SetSoftDeleteMode(bool enabled) {
    setenv("SOFT_DELETE", enabled ? "true" : "false", 1);
    // Create new config to pick up environment variable
    test_config_ = std::make_unique<Config>();
  }

private:
  std::string original_soft_delete_;
  bool has_original_soft_delete_;
  std::unique_ptr<Config> test_config_;
};

TEST_F(HardDeleteTest, ConfigurationTest) {
  // Test default configuration (soft delete enabled)
  SetSoftDeleteMode(true);
  Config config1;
  EXPECT_TRUE(config1.SoftDelete.load());

  // Test hard delete enabled
  SetSoftDeleteMode(false);
  Config config2;
  EXPECT_FALSE(config2.SoftDelete.load());
}

TEST_F(HardDeleteTest, EnvironmentVariableTest) {
  // Test various environment variable values
  const std::vector<std::pair<std::string, bool>> test_cases = {
    {"true", true},
    {"TRUE", true},
    {"True", true},
    {"1", true},
    {"yes", true},
    {"YES", true},
    {"false", false},
    {"FALSE", false},
    {"False", false},
    {"0", false},
    {"no", false},
    {"NO", false},
    {"invalid", false}, // Invalid values should default to false
    {"", false}
  };

  for (const auto& test_case : test_cases) {
    setenv("SOFT_DELETE", test_case.first.c_str(), 1);
    Config config;
    EXPECT_EQ(config.SoftDelete.load(), test_case.second) 
        << "Failed for input: '" << test_case.first << "'";
  }
}

TEST_F(HardDeleteTest, SoftDeleteModeSetter) {
  Config config;
  
  // Test setting soft delete mode
  config.setSoftDelete(false);
  EXPECT_FALSE(config.SoftDelete.load());
  
  config.setSoftDelete(true);
  EXPECT_TRUE(config.SoftDelete.load());
}

TEST_F(HardDeleteTest, JsonConfigurationTest) {
  Config config;
  bool needSwapExecutors = false;
  
  // Test updating via JSON
  vectordb::Json json_config;
  json_config.LoadFromString("{}");
  json_config.SetBool("SoftDelete", false);
  
  config.updateConfig(json_config, needSwapExecutors);
  EXPECT_FALSE(config.SoftDelete.load());
  
  // Test getting configuration as JSON
  auto config_json = config.getConfigAsJson();
  EXPECT_TRUE(config_json.HasMember("SoftDelete"));
  EXPECT_FALSE(config_json.GetBool("SoftDelete"));
}

// Mock database test to verify delete behavior selection
TEST_F(HardDeleteTest, DeleteModeSelection) {
  // This test verifies that the correct delete method is called based on configuration
  // In a real implementation, you would create test tables and verify behavior
  
  // Test soft delete mode
  SetSoftDeleteMode(true);
  Config soft_config;
  EXPECT_TRUE(soft_config.SoftDelete.load());
  
  // Test hard delete mode  
  SetSoftDeleteMode(false);
  Config hard_config;
  EXPECT_FALSE(hard_config.SoftDelete.load());
  
  // In a full implementation, here you would:
  // 1. Create a test database with sample records
  // 2. Delete records in soft mode and verify they're marked as deleted but data remains
  // 3. Delete records in hard mode and verify they're physically removed
  // 4. Verify record counts and memory usage differ between the two modes
}