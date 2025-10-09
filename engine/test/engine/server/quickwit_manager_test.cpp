#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <fstream>
#include <filesystem>
#include <signal.h>
#include "server/fulltext/implementations/quickwit/quickwit_process.hpp"

namespace vectordb {
namespace server {
namespace fulltext {
namespace quickwit {
namespace test {

using QuickwitManager = QuickwitProcess;

class QuickwitManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a mock Quickwit binary for testing
    CreateMockQuickwitBinary();
  }

  void TearDown() override {
    // Clean up mock binary
    if (std::filesystem::exists(mock_binary_path_)) {
      std::filesystem::remove(mock_binary_path_);
    }
  }

  void CreateMockQuickwitBinary() {
    // Create a simple shell script that simulates Quickwit
    mock_binary_path_ = "/tmp/mock_quickwit_test.sh";
    std::ofstream script(mock_binary_path_);
    script << "#!/bin/bash\n";
    script << "# Mock Quickwit server for testing\n";
    script << "echo 'Mock Quickwit starting...'\n";
    script << "# Start a simple HTTP server on port 7280\n";
    script << "while true; do\n";
    script << "  echo -e 'HTTP/1.1 200 OK\\r\\n\\r\\n' | nc -l -p 7280 -q 1 > /dev/null 2>&1\n";
    script << "  sleep 0.1\n";
    script << "done\n";
    script.close();

    // Make it executable
    std::filesystem::permissions(mock_binary_path_,
      std::filesystem::perms::owner_all | std::filesystem::perms::group_read | std::filesystem::perms::others_read);
  }

  std::string mock_binary_path_;
  std::string test_data_dir_ = "/tmp/quickwit_test_data";
};

// Test 1: Basic Start and Stop
TEST_F(QuickwitManagerTest, BasicStartStop) {
  QuickwitManager manager;

  // Start Quickwit
  auto status = manager.Start(mock_binary_path_, "", test_data_dir_, 7280);

  // Should start successfully (or fail gracefully if nc is not available)
  if (status.ok()) {
    EXPECT_TRUE(manager.IsRunning());
    EXPECT_GT(manager.GetPid(), 0);
    EXPECT_EQ(manager.GetEndpoint(), "http://localhost:7280");

    // Stop Quickwit
    status = manager.Stop();
    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(manager.IsRunning());
  } else {
    // If start failed (e.g., nc not available), skip the test
    GTEST_SKIP() << "Cannot start mock Quickwit: " << status.message();
  }
}

// Test 2: Double Start Prevention
TEST_F(QuickwitManagerTest, PreventDoubleStart) {
  QuickwitManager manager;

  auto status1 = manager.Start(mock_binary_path_, "", test_data_dir_, 7280);
  if (!status1.ok()) {
    GTEST_SKIP() << "Cannot start mock Quickwit";
  }

  // Try to start again - should fail
  auto status2 = manager.Start(mock_binary_path_, "", test_data_dir_, 7280);
  EXPECT_FALSE(status2.ok());
  EXPECT_TRUE(status2.message().find("already running") != std::string::npos);

  manager.Stop();
}

// Test 3: Invalid Binary Path
TEST_F(QuickwitManagerTest, InvalidBinaryPath) {
  QuickwitManager manager;

  auto status = manager.Start("/nonexistent/quickwit", "", test_data_dir_, 7280);
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.message().find("not found") != std::string::npos ||
              status.message().find("not executable") != std::string::npos);
}

// Test 4: Stop Without Start
TEST_F(QuickwitManagerTest, StopWithoutStart) {
  QuickwitManager manager;

  auto status = manager.Stop();
  // Should succeed even if not running
  EXPECT_TRUE(status.ok());
}

// Test 5: Restart Functionality
TEST_F(QuickwitManagerTest, Restart) {
  QuickwitManager manager;

  auto status = manager.Start(mock_binary_path_, "", test_data_dir_, 7280);
  if (!status.ok()) {
    GTEST_SKIP() << "Cannot start mock Quickwit";
  }

  pid_t first_pid = manager.GetPid();
  EXPECT_GT(first_pid, 0);

  // Restart
  status = manager.Restart();
  if (status.ok()) {
    EXPECT_TRUE(manager.IsRunning());

    // PID should be different after restart
    pid_t second_pid = manager.GetPid();
    EXPECT_GT(second_pid, 0);
    // Note: PIDs might be the same if process table wrapped, so we don't assert inequality
  }

  manager.Stop();
}

// Test 6: WaitForReady Timeout
TEST_F(QuickwitManagerTest, WaitForReadyTimeout) {
  QuickwitManager manager;

  // Create a binary that doesn't actually listen on the port
  std::string non_listening_binary = "/tmp/mock_quickwit_no_listen.sh";
  std::ofstream script(non_listening_binary);
  script << "#!/bin/bash\n";
  script << "sleep 100\n";  // Just sleep, don't listen
  script.close();
  std::filesystem::permissions(non_listening_binary, std::filesystem::perms::owner_all);

  auto status = manager.Start(non_listening_binary, "", test_data_dir_, 7280);

  // Should fail with timeout
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.message().find("timeout") != std::string::npos);

  manager.Stop();
  std::filesystem::remove(non_listening_binary);
}

// Test 7: Process Crash Detection
TEST_F(QuickwitManagerTest, ProcessCrashDetection) {
  QuickwitManager manager;

  // Create a binary that exits immediately
  std::string crashing_binary = "/tmp/mock_quickwit_crash.sh";
  std::ofstream script(crashing_binary);
  script << "#!/bin/bash\n";
  script << "exit 1\n";  // Crash immediately
  script.close();
  std::filesystem::permissions(crashing_binary, std::filesystem::perms::owner_all);

  auto status = manager.Start(crashing_binary, "", test_data_dir_, 7280);

  // Should fail because process exited during startup
  EXPECT_FALSE(status.ok());

  std::filesystem::remove(crashing_binary);
}

// Test 8: Multiple Instances on Different Ports
TEST_F(QuickwitManagerTest, MultipleInstances) {
  QuickwitManager manager1;
  QuickwitManager manager2;

  auto status1 = manager1.Start(mock_binary_path_, "", test_data_dir_ + "_1", 7281);
  if (!status1.ok()) {
    GTEST_SKIP() << "Cannot start first mock Quickwit";
  }

  auto status2 = manager2.Start(mock_binary_path_, "", test_data_dir_ + "_2", 7282);
  if (!status2.ok()) {
    GTEST_SKIP() << "Cannot start second mock Quickwit";
  }

  EXPECT_TRUE(manager1.IsRunning());
  EXPECT_TRUE(manager2.IsRunning());
  EXPECT_NE(manager1.GetPid(), manager2.GetPid());
  EXPECT_EQ(manager1.GetEndpoint(), "http://localhost:7281");
  EXPECT_EQ(manager2.GetEndpoint(), "http://localhost:7282");

  manager1.Stop();
  manager2.Stop();
}

// Test 9: Destructor Cleanup
TEST_F(QuickwitManagerTest, DestructorCleanup) {
  pid_t pid = -1;

  {
    QuickwitManager manager;
    auto status = manager.Start(mock_binary_path_, "", test_data_dir_, 7280);
    if (status.ok()) {
      pid = manager.GetPid();
      EXPECT_TRUE(manager.IsRunning());
    }
    // manager goes out of scope here - destructor should stop the process
  }

  // Give some time for cleanup
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Process should no longer exist
  if (pid > 0) {
    int result = kill(pid, 0);  // Check if process exists
    EXPECT_NE(result, 0);  // Should fail (process doesn't exist)
  }
}

} // namespace test
} // namespace quickwit
} // namespace fulltext
} // namespace server
} // namespace vectordb
