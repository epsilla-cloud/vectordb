#include "utils/status.hpp"

#include <gtest/gtest.h>
#include <thread>
#include <vector>

using namespace vectordb;

class StatusTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(StatusTest, DefaultConstructor) {
  Status status;
  
  // Default should be OK
  EXPECT_TRUE(status.IsOk());
  EXPECT_EQ(status.code(), StatusCode::OK);
  EXPECT_TRUE(status.message().empty());
}

TEST_F(StatusTest, OkStatus) {
  Status status = Status::OK();
  
  EXPECT_TRUE(status.IsOk());
  EXPECT_EQ(status.code(), StatusCode::OK);
  EXPECT_TRUE(status.message().empty());
}

TEST_F(StatusTest, ErrorStatus) {
  Status status = Status::Error(StatusCode::INVALID_ARGUMENT, "Test error message");
  
  EXPECT_FALSE(status.IsOk());
  EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(status.message(), "Test error message");
}

TEST_F(StatusTest, ConvenienceConstructors) {
  // Test various error types
  auto invalid_arg = Status::InvalidArgument("Invalid argument");
  EXPECT_EQ(invalid_arg.code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(invalid_arg.message(), "Invalid argument");
  
  auto not_found = Status::NotFound("Resource not found");
  EXPECT_EQ(not_found.code(), StatusCode::NOT_FOUND);
  EXPECT_EQ(not_found.message(), "Resource not found");
  
  auto internal_error = Status::InternalError("Internal error occurred");
  EXPECT_EQ(internal_error.code(), StatusCode::INTERNAL_ERROR);
  EXPECT_EQ(internal_error.message(), "Internal error occurred");
  
  auto already_exists = Status::AlreadyExists("Resource already exists");
  EXPECT_EQ(already_exists.code(), StatusCode::ALREADY_EXISTS);
  EXPECT_EQ(already_exists.message(), "Resource already exists");
}

TEST_F(StatusTest, CopyConstructor) {
  Status original = Status::Error(StatusCode::NOT_FOUND, "Original message");
  Status copy(original);
  
  EXPECT_EQ(copy.code(), original.code());
  EXPECT_EQ(copy.message(), original.message());
  EXPECT_FALSE(copy.IsOk());
}

TEST_F(StatusTest, MoveConstructor) {
  Status original = Status::Error(StatusCode::INTERNAL_ERROR, "Original message");
  Status moved(std::move(original));
  
  EXPECT_EQ(moved.code(), StatusCode::INTERNAL_ERROR);
  EXPECT_EQ(moved.message(), "Original message");
  EXPECT_FALSE(moved.IsOk());
}

TEST_F(StatusTest, AssignmentOperator) {
  Status status1 = Status::OK();
  Status status2 = Status::Error(StatusCode::INVALID_ARGUMENT, "Error message");
  
  status1 = status2;
  
  EXPECT_EQ(status1.code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(status1.message(), "Error message");
  EXPECT_FALSE(status1.IsOk());
}

TEST_F(StatusTest, MoveAssignmentOperator) {
  Status status1 = Status::OK();
  Status status2 = Status::Error(StatusCode::NOT_FOUND, "Not found message");
  
  status1 = std::move(status2);
  
  EXPECT_EQ(status1.code(), StatusCode::NOT_FOUND);
  EXPECT_EQ(status1.message(), "Not found message");
  EXPECT_FALSE(status1.IsOk());
}

TEST_F(StatusTest, EqualityOperator) {
  Status status1 = Status::Error(StatusCode::INVALID_ARGUMENT, "Same message");
  Status status2 = Status::Error(StatusCode::INVALID_ARGUMENT, "Same message");
  Status status3 = Status::Error(StatusCode::NOT_FOUND, "Different message");
  Status status4 = Status::Error(StatusCode::INVALID_ARGUMENT, "Different message");
  
  EXPECT_EQ(status1, status2);
  EXPECT_NE(status1, status3);
  EXPECT_NE(status1, status4);
}

TEST_F(StatusTest, ToStringMethod) {
  Status ok_status = Status::OK();
  EXPECT_EQ(ok_status.ToString(), "OK");
  
  Status error_status = Status::Error(StatusCode::INVALID_ARGUMENT, "Test error");
  std::string error_string = error_status.ToString();
  EXPECT_NE(error_string.find("INVALID_ARGUMENT"), std::string::npos);
  EXPECT_NE(error_string.find("Test error"), std::string::npos);
}

TEST_F(StatusTest, StatusCodes) {
  // Test all status codes
  Status ok = Status::OK();
  EXPECT_EQ(ok.code(), StatusCode::OK);
  
  Status invalid_arg = Status::InvalidArgument("msg");
  EXPECT_EQ(invalid_arg.code(), StatusCode::INVALID_ARGUMENT);
  
  Status not_found = Status::NotFound("msg");
  EXPECT_EQ(not_found.code(), StatusCode::NOT_FOUND);
  
  Status already_exists = Status::AlreadyExists("msg");
  EXPECT_EQ(already_exists.code(), StatusCode::ALREADY_EXISTS);
  
  Status internal_error = Status::InternalError("msg");
  EXPECT_EQ(internal_error.code(), StatusCode::INTERNAL_ERROR);
}

TEST_F(StatusTest, EmptyMessage) {
  Status status = Status::Error(StatusCode::INVALID_ARGUMENT, "");
  
  EXPECT_FALSE(status.IsOk());
  EXPECT_EQ(status.code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_TRUE(status.message().empty());
}

TEST_F(StatusTest, LongMessage) {
  std::string long_message(10000, 'A');  // 10K character message
  Status status = Status::Error(StatusCode::INTERNAL_ERROR, long_message);
  
  EXPECT_FALSE(status.IsOk());
  EXPECT_EQ(status.code(), StatusCode::INTERNAL_ERROR);
  EXPECT_EQ(status.message(), long_message);
  EXPECT_EQ(status.message().length(), 10000);
}

TEST_F(StatusTest, ThreadSafety) {
  // Test that Status objects are thread-safe for read operations
  Status shared_status = Status::Error(StatusCode::NOT_FOUND, "Shared error");
  
  const int num_threads = 8;
  const int iterations = 1000;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};
  
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&shared_status, iterations, &success_count]() {
      for (int j = 0; j < iterations; ++j) {
        // Read operations should be thread-safe
        auto code = shared_status.code();
        auto message = shared_status.message();
        bool is_ok = shared_status.IsOk();
        
        if (code == StatusCode::NOT_FOUND && 
            message == "Shared error" && 
            !is_ok) {
          success_count++;
        }
      }
    });
  }
  
  for (auto& t : threads) {
    t.join();
  }
  
  EXPECT_EQ(success_count.load(), num_threads * iterations);
}

TEST_F(StatusTest, StatusChaining) {
  // Test function that returns Status
  auto create_error = [](bool should_fail) -> Status {
    if (should_fail) {
      return Status::InvalidArgument("Function failed");
    }
    return Status::OK();
  };
  
  // Test success case
  Status success_result = create_error(false);
  EXPECT_TRUE(success_result.IsOk());
  
  // Test failure case
  Status failure_result = create_error(true);
  EXPECT_FALSE(failure_result.IsOk());
  EXPECT_EQ(failure_result.code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(failure_result.message(), "Function failed");
}

TEST_F(StatusTest, StatusInContainers) {
  // Test Status objects in STL containers
  std::vector<Status> status_vector;
  status_vector.push_back(Status::OK());
  status_vector.push_back(Status::NotFound("Item 1"));
  status_vector.push_back(Status::InvalidArgument("Item 2"));
  status_vector.push_back(Status::InternalError("Item 3"));
  
  EXPECT_EQ(status_vector.size(), 4);
  
  EXPECT_TRUE(status_vector[0].IsOk());
  EXPECT_EQ(status_vector[1].code(), StatusCode::NOT_FOUND);
  EXPECT_EQ(status_vector[2].code(), StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(status_vector[3].code(), StatusCode::INTERNAL_ERROR);
  
  // Test Status in map
  std::unordered_map<std::string, Status> status_map;
  status_map["success"] = Status::OK();
  status_map["error"] = Status::Error(StatusCode::NOT_FOUND, "Map error");
  
  EXPECT_TRUE(status_map["success"].IsOk());
  EXPECT_FALSE(status_map["error"].IsOk());
}

// Performance test
TEST_F(StatusTest, DISABLED_PerformanceTest) {
  const int iterations = 1000000;
  
  auto start = std::chrono::high_resolution_clock::now();
  
  for (int i = 0; i < iterations; ++i) {
    Status status = Status::Error(StatusCode::INVALID_ARGUMENT, "Performance test");
    volatile bool is_ok = status.IsOk();  // Prevent optimization
    (void)is_ok;
  }
  
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  
  // Should be able to create 1M Status objects in reasonable time (< 1 second)
  EXPECT_LT(duration.count(), 1000000);  // Less than 1 second
}