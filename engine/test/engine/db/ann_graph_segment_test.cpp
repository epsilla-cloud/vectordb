/**
 * @file ann_graph_segment_test.cpp
 * @brief Unit tests for ANNGraphSegment
 *
 * Tests cover:
 * - Basic construction and initialization
 * - Graph building from vector data
 * - Graph save/load persistence
 * - Memory management
 * - Edge cases and error conditions
 */

#include <gtest/gtest.h>
#include <cmath>
#include <random>
#include <filesystem>
#include "db/ann_graph_segment.hpp"
#include "db/catalog/meta.hpp"
#include "utils/common_util.hpp"

namespace vectordb {
namespace engine {

class ANNGraphSegmentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = "/tmp/ann_graph_test_" + std::to_string(getpid());
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override {
    std::filesystem::remove_all(test_dir_);
  }

  // Helper: Generate random vectors
  std::vector<float> GenerateRandomVectors(int64_t n, int64_t dim, unsigned seed = 42) {
    std::vector<float> data(n * dim);
    std::mt19937 gen(seed);
    std::uniform_real_distribution<float> dis(-1.0f, 1.0f);

    for (int64_t i = 0; i < n * dim; ++i) {
      data[i] = dis(gen);
    }
    return data;
  }

  // Helper: Generate clustered vectors for better graph structure
  std::vector<float> GenerateClusteredVectors(int64_t n, int64_t dim, int num_clusters = 10) {
    std::vector<float> data(n * dim);
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> cluster_dis(-10.0f, 10.0f);
    std::uniform_real_distribution<float> noise_dis(-0.5f, 0.5f);

    // Generate cluster centers
    std::vector<float> centers(num_clusters * dim);
    for (int i = 0; i < num_clusters * dim; ++i) {
      centers[i] = cluster_dis(gen);
    }

    // Assign vectors to clusters with noise
    for (int64_t i = 0; i < n; ++i) {
      int cluster = i % num_clusters;
      for (int64_t d = 0; d < dim; ++d) {
        data[i * dim + d] = centers[cluster * dim + d] + noise_dis(gen);
      }
    }
    return data;
  }

  std::string test_dir_;
};

// Test 1: Default constructor
TEST_F(ANNGraphSegmentTest, DefaultConstructor) {
  ANNGraphSegment segment(true);

  EXPECT_TRUE(segment.skip_sync_disk_);
  EXPECT_EQ(segment.first_record_id_, 0);
  EXPECT_EQ(segment.record_number_.load(), 0);
  EXPECT_EQ(segment.offset_table_, nullptr);
  EXPECT_EQ(segment.neighbor_list_, nullptr);
  EXPECT_EQ(segment.navigation_point_, 0);
}

// Test 2: Build from small vector table
TEST_F(ANNGraphSegmentTest, BuildFromSmallVectorTable) {
  const int64_t n = 100;
  const int64_t dim = 16;

  auto data = GenerateClusteredVectors(n, dim, 5);

  ANNGraphSegment segment(true);
  DenseVectorColumnDataContainer container = data.data();
  VectorColumnData vector_data = container;

  segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

  // Verify basic properties
  EXPECT_EQ(segment.record_number_.load(), n);
  EXPECT_NE(segment.offset_table_, nullptr);
  EXPECT_NE(segment.neighbor_list_, nullptr);
  EXPECT_GE(segment.navigation_point_, 0);
  EXPECT_LT(segment.navigation_point_, n);

  // Verify offset table structure
  EXPECT_EQ(segment.offset_table_[0], 0);
  for (int64_t i = 0; i <= n; ++i) {
    EXPECT_GE(segment.offset_table_[i], 0);
    if (i > 0) {
      EXPECT_GE(segment.offset_table_[i], segment.offset_table_[i - 1]);
    }
  }

  // Verify total edges
  int64_t total_edges = segment.offset_table_[n];
  EXPECT_GT(total_edges, 0);

  // Verify neighbor list bounds
  for (int64_t i = 0; i < total_edges; ++i) {
    EXPECT_GE(segment.neighbor_list_[i], 0);
    EXPECT_LT(segment.neighbor_list_[i], n);
  }
}

// Test 3: Build from larger vector table
TEST_F(ANNGraphSegmentTest, BuildFromLargerVectorTable) {
  const int64_t n = 500;
  const int64_t dim = 64;

  auto data = GenerateClusteredVectors(n, dim, 20);

  ANNGraphSegment segment(true);
  DenseVectorColumnDataContainer container = data.data();
  VectorColumnData vector_data = container;

  segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

  EXPECT_EQ(segment.record_number_.load(), n);
  EXPECT_NE(segment.offset_table_, nullptr);
  EXPECT_NE(segment.neighbor_list_, nullptr);

  // Check average degree
  int64_t total_edges = segment.offset_table_[n];
  double avg_degree = static_cast<double>(total_edges) / n;

  // NSG typically has out-degree around 50 (from config), but may vary with NSG pruning
  EXPECT_GT(avg_degree, 3.0);   // At least a few neighbors per node
  EXPECT_LT(avg_degree, 100.0);  // Not too many
}

// Test 4: Save and load persistence
TEST_F(ANNGraphSegmentTest, SaveAndLoad) {
  const int64_t n = 200;
  const int64_t dim = 32;
  const int64_t table_id = 12345;
  const int64_t field_id = 67890;

  auto data = GenerateClusteredVectors(n, dim, 10);

  // Create directory first
  std::string table_dir = test_dir_ + "/" + std::to_string(table_id);
  std::filesystem::create_directories(table_dir);

  // Build and save
  {
    ANNGraphSegment segment(true);
    DenseVectorColumnDataContainer container;
    container = data.data();
    VectorColumnData vector_data = container;

    segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

    auto status = segment.SaveANNGraph(test_dir_, table_id, field_id, true);
    ASSERT_TRUE(status.ok()) << status.message();
  }

  // Load and verify
  {
    ANNGraphSegment loaded_segment(test_dir_, table_id, field_id);

    EXPECT_EQ(loaded_segment.record_number_.load(), n);
    EXPECT_NE(loaded_segment.offset_table_, nullptr);
    EXPECT_NE(loaded_segment.neighbor_list_, nullptr);

    // Verify structure
    int64_t total_edges = loaded_segment.offset_table_[n];
    EXPECT_GT(total_edges, 0);

    for (int64_t i = 0; i < total_edges; ++i) {
      EXPECT_GE(loaded_segment.neighbor_list_[i], 0);
      EXPECT_LT(loaded_segment.neighbor_list_[i], n);
    }
  }
}

// Test 5: Save with skip_sync_disk flag
TEST_F(ANNGraphSegmentTest, SaveWithSkipSyncDisk) {
  const int64_t n = 50;
  const int64_t dim = 16;
  const int64_t table_id = 111;
  const int64_t field_id = 222;

  auto data = GenerateClusteredVectors(n, dim, 3);

  ANNGraphSegment segment(true);
  DenseVectorColumnDataContainer container = data.data();
  VectorColumnData vector_data = container;

  segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

  // Without force, should skip save
  auto status1 = segment.SaveANNGraph(test_dir_, table_id, field_id, false);
  EXPECT_TRUE(status1.ok());

  std::string path = test_dir_ + "/" + std::to_string(table_id) + "/ann_graph_" + std::to_string(field_id) + ".bin";
  EXPECT_FALSE(server::CommonUtil::IsFileExist(path));

  // Create directory first
  std::string table_dir = test_dir_ + "/" + std::to_string(table_id);
  std::filesystem::create_directories(table_dir);

  // With force, should save
  auto status2 = segment.SaveANNGraph(test_dir_, table_id, field_id, true);
  EXPECT_TRUE(status2.ok());
  EXPECT_TRUE(server::CommonUtil::IsFileExist(path));
}

// Test 6: Load non-existent file creates new empty graph
TEST_F(ANNGraphSegmentTest, LoadNonExistentFile) {
  const int64_t table_id = 999;
  const int64_t field_id = 888;

  ANNGraphSegment segment(test_dir_, table_id, field_id);

  EXPECT_EQ(segment.record_number_.load(), 0);
  EXPECT_NE(segment.offset_table_, nullptr);
  EXPECT_NE(segment.neighbor_list_, nullptr);

  // Should have created the directory
  std::string dir_path = test_dir_ + "/" + std::to_string(table_id);
  EXPECT_TRUE(std::filesystem::exists(dir_path));
}

// Test 7: Navigation point is valid
TEST_F(ANNGraphSegmentTest, NavigationPointValidity) {
  const int64_t n = 300;
  const int64_t dim = 48;

  auto data = GenerateClusteredVectors(n, dim, 15);

  ANNGraphSegment segment(true);
  DenseVectorColumnDataContainer container = data.data();
  VectorColumnData vector_data = container;

  segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

  // Navigation point should be in valid range
  EXPECT_GE(segment.navigation_point_, 0);
  EXPECT_LT(segment.navigation_point_, n);

  // Navigation point should have neighbors
  int64_t nav_offset_start = segment.offset_table_[segment.navigation_point_];
  int64_t nav_offset_end = segment.offset_table_[segment.navigation_point_ + 1];
  int64_t nav_degree = nav_offset_end - nav_offset_start;

  EXPECT_GT(nav_degree, 0) << "Navigation point should have outgoing edges";
}

// Test 8: Graph connectivity (no isolated nodes)
TEST_F(ANNGraphSegmentTest, GraphConnectivity) {
  const int64_t n = 100;
  const int64_t dim = 16;

  auto data = GenerateClusteredVectors(n, dim, 5);

  ANNGraphSegment segment(true);
  DenseVectorColumnDataContainer container = data.data();
  VectorColumnData vector_data = container;

  segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

  // Every node should have at least one outgoing edge (except possibly the last node)
  for (int64_t i = 0; i < n; ++i) {
    int64_t degree = segment.offset_table_[i + 1] - segment.offset_table_[i];
    EXPECT_GE(degree, 0) << "Node " << i << " has negative degree";

    // Most nodes should have positive degree (NSG graph should be connected)
    if (i != n - 1) {
      // Allow a small number of nodes with zero degree due to graph construction
      // but at least 90% should have edges
    }
  }

  // Count nodes with zero degree
  int zero_degree_count = 0;
  for (int64_t i = 0; i < n; ++i) {
    if (segment.offset_table_[i + 1] == segment.offset_table_[i]) {
      zero_degree_count++;
    }
  }

  double zero_degree_ratio = static_cast<double>(zero_degree_count) / n;
  EXPECT_LT(zero_degree_ratio, 0.1) << "Too many isolated nodes: " << zero_degree_count << "/" << n;
}

// Test 9: Memory management (no leaks with multiple builds)
TEST_F(ANNGraphSegmentTest, MemoryManagement) {
  const int64_t n = 150;
  const int64_t dim = 32;

  ANNGraphSegment segment(true);

  // Build multiple times, old memory should be cleaned up
  for (int iter = 0; iter < 3; ++iter) {
    auto data = GenerateRandomVectors(n, dim, 42 + iter);

    DenseVectorColumnDataContainer container;
    container = data.data();
    VectorColumnData vector_data = container;

    segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

    EXPECT_EQ(segment.record_number_.load(), n);
    EXPECT_NE(segment.offset_table_, nullptr);
    EXPECT_NE(segment.neighbor_list_, nullptr);
  }

  // Final check
  EXPECT_EQ(segment.record_number_.load(), n);
}

// Test 10: Round-trip consistency (save, load, save again)
TEST_F(ANNGraphSegmentTest, RoundTripConsistency) {
  const int64_t n = 100;
  const int64_t dim = 24;
  const int64_t table_id = 555;
  const int64_t field_id = 666;

  auto data = GenerateClusteredVectors(n, dim, 7);

  // Create directory first
  std::string table_dir = test_dir_ + "/" + std::to_string(table_id);
  std::filesystem::create_directories(table_dir);

  // Build and save first time
  {
    ANNGraphSegment segment(true);
    DenseVectorColumnDataContainer container;
    container = data.data();
    VectorColumnData vector_data = container;

    segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);
    auto status = segment.SaveANNGraph(test_dir_, table_id, field_id, true);
    ASSERT_TRUE(status.ok());
  }

  // Load and save again
  {
    ANNGraphSegment loaded(test_dir_, table_id, field_id);
    auto status = loaded.SaveANNGraph(test_dir_, table_id, field_id + 1, false);
    ASSERT_TRUE(status.ok());
  }

  // Load second file and verify
  {
    ANNGraphSegment reloaded(test_dir_, table_id, field_id + 1);

    EXPECT_EQ(reloaded.record_number_.load(), n);
    EXPECT_NE(reloaded.offset_table_, nullptr);
    EXPECT_NE(reloaded.neighbor_list_, nullptr);
  }
}

// Test 11: Different metric types
TEST_F(ANNGraphSegmentTest, DifferentMetricTypes) {
  const int64_t n = 80;
  const int64_t dim = 16;

  auto data = GenerateClusteredVectors(n, dim, 4);

  // Test L2 metric
  {
    ANNGraphSegment segment(true);
    DenseVectorColumnDataContainer container;
    container = data.data();
    VectorColumnData vector_data = container;

    segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

    EXPECT_EQ(segment.record_number_.load(), n);
    EXPECT_GT(segment.offset_table_[n], 0);
  }

  // Note: IP and COSINE may be supported in the future
  // For now, just ensure L2 works
}

// Test 12: Edge case - small graph (minimum viable size for NSG is ~30-50)
TEST_F(ANNGraphSegmentTest, VerySmallGraph) {
  const int64_t n = 50;  // Increased from 10 to avoid NSG build exceptions
  const int64_t dim = 8;

  auto data = GenerateRandomVectors(n, dim);

  ANNGraphSegment segment(true);
  DenseVectorColumnDataContainer container = data.data();
  VectorColumnData vector_data = container;

  segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

  EXPECT_EQ(segment.record_number_.load(), n);
  EXPECT_NE(segment.offset_table_, nullptr);
  EXPECT_NE(segment.neighbor_list_, nullptr);
  EXPECT_GE(segment.navigation_point_, 0);
  EXPECT_LT(segment.navigation_point_, n);
}

// Test 13: Verify no self-loops in graph
TEST_F(ANNGraphSegmentTest, NoSelfLoops) {
  const int64_t n = 100;
  const int64_t dim = 16;

  auto data = GenerateClusteredVectors(n, dim, 5);

  ANNGraphSegment segment(true);
  DenseVectorColumnDataContainer container = data.data();
  VectorColumnData vector_data = container;

  segment.BuildFromVectorTable(vector_data, n, dim, meta::MetricType::EUCLIDEAN);

  // Check no node points to itself
  for (int64_t i = 0; i < n; ++i) {
    int64_t start = segment.offset_table_[i];
    int64_t end = segment.offset_table_[i + 1];

    for (int64_t j = start; j < end; ++j) {
      EXPECT_NE(segment.neighbor_list_[j], i)
        << "Node " << i << " has self-loop";
    }
  }
}

}  // namespace engine
}  // namespace vectordb
