#include "db/sparse_vector.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <vector>

using vectordb::engine::GetCosineDist;
using vectordb::engine::SparseVector;

// Demonstrate some basic assertions.
TEST(SparseVector, GetCosineDist) {
  SparseVector v1 = {{0, 1}, {1, 0}};
  auto totalSteps = 360;
  for (int step = 0; step < totalSteps; step++) {
    float theta = 2 * M_PI / totalSteps * step;
    // make the vector length more than 1
    auto x = std::cos(theta) * (step + 1);
    auto y = std::sin(theta) * (step + 1);
    SparseVector v2 = {{0, x}, {1, y}};
    EXPECT_FLOAT_EQ(GetCosineDist(v1, v2), std::cos(theta));
  }
}