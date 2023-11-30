#include "db/vector.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <vector>

using vectordb::engine::GetCosineDist;
using vectordb::engine::GetInnerProductDist;
using vectordb::engine::GetL2Dist;
using vectordb::engine::GetL2DistSqr;
using vectordb::engine::SparseVector;

TEST(SparseVector, GetCosineDist) {
  SparseVector v1 = {{0, 1}, {1, 0}};
  auto totalSteps = 360;
  for (int step = 0; step < totalSteps; step++) {
    float theta = 2 * M_PI / totalSteps * step;
    // make the vector length more than 1
    auto x = std::cos(theta) * (step + 1);
    auto y = std::sin(theta) * (step + 1);
    SparseVector v2 = {{0, x}, {1, y}};
    EXPECT_FLOAT_EQ(GetCosineDist(v1, v2), -std::cos(theta));
  }
}

TEST(SparseVector, GetInnerProductDist) {
  SparseVector v1 = {{0, 1}, {1, 1}};
  auto totalSteps = 360;
  for (int step = 0; step < totalSteps; step++) {
    float theta = 2 * M_PI / totalSteps * step;
    // make the vector length more than 1
    auto x = std::cos(theta) * (step + 1);
    auto y = std::sin(theta) * (step + 1);
    SparseVector v2 = {{0, x}, {1, y}};
    EXPECT_FLOAT_EQ(GetInnerProductDist(v1, v2), 1 - (x + y));
  }
}

TEST(SparseVector, GetL2Dist) {
  SparseVector v1 = {{0, 1}, {1, 0}};
  auto totalSteps = 360;
  for (int step = 0; step < totalSteps; step++) {
    float theta = 2 * M_PI / totalSteps * step;
    // make the vector length more than 1
    auto x = std::cos(theta) * (step + 1);
    auto y = std::sin(theta) * (step + 1);
    SparseVector v2 = {{0, x}, {1, y}};

    auto dist = std::sqrt((x - 1) * (x - 1) + y * y);
    EXPECT_FLOAT_EQ(GetL2Dist(v1, v2), dist);
  }
}

TEST(SparseVector, GetL2DistSqr) {
  SparseVector v1 = {{0, 1}, {1, 0}};
  auto totalSteps = 360;
  for (int step = 0; step < totalSteps; step++) {
    float theta = 2 * M_PI / totalSteps * step;
    // make the vector length more than 1
    auto x = std::cos(theta) * (step + 1);
    auto y = std::sin(theta) * (step + 1);
    SparseVector v2 = {{0, x}, {1, y}};

    auto dist = (x - 1) * (x - 1) + y * y;
    EXPECT_FLOAT_EQ(GetL2DistSqr(v1, v2), dist);
  }

  {
    SparseVector v2 = {{0, 1}},
                 v3 = {{1, 1}, {2, 1}, {3, 1}};
    EXPECT_FLOAT_EQ(GetL2DistSqr(v2, v3), 4.0f);
  }
}

TEST(SparseVector, Normalize) {
  SparseVector v1 = {{0, 5}, {1, 0}};
  Normalize(v1);
  EXPECT_EQ(v1[0].value, 1);
  EXPECT_EQ(v1[1].value, 0);
}
