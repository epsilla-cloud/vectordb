#pragma once

namespace vectordb {
namespace engine {
namespace index {

struct Distance {
  virtual float Compare(const float* a, const float* b, unsigned size) const = 0;
};

struct DistanceL2 : public Distance {
  float Compare(const float* a, const float* b, unsigned size) const override;
};

struct DistanceIP : public Distance {
  float Compare(const float* a, const float* b, unsigned size) const override;
};

}  // namespace index
}  // namespace engine
}  // namespace vectordb
