#include "db/sparse_vector.hpp"

namespace vectordb {
namespace engine {

float GetCosineDist(const SparseVector v1, size_t size1, const SparseVector v2, size_t size2) {
  float dot_prod = 0, v1_prod = 0, v2_prod = 0;
  for (int i = 0; i < size1; i++) {
    v1_prod += v1[i].value * v1[i].value;
  }
  for (int i = 0; i < size2; i++) {
    v2_prod += v2[i].value * v2[i].value;
  }
  for (int i1 = 0, i2 = 0; i1 < size1 && i2 < size2;) {
    if (v1[i1].index == v2[i2].index) {
      dot_prod += v1[i1].value * v2[i2].value;
      i1++;
      i2++;
    } else if (v1[i1].index > v2[i2].index) {
      i2++;
    } else {
      i1++;
    }
  }
  return dot_prod / std::sqrt(v1_prod * v2_prod);
}

float GetL2Dist(const SparseVector v1, size_t size1, const SparseVector v2, size_t size2) {
  float sum = 0;
  for (int i1 = 0, i2 = 0; i1 < size1 && i2 < size2;) {
    if (v1[i1].index == v2[i2].index) {
      auto diff = v1[i1].value - v2[i2].value;
      sum += diff * diff;
      i1++;
      i2++;
    } else if (v1[i1].index > v2[i2].index) {
      sum += v2[i2].value * v2[i2].value;
      i2++;
    } else {
      sum += v1[i1].value * v1[i1].value;
      i1++;
    }
  }
  return std::sqrt(sum);
}

}  // namespace engine
}  // namespace vectordb
