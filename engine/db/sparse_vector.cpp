#include "db/sparse_vector.hpp"

namespace vectordb {
namespace engine {

float GetCosineDist(const SparseVector &v1, const SparseVector &v2) {
  float dot_prod = 0, v1_prod = 0, v2_prod = 0;
  for (int i = 0; i < v1.size; i++) {
    v1_prod += v1.data[i].value * v1.data[i].value;
  }
  for (int i = 0; i < v2.size; i++) {
    v2_prod += v2.data[i].value * v2.data[i].value;
  }
  for (int i1 = 0, i2 = 0; i1 < v1.size && i2 < v2.size;) {
    if (v1.data[i1].index == v2.data[i2].index) {
      dot_prod += v1.data[i1].value * v2.data[i2].value;
      i1++;
      i2++;
    } else if (v1.data[i1].index > v2.data[i2].index) {
      i2++;
    } else {
      i1++;
    }
  }
  return dot_prod / std::sqrt(v1_prod * v2_prod);
}

float GetL2Dist(const SparseVector &v1, const SparseVector &v2) {
  float sum = 0;
  for (int i1 = 0, i2 = 0; i1 < v1.size && i2 < v2.size;) {
    if (v1.data[i1].index == v2.data[i2].index) {
      auto diff = v1.data[i1].value - v2.data[i2].value;
      sum += diff * diff;
      i1++;
      i2++;
    } else if (v1.data[i1].index > v2.data[i2].index) {
      sum += v2.data[i2].value * v2.data[i2].value;
      i2++;
    } else {
      sum += v1.data[i1].value * v1.data[i1].value;
      i1++;
    }
  }
  return std::sqrt(sum);
}

}  // namespace engine
}  // namespace vectordb
