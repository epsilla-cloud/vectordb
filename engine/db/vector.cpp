#include "db/vector.hpp"

#include <iostream>
namespace vectordb {
namespace engine {

float GetInnerProductDist(const SparseVector &v1, const SparseVector &v2) {
  return 1.0f - GetInnerProduct(v1, v2);
}

float GetInnerProduct(const SparseVector &v1, const SparseVector &v2) {
  float dot_prod = 0;
  for (int i1 = 0, i2 = 0; i1 < v1.size() && i2 < v2.size();) {
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
  return dot_prod;
}

float GetCosineDist(const SparseVector &v1, const SparseVector &v2) {
  float dot_prod = 0, v1_prod = 0, v2_prod = 0;
  for (int i = 0; i < v1.size(); i++) {
    v1_prod += v1[i].value * v1[i].value;
  }
  for (int i = 0; i < v2.size(); i++) {
    v2_prod += v2[i].value * v2[i].value;
  }
  for (int i1 = 0, i2 = 0; i1 < v1.size() && i2 < v2.size();) {
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
  return -dot_prod / std::sqrt(v1_prod * v2_prod);
}
void Normalize(SparseVector &v) {
  float sum = 0;
  for (auto &elem : v) {
    sum += elem.value * elem.value;
  }
  sum = std::sqrt(sum);
  for (auto &elem : v) {
    elem.value /= sum;
  }
}

void Normalize(DenseVectorPtr v, size_t dimension) {
  float sum = 0;
  for (int i = 0; i < dimension; i++) {
    sum += v[i] * v[i];
  }
  sum = std::sqrt(sum);
  for (int i = 0; i < dimension; i++) {
    v[i] /= sum;
  }
}

float GetL2DistSqr(const SparseVector &v1, const SparseVector &v2) {
  float sum = 0;
  for (int i1 = 0, i2 = 0; i1 < v1.size() && i2 < v2.size();) {
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
  return sum;
}

float GetL2Dist(const SparseVector &v1, const SparseVector &v2) {
  return std::sqrt(GetL2DistSqr(v1, v2));
}

Json ToJson(const SparseVector &v1) {
  Json result, indices, values;
  indices.LoadFromString("[]");
  values.LoadFromString("[]");
  for (const auto &elem : v1) {
    indices.AddIntToArray(elem.index);
    values.AddDoubleToArray(elem.value);
  }
  result.SetObject(SparseVecObjIndicesKey, indices);
  result.SetObject(SparseVecObjValuesKey, values);
  return result;
}

}  // namespace engine
}  // namespace vectordb
