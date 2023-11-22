#ifndef SPARSE_VECTOR_HPP
#define SPARSE_VECTOR_HPP

#include <cmath>
#include <functional>
#include <variant>
#include <vector>

namespace vectordb {
namespace engine {

struct SparseVectorElement {
  size_t index;
  float value;
};

struct SparseVector {
  size_t size;                  // non-zero values
  SparseVectorElement data[0];  // fixed-size element
};

constexpr const char SparseVecObjIndicesKey[] = "indices",
                     SparseVecObjValuesKey[] = "values";

using DenseVector = float *;
using QueryData = std::variant<DenseVector, SparseVector>;
using SparseVecDistFunc = std::function<float(const SparseVector, const SparseVector)>;
using VariableLenAttrTable = std::vector<std::vector<unsigned char>>;
float GetCosineDist(const SparseVector &v1, const SparseVector &v2);

float GetL2Dist(const SparseVector &v1, const SparseVector &v2);

}  // namespace engine
}  // namespace vectordb

#endif