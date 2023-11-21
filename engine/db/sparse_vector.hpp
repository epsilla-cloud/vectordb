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

using DenseVector = float *;
using SparseVector = SparseVectorElement *;
using QueryData = std::variant<DenseVector, SparseVector>;
using SparseVecDistFunc = std::function<float(const SparseVector &, size_t, const SparseVector &, size_t)>;

float GetCosineDist(const SparseVector &v1, const SparseVector &v2);

float GetL2Dist(const SparseVector &v1, const SparseVector &v2);

}  // namespace engine
}  // namespace vectordb

#endif