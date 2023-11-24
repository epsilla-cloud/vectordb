#ifndef SPARSE_VECTOR_HPP
#define SPARSE_VECTOR_HPP

#include <cmath>
#include <functional>
#include <string>
#include <variant>
#include <vector>

namespace vectordb {
namespace engine {

struct SparseVectorElement {
  size_t index;
  float value;
};

// SparseVector assumes that the stored SparseVectorElement.index is increasing in the std::vector
using SparseVector = std::vector<SparseVectorElement>;

constexpr const char SparseVecObjIndicesKey[] = "indices",
                     SparseVecObjValuesKey[] = "values";

using DenseVector = float *;
using DenseVectorElement = float;

// TODO: Use a different type other than float* (e.g.: std::vector<DenseVector> or std::vector<DenseVectorElement>),
// otherwise the type is confusing
using DenseVectorColumnDataContainer = float *;
using VariableLenAttr = std::variant<std::string, SparseVector>;
using VariableLenAttrColumnContainer = std::vector<VariableLenAttr>;

using Vector = std::variant<DenseVector, SparseVector>;
using SparseVecDistFunc = std::function<float(const SparseVector, const SparseVector)>;

float GetCosineDist(const SparseVector &v1, const SparseVector &v2);

float GetL2Dist(const SparseVector &v1, const SparseVector &v2);
float GetL2DistSqr(const SparseVector &v1, const SparseVector &v2);

SparseVector &CastToSparseVector(VariableLenAttr &vec);

}  // namespace engine
}  // namespace vectordb

#endif