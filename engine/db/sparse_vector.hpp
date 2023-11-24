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

struct SparseVectorDeleter {
  void operator()(SparseVector *ptr) const {
    // Note: Deallocate memory based on the correct size
    operator delete(ptr, sizeof(SparseVector) + sizeof(float) * ptr->size);
  }
};

constexpr const char SparseVecObjIndicesKey[] = "indices",
                     SparseVecObjValuesKey[] = "values";

using DenseVector = float *;
using DenseVectorElement = float;

// TODO: Use a different type other than float* (e.g.: std::vector<DenseVector> or std::vector<DenseVectorElement>),
// otherwise the type is confusing
using DenseVectorColumnDataContainer = float *;
using VariableLenAttrDataContainer = std::vector<unsigned char>;
using VariableLenAttrColumnDataContainer = std::vector<VariableLenAttrDataContainer>;
using SparseVectorArrayDataContainer = VariableLenAttrColumnDataContainer;
using VectorArrayData = std::variant<DenseVectorColumnDataContainer, SparseVectorArrayDataContainer>;

using Vector = std::variant<DenseVector, SparseVector>;
using SparseVecDistFunc = std::function<float(const SparseVector, const SparseVector)>;

float GetCosineDist(const SparseVector &v1, const SparseVector &v2);

float GetL2Dist(const SparseVector &v1, const SparseVector &v2);
float GetL2DistSqr(const SparseVector &v1, const SparseVector &v2);

SparseVector CastToSparseVector(VariableLenAttrDataContainer &vec);

}  // namespace engine
}  // namespace vectordb

#endif