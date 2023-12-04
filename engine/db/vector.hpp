#pragma once
#include <cmath>
#include <functional>
#include <string>
#include <variant>
#include <vector>

#include "utils/json.hpp"

namespace vectordb {
namespace engine {

struct SparseVectorElement {
  size_t index;
  float value;
};

// SparseVector assumes that the stored SparseVectorElement.index is increasing in the std::vector
using SparseVector = std::vector<SparseVectorElement>;
using SparseVectorPtr = std::shared_ptr<SparseVector>;
constexpr const char SparseVecObjIndicesKey[] = "indices",
                     SparseVecObjValuesKey[] = "values";

using DenseVectorPtr = float *;
using DenseVectorElement = float;

// TODO: Use a different type other than float* (e.g.: std::vector<DenseVectorPtr> or std::vector<DenseVectorElement>),
// otherwise the type is confusing
using DenseVectorColumnDataContainer = float *;
using VariableLenAttr = std::variant<std::string, SparseVectorPtr>;
using VariableLenAttrColumnContainer = std::vector<VariableLenAttr>;

using VectorPtr = std::variant<DenseVectorPtr, SparseVectorPtr>;
using SparseVecDistFunc = std::function<float(const SparseVector &, const SparseVector &)>;

float GetCosineDist(const SparseVector &v1, const SparseVector &v2);

float GetL2Dist(const SparseVector &v1, const SparseVector &v2);
float GetL2DistSqr(const SparseVector &v1, const SparseVector &v2);
float GetInnerProductDist(const SparseVector &v1, const SparseVector &v2);
float GetInnerProduct(const SparseVector &v1, const SparseVector &v2);
Json ToJson(const SparseVector &v1);
void Normalize(DenseVectorPtr v, size_t dimension);
void Normalize(SparseVector &v);

}  // namespace engine
}  // namespace vectordb
