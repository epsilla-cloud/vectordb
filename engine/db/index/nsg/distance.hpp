#ifndef DB_SPARSE_VECTOR_HPP
#define DB_SPARSE_VECTOR_HPP
#pragma once
#include "db/vector.hpp"
namespace vectordb {
namespace engine {
namespace index {

struct Distance {
  virtual float Compare(const DenseVectorPtr a, const DenseVectorPtr b, unsigned size) const = 0;
  virtual float Compare(const SparseVector a, const SparseVector b) const = 0;
  virtual float Compare(const VectorPtr a, const VectorPtr b) const = 0;
};

struct DistanceL2 : public Distance {
  float Compare(const DenseVectorPtr a, const DenseVectorPtr b, unsigned size) const override;
  float Compare(const SparseVector a, const SparseVector b) const override;
  float Compare(const VectorPtr a, const VectorPtr b) const override;
};

struct DistanceIP : public Distance {
  float Compare(const DenseVectorPtr a, const DenseVectorPtr b, unsigned size) const override;
  float Compare(const SparseVector a, const SparseVector b) const override;
  float Compare(const VectorPtr a, const VectorPtr b) const override;
};

struct DistanceCosine : public Distance {
  float Compare(const DenseVectorPtr a, const DenseVectorPtr b, unsigned size) const override;
  float Compare(const SparseVector a, const SparseVector b) const override;
  float Compare(const VectorPtr a, const VectorPtr b) const override;
};

}  // namespace index
}  // namespace engine
}  // namespace vectordb
#endif