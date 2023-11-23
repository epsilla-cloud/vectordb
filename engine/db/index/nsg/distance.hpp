#ifndef DB_SPARSE_VECTOR_HPP
#define DB_SPARSE_VECTOR_HPP
#pragma once
#include "db/sparse_vector.hpp"
namespace vectordb {
namespace engine {
namespace index {

struct Distance {
  virtual float Compare(const DenseVector a, const DenseVector b, unsigned size) const = 0;
  virtual float Compare(const SparseVector a, const SparseVector b) const = 0;
  virtual float Compare(const Vector a, const Vector b) const = 0;
};

struct DistanceL2 : public Distance {
  float Compare(const DenseVector a, const DenseVector b, unsigned size) const override;
  float Compare(const SparseVector a, const SparseVector b) const override;
  float Compare(const Vector a, const Vector b) const override;
};

struct DistanceIP : public Distance {
  float Compare(const DenseVector a, const DenseVector b, unsigned size) const override;
  float Compare(const SparseVector a, const SparseVector b) const override;
  float Compare(const Vector a, const Vector b) const override;
};

struct DistanceCosine : public Distance {
  float Compare(const DenseVector a, const DenseVector b, unsigned size) const override;
  float Compare(const SparseVector a, const SparseVector b) const override;
  float Compare(const Vector a, const Vector b) const override;
};

}  // namespace index
}  // namespace engine
}  // namespace vectordb
#endif