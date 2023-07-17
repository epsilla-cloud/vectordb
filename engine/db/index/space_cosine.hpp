#pragma once

#include "db/index/index.hpp"
// #include "db/index/hook.hpp"

namespace vectordb {

static float
Cosine(const void* pVect1, const void* pVect2, const void* qty_ptr) {
  return vectordb::fvec_inner_product((const float*)pVect1, (const float*)pVect2, *((size_t*)qty_ptr));
}

static float
CosineDistance(const void* pVect1, const void* pVect2, const void* qty_ptr) {
  return -1.0f * Cosine(pVect1, pVect2, qty_ptr);
}

class CosineSpace : public SpaceInterface<float> {
  DISTFUNC<float> fstdistfunc_;
  size_t data_size_;
  size_t dim_;

 public:
  CosineSpace(size_t dim) {
    fstdistfunc_ = CosineDistance;
    dim_ = dim;
    data_size_ = dim * sizeof(float);
  }

  size_t
  get_data_size() {
    return data_size_;
  }

  DISTFUNC<float>
  get_dist_func() {
    return fstdistfunc_;
  }

  void*
  get_dist_func_param() {
    return &dim_;
  }

  ~CosineSpace() {
  }
};

}  // namespace vectordb
