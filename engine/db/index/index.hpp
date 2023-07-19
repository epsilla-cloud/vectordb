#pragma once

#include "db/index/distances.hpp"

namespace vectordb {

template <typename MTYPE>
using DISTFUNC = MTYPE (*)(const void*, const void*, const void*);

template <typename MTYPE>
class SpaceInterface {
 public:
    // virtual void search(void *);
    virtual size_t
    get_data_size() = 0;

    virtual DISTFUNC<MTYPE>
    get_dist_func() = 0;

    virtual void*
    get_dist_func_param() = 0;

    virtual ~SpaceInterface() {
    }
};



}  // namespace vectordb

/**
 * To calculate distance of 2 vectors.
 
  float arr1[] = {1, 2, 3, 4};
  size_t arr_size1 = sizeof(arr1);
  char* buffer1 = new char[arr_size1];
  // Copy data
  std::memcpy(buffer1, arr1, arr_size1);

  float arr2[] = {9, 10, 11, 12};
  size_t arr_size2 = sizeof(arr2);
  char* buffer2 = new char[arr_size2];
  // Copy data
  std::memcpy(buffer2, arr2, arr_size2);
  

  auto space_ = new vectordb::L2Space(4);
  auto fstdistfunc_ = space_->get_dist_func();
  auto dist_func_param_ = space_->get_dist_func_param();
  auto dist = fstdistfunc_(buffer1, buffer2, dist_func_param_);
  std::cout << dist << std::endl;
*/