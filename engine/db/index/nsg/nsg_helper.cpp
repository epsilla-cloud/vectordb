#include "db/index/nsg/nsg_helper.hpp"

#include <cstring>

namespace vectordb {
namespace engine {
namespace index {

// TODO: impl search && insert && return insert pos. why not just find and swap?
int InsertIntoPool(Neighbor* addr, unsigned K, Neighbor nn) {
  //>> Fix: Add assert
  //    for (unsigned int i = 0; i < K; ++i) {
  //        assert(addr[i].id != nn.id);
  //    }

  // find the location to insert
  int left = 0, right = K - 1;
  if (addr[left].distance > nn.distance) {
    //>> Fix: memmove overflow, dump when vector<Neighbor> deconstruct
    memmove((char*)&addr[left + 1], &addr[left], (K - 1) * sizeof(Neighbor));
    addr[left] = nn;
    return left;
  }
  if (addr[right].distance < nn.distance) {
    return K;
  }
  while (left < right - 1) {
    int mid = (left + right) / 2;
    if (addr[mid].distance > nn.distance)
      right = mid;
    else
      left = mid;
  }
  // check equal ID

  while (left > 0) {
    if (addr[left].distance < nn.distance)  // pos is right
      break;
    if (addr[left].id == nn.id)
      return K + 1;
    left--;
  }
  if (addr[left].id == nn.id || addr[right].id == nn.id)
    return K + 1;

  //>> Fix: memmove overflow, dump when vector<Neighbor> deconstruct
  memmove((char*)&addr[right + 1], &addr[right], (K - 1 - right) * sizeof(Neighbor));
  addr[right] = nn;
  return right;
}

};  // namespace index
}  // namespace engine
}  // namespace vectordb
