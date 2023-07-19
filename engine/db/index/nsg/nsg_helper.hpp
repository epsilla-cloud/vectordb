#pragma once

#include "db/index/nsg/neighbor.hpp"

namespace vectordb {
namespace engine {
namespace index {

extern int InsertIntoPool(Neighbor* addr, unsigned K, Neighbor nn);

}  // namespace index
}  // namespace engine
}  // namespace vectordb
