#include "utils/concurrent_bitset.hpp"

namespace vectordb {

ConcurrentBitset::ConcurrentBitset(id_type_t capacity)
    : capacity_(capacity), bitset_((capacity + 8 - 1) >> 3) {
}

bool ConcurrentBitset::test(id_type_t id) {
  return bitset_[id >> 3].load() & (0x1 << (id & 0x7));
}

void ConcurrentBitset::set(id_type_t id) {
  bitset_[id >> 3].fetch_or(0x1 << (id & 0x7));
}

void ConcurrentBitset::clear(id_type_t id) {
  bitset_[id >> 3].fetch_and(~(0x1 << (id & 0x7)));
}

size_t ConcurrentBitset::capacity() {
  return capacity_;
}

size_t ConcurrentBitset::size() {
  return ((capacity_ + 8 - 1) >> 3);
}

uint8_t* ConcurrentBitset::data() {
  return reinterpret_cast<uint8_t*>(bitset_.data());
}

}  // namespace vectordb
