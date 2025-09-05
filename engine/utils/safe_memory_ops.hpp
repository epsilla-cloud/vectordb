#pragma once

#include <cstring>
#include <stdexcept>
#include <limits>
#include <type_traits>

namespace vectordb {
namespace utils {

/**
 * Safe memory operations with bounds checking
 */
class SafeMemoryOps {
public:
    /**
     * Safe memmove with bounds checking
     * @throws std::out_of_range if operation would overflow
     */
    template<typename T>
    static void SafeMemmove(T* dest, const T* src, size_t count, 
                            size_t dest_capacity, size_t src_capacity) {
        if (!dest || !src) {
            throw std::invalid_argument("Null pointer in memmove");
        }
        
        // Check for integer overflow
        size_t byte_count;
        if (__builtin_mul_overflow(count, sizeof(T), &byte_count)) {
            throw std::overflow_error("Size overflow in memmove");
        }
        
        // Bounds checking
        if (count > dest_capacity || count > src_capacity) {
            throw std::out_of_range("Buffer overflow in memmove");
        }
        
        // Perform the move
        std::memmove(dest, src, byte_count);
    }
    
    /**
     * Safe memcpy with bounds checking
     * @throws std::out_of_range if operation would overflow
     */
    template<typename T>
    static void SafeMemcpy(T* dest, const T* src, size_t count,
                           size_t dest_capacity, size_t src_capacity) {
        if (!dest || !src) {
            throw std::invalid_argument("Null pointer in memcpy");
        }
        
        // Check for integer overflow
        size_t byte_count;
        if (__builtin_mul_overflow(count, sizeof(T), &byte_count)) {
            throw std::overflow_error("Size overflow in memcpy");
        }
        
        // Bounds checking
        if (count > dest_capacity || count > src_capacity) {
            throw std::out_of_range("Buffer overflow in memcpy");
        }
        
        // Check for overlap (memcpy requires non-overlapping)
        const char* src_bytes = reinterpret_cast<const char*>(src);
        char* dest_bytes = reinterpret_cast<char*>(dest);
        
        if ((dest_bytes >= src_bytes && dest_bytes < src_bytes + byte_count) ||
            (src_bytes >= dest_bytes && src_bytes < dest_bytes + byte_count)) {
            throw std::logic_error("Overlapping buffers in memcpy");
        }
        
        // Perform the copy
        std::memcpy(dest, src, byte_count);
    }
    
    /**
     * Safe array bounds check
     */
    static void CheckBounds(size_t index, size_t size) {
        if (index >= size) {
            throw std::out_of_range("Array index out of bounds");
        }
    }
    
    /**
     * Safe range check
     */
    static void CheckRange(size_t start, size_t end, size_t size) {
        if (start > end || end > size) {
            throw std::out_of_range("Range out of bounds");
        }
    }
};

} // namespace utils
} // namespace vectordb