# SIMD Distance Computation

## Overview

This directory contains optimized distance computation implementations using SIMD (Single Instruction, Multiple Data) instructions for improved performance.

## File Structure

### Active Implementation
- **`distance_simd.cpp`** - Primary implementation with multi-platform SIMD support
  - Self-contained, no external BLAS dependency
  - Automatic fallback to pure C++ on platforms without SIMD
  - Optimized for common architectures

### Legacy Implementation (Excluded from Build)
- **`distances.cpp`** - Legacy implementation requiring external BLAS library
  - Excluded from build via CMakeLists.txt
  - Kept for reference only
  - Requires linking against BLAS/LAPACK

## Platform Support

### x86_64 Architectures
- **AVX2 + FMA** (preferred)
  - Automatically detected via CMake
  - Enabled with `-mavx2 -mfma` flags
  - Provides ~4-8x speedup over scalar code
  
- **SSE3** (fallback)
  - Used when AVX2 is not available
  - Provides ~2-4x speedup over scalar code

### ARM Architectures
- **NEON** (ARM64/AArch64)
  - Automatically detected for ARM processors
  - Provides ~4-8x speedup over scalar code
  - Implemented in `simdlib_neon.hpp`

### Generic Fallback
- **Pure C++ with compiler auto-vectorization**
  - Used when no SIMD instructions are available
  - Compiler may still optimize using available instructions
  - Guaranteed to work on all platforms

## Implementation Details

### Core Functions (in `distance_simd.cpp`)

```cpp
// Basic distance functions with SIMD optimization
float fvec_L2sqr(const float* x, const float* y, size_t d);
float fvec_inner_product(const float* x, const float* y, size_t d);
float fvec_norm_L2sqr(const float* x, size_t d);

// Batch operations for better cache utilization
void fvec_L2sqr_batch_4(...);
void fvec_inner_product_batch_4(...);
```

### SIMD Libraries
- `simdlib.hpp` - Common SIMD abstractions
- `simdlib_avx2.hpp` - AVX2 implementations
- `simdlib_neon.hpp` - ARM NEON implementations
- `simdlib_emulated.hpp` - Scalar fallback

## Build Configuration

The build system automatically detects and enables appropriate SIMD support:

```cmake
# In CMakeLists.txt
check_cxx_compiler_flag("-mavx2" COMPILER_SUPPORTS_AVX2)
if(COMPILER_SUPPORTS_AVX2)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -mfma")
    add_definitions(-DUSE_AVX2)
endif()

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
    add_definitions(-DUSE_NEON)
endif()
```

## Performance Characteristics

### Expected Speedups (vs scalar code)
- **AVX2**: 4-8x for distance computations
- **NEON**: 4-8x for distance computations  
- **SSE3**: 2-4x for distance computations
- **Auto-vectorization**: 1.5-3x (compiler dependent)

### Memory Access Patterns
- Optimized for cache-line aligned data
- Prefetching hints for better cache utilization
- Batch operations to amortize overhead

## Testing

To verify SIMD is working correctly:

```bash
# Build with debug output
cmake -DCMAKE_BUILD_TYPE=Release ..
make

# Check build output for SIMD status
# Should see: "✓ AVX2 SIMD support enabled" or similar
```

## Troubleshooting

### SIMD not detected
- Check CPU capabilities: `cat /proc/cpuinfo | grep flags` (Linux)
- Verify compiler supports SIMD flags
- Try forcing: `cmake -DCMAKE_CXX_FLAGS="-mavx2" ..`

### Performance issues
- Ensure Release build: `cmake -DCMAKE_BUILD_TYPE=Release ..`
- Check alignment of input data (16-byte aligned preferred)
- Profile to identify bottlenecks

## Migration Notes

If you need to use the legacy `distances.cpp`:

1. **Not recommended** - requires BLAS dependency
2. Edit `CMakeLists.txt` to include `distances.cpp`
3. Link against BLAS library (`-lblas` or `-lopenblas`)
4. May have linking issues on some platforms

The current `distance_simd.cpp` implementation is preferred for:
- ✅ No external dependencies
- ✅ Better portability
- ✅ Cleaner build process
- ✅ Comparable or better performance
