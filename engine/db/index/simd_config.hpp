#pragma once

// SIMD 配置
#if defined(USE_AVX2)
    #define SIMD_WIDTH 8
    #define SIMD_TYPE "AVX2"
#elif defined(USE_NEON)
    #define SIMD_WIDTH 4
    #define SIMD_TYPE "NEON"
#else
    #define SIMD_WIDTH 1
    #define SIMD_TYPE "Scalar"
#endif

namespace vectordb {
namespace simd {
    inline const char* GetSIMDType() {
        return SIMD_TYPE;
    }
    
    inline int GetSIMDWidth() {
        return SIMD_WIDTH;
    }
}
}
