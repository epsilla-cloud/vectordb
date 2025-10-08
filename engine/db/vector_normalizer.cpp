#include "db/batch_insertion_optimizer.hpp"

#ifdef __AVX2__
#include <immintrin.h>

namespace vectordb {
namespace engine {
namespace db {

void VectorNormalizer::NormalizeBatchAVX2(float* vectors, size_t count, size_t dimension) {
    for (size_t i = 0; i < count; ++i) {
        float* vec = vectors + i * dimension;
        
        // Compute norm using AVX2
        __m256 sum_vec = _mm256_setzero_ps();
        size_t j = 0;
        
        for (; j + 7 < dimension; j += 8) {
            __m256 v = _mm256_loadu_ps(vec + j);
            sum_vec = _mm256_fmadd_ps(v, v, sum_vec);
        }
        
        // Horizontal sum
        __m128 sum_high = _mm256_extractf128_ps(sum_vec, 1);
        __m128 sum_low = _mm256_castps256_ps128(sum_vec);
        __m128 sum128 = _mm_add_ps(sum_high, sum_low);
        sum128 = _mm_hadd_ps(sum128, sum128);
        sum128 = _mm_hadd_ps(sum128, sum128);
        
        float sum = _mm_cvtss_f32(sum128);
        
        // Handle remaining elements
        for (; j < dimension; ++j) {
            sum += vec[j] * vec[j];
        }
        
        // Normalize using AVX2
        float inv_norm = 1.0f / std::sqrt(sum);
        __m256 inv_norm_vec = _mm256_set1_ps(inv_norm);
        
        j = 0;
        for (; j + 7 < dimension; j += 8) {
            __m256 v = _mm256_loadu_ps(vec + j);
            v = _mm256_mul_ps(v, inv_norm_vec);
            _mm256_storeu_ps(vec + j, v);
        }
        
        // Handle remaining elements
        for (; j < dimension; ++j) {
            vec[j] *= inv_norm;
        }
    }
}

} // namespace db
} // namespace engine
} // namespace vectordb

#endif // __AVX2__
