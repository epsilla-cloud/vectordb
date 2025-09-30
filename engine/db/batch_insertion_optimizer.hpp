#pragma once

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <future>
#include "logger/logger.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace engine {
namespace db {

/**
 * @brief Configuration for batch insertion optimization
 */
struct BatchInsertionConfig {
    size_t max_batch_size = 1000;        // Maximum vectors per batch
    size_t worker_threads = 4;           // Number of parallel workers
    bool enable_async_wal = true;        // Asynchronous WAL writes
    size_t wal_sync_interval_ms = 100;   // WAL sync interval
    bool enable_simd = true;             // SIMD optimization for normalization
    bool pre_allocate_buffers = true;    // Pre-allocate memory buffers
    size_t buffer_pool_size = 10;        // Number of pre-allocated buffers
};

/**
 * @brief Optimized batch for vector insertion
 */
struct OptimizedBatch {
    std::vector<int64_t> ids;
    std::vector<float> vectors;  // Flattened vector data
    size_t dimension;
    size_t count;
    bool normalized = false;

    // Pre-allocated buffer for reuse
    void Reset() {
        ids.clear();
        vectors.clear();
        count = 0;
        normalized = false;
    }

    void Reserve(size_t num_vectors, size_t dim) {
        ids.reserve(num_vectors);
        vectors.reserve(num_vectors * dim);
        dimension = dim;
    }
};

/**
 * @brief Fast vector normalization with SIMD
 */
class VectorNormalizer {
private:
    mutable Logger logger_;

public:
    VectorNormalizer() {}

    /**
     * @brief Normalize vectors in batch using SIMD
     */
    void NormalizeBatch(float* vectors, size_t count, size_t dimension) {
        #ifdef __AVX2__
        if (dimension % 8 == 0) {
            NormalizeBatchAVX2(vectors, count, dimension);
            return;
        }
        #endif
        NormalizeBatchScalar(vectors, count, dimension);
    }

private:
    void NormalizeBatchScalar(float* vectors, size_t count, size_t dimension) {
        for (size_t i = 0; i < count; ++i) {
            float* vec = vectors + i * dimension;
            float sum = 0.0f;

            // Compute norm
            for (size_t j = 0; j < dimension; ++j) {
                sum += vec[j] * vec[j];
            }

            // Normalize
            float inv_norm = 1.0f / std::sqrt(sum);
            for (size_t j = 0; j < dimension; ++j) {
                vec[j] *= inv_norm;
            }
        }
    }

    #ifdef __AVX2__
    void NormalizeBatchAVX2(float* vectors, size_t count, size_t dimension);
    #endif
};

/**
 * @brief Parallel JSON parser for batch data
 */
class ParallelJSONParser {
private:
    mutable Logger logger_;
    size_t worker_threads_;

public:
    explicit ParallelJSONParser(size_t threads = 4)
        : worker_threads_(threads) {}

    /**
     * @brief Parse JSON records in parallel
     */
    std::vector<OptimizedBatch> ParseBatches(
        const Json& json_data,
        size_t dimension,
        size_t batch_size = 1000) {

        auto records = json_data.GetArray("records");
        size_t total_records = records.GetSize();
        size_t num_batches = (total_records + batch_size - 1) / batch_size;

        std::vector<OptimizedBatch> batches(num_batches);
        std::vector<std::future<void>> futures;

        // Parse batches in parallel
        for (size_t b = 0; b < num_batches; ++b) {
            futures.push_back(std::async(std::launch::async, [&, b]() {
                size_t start = b * batch_size;
                size_t end = std::min(start + batch_size, total_records);

                auto& batch = batches[b];
                batch.Reserve(end - start, dimension);
                batch.dimension = dimension;

                for (size_t i = start; i < end; ++i) {
                    auto record = records.GetArrayElement(i);

                    // Parse ID
                    int64_t id = record.GetInt("id");
                    batch.ids.push_back(id);

                    // Parse vector
                    auto vec_array = record.GetArray("vec");
                    for (size_t j = 0; j < dimension; ++j) {
                        batch.vectors.push_back(
                            static_cast<float>(vec_array.GetArrayElement(j).GetDouble())
                        );
                    }
                }

                batch.count = end - start;
            }));
        }

        // Wait for all parsing to complete
        for (auto& future : futures) {
            future.wait();
        }

        return batches;
    }
};

/**
 * @brief Buffer pool for batch reuse
 */
class BatchBufferPool {
private:
    std::queue<std::unique_ptr<OptimizedBatch>> pool_;
    std::mutex mutex_;
    size_t pool_size_;
    size_t dimension_;
    size_t batch_size_;

public:
    BatchBufferPool(size_t pool_size, size_t dimension, size_t batch_size)
        : pool_size_(pool_size), dimension_(dimension), batch_size_(batch_size) {
        // Pre-allocate buffers
        for (size_t i = 0; i < pool_size_; ++i) {
            auto batch = std::make_unique<OptimizedBatch>();
            batch->Reserve(batch_size_, dimension_);
            pool_.push(std::move(batch));
        }
    }

    std::unique_ptr<OptimizedBatch> Acquire() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!pool_.empty()) {
            auto batch = std::move(pool_.front());
            pool_.pop();
            batch->Reset();
            return batch;
        }

        // Create new if pool is empty
        auto batch = std::make_unique<OptimizedBatch>();
        batch->Reserve(batch_size_, dimension_);
        return batch;
    }

    void Release(std::unique_ptr<OptimizedBatch> batch) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (pool_.size() < pool_size_) {
            batch->Reset();
            pool_.push(std::move(batch));
        }
    }
};

/**
 * @brief Main batch insertion optimizer
 */
class BatchInsertionOptimizer {
private:
    BatchInsertionConfig config_;
    std::unique_ptr<VectorNormalizer> normalizer_;
    std::unique_ptr<ParallelJSONParser> parser_;
    std::unique_ptr<BatchBufferPool> buffer_pool_;
    mutable Logger logger_;

    // Metrics
    std::atomic<size_t> total_vectors_processed_{0};
    std::atomic<size_t> total_batches_processed_{0};
    std::atomic<double> avg_vectors_per_second_{0.0};

public:
    explicit BatchInsertionOptimizer(const BatchInsertionConfig& config = {})
        : config_(config) {

        normalizer_ = std::make_unique<VectorNormalizer>();
        parser_ = std::make_unique<ParallelJSONParser>(config.worker_threads);

        logger_.Info("Batch insertion optimizer initialized with " +
                    std::to_string(config.worker_threads) + " worker threads");
    }

    /**
     * @brief Initialize buffer pool for specific dimension
     */
    void InitializeBufferPool(size_t dimension) {
        if (config_.pre_allocate_buffers) {
            buffer_pool_ = std::make_unique<BatchBufferPool>(
                config_.buffer_pool_size, dimension, config_.max_batch_size
            );
            logger_.Info("Buffer pool initialized with " +
                        std::to_string(config_.buffer_pool_size) + " buffers");
        }
    }

    /**
     * @brief Process insertion request with optimization
     */
    std::vector<OptimizedBatch> ProcessInsertionRequest(
        const Json& json_data,
        size_t dimension) {

        auto start_time = std::chrono::high_resolution_clock::now();

        // Parse batches in parallel
        auto batches = parser_->ParseBatches(json_data, dimension, config_.max_batch_size);

        // Normalize vectors in parallel
        std::vector<std::future<void>> norm_futures;
        for (auto& batch : batches) {
            norm_futures.push_back(std::async(std::launch::async, [&]() {
                normalizer_->NormalizeBatch(
                    batch.vectors.data(), batch.count, batch.dimension
                );
                batch.normalized = true;
            }));
        }

        // Wait for normalization to complete
        for (auto& future : norm_futures) {
            future.wait();
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();

        // Update metrics
        size_t total_vectors = 0;
        for (const auto& batch : batches) {
            total_vectors += batch.count;
        }

        total_vectors_processed_ += total_vectors;
        total_batches_processed_ += batches.size();

        double vectors_per_second = (total_vectors * 1000.0) / duration_ms;
        avg_vectors_per_second_ = vectors_per_second;

        logger_.Debug("Processed " + std::to_string(total_vectors) +
                     " vectors in " + std::to_string(duration_ms) +
                     "ms (" + std::to_string(vectors_per_second) + " vec/s)");

        return batches;
    }

    /**
     * @brief Get optimization metrics
     */
    struct Metrics {
        size_t total_vectors_processed;
        size_t total_batches_processed;
        double avg_vectors_per_second;
    };

    Metrics GetMetrics() const {
        return {
            total_vectors_processed_.load(),
            total_batches_processed_.load(),
            avg_vectors_per_second_.load()
        };
    }

    /**
     * @brief Set configuration
     */
    void SetConfig(const BatchInsertionConfig& config) {
        config_ = config;
        parser_ = std::make_unique<ParallelJSONParser>(config.worker_threads);
    }
};

} // namespace db
} // namespace engine
} // namespace vectordb