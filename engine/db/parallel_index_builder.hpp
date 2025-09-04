#pragma once

#include <atomic>
#include <thread>
#include <vector>
#include <queue>
#include <future>
#include <condition_variable>
#include <execution>
#ifdef __AVX2__
#include <immintrin.h>  // For AVX2 intrinsics
#endif
#include "db/index/nsg/nsg.hpp"
#include "db/ann_graph_segment.hpp"
#include "logger/logger.hpp"
#include "utils/status.hpp"
#include "utils/thread_pool.hpp"

namespace vectordb {
namespace engine {

// Configuration for parallel index building
struct ParallelIndexConfig {
    size_t num_threads = std::thread::hardware_concurrency();  // Number of threads to use
    size_t batch_size = 10000;                                 // Batch size for parallel processing
    size_t max_neighbors = 32;                                 // Maximum neighbors in graph
    size_t ef_construction = 200;                              // Construction parameter
    bool use_gpu = false;                                      // Use GPU acceleration if available
    bool incremental_build = true;                             // Support incremental updates
    size_t merge_batch_size = 50000;                          // Batch size for merging sub-indices
};

// Statistics for monitoring
struct IndexBuildStats {
    size_t vectors_processed = 0;
    size_t edges_created = 0;
    double build_time_ms = 0;
    double search_time_ms = 0;
    size_t memory_used_bytes = 0;
    size_t num_partitions = 0;
};

// Thread pool for parallel execution
class ThreadPool {
public:
    ThreadPool(size_t num_threads) : stop_(false) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex_);
                        condition_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
                        
                        if (stop_ && tasks_.empty()) return;
                        
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                }
            });
        }
    }
    
    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            stop_ = true;
        }
        condition_.notify_all();
        
        for (auto& worker : workers_) {
            worker.join();
        }
    }
    
    template<class F>
    auto enqueue(F&& f) -> std::future<typename std::result_of<F()>::type> {
        using return_type = typename std::result_of<F()>::type;
        
        auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::forward<F>(f)
        );
        
        std::future<return_type> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            tasks_.emplace([task]() { (*task)(); });
        }
        condition_.notify_one();
        return res;
    }
    
private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    bool stop_;
};

// Parallel index builder
class ParallelIndexBuilder {
public:
    ParallelIndexBuilder(const ParallelIndexConfig& config = ParallelIndexConfig())
        : config_(config),
          thread_pool_(config.num_threads),
          vectors_processed_(0) {
        
        logger_.Info("ParallelIndexBuilder initialized with " + 
                    std::to_string(config.num_threads) + " threads");
    }
    
    // Build index from vectors
    Status BuildIndex(
        const float* vectors,
        size_t num_vectors,
        size_t dimension,
        meta::MetricType metric_type,
        std::shared_ptr<ANNGraphSegment>& index_out);
    
    // Build multiple indices in parallel (for multi-field tables)
    Status BuildMultipleIndices(
        const std::vector<std::pair<float*, meta::MetricType>>& vector_fields,
        size_t num_vectors,
        const std::vector<size_t>& dimensions,
        std::vector<std::shared_ptr<ANNGraphSegment>>& indices_out);
    
    // Incremental index update
    Status UpdateIndex(
        std::shared_ptr<ANNGraphSegment>& index,
        const float* new_vectors,
        size_t num_new_vectors,
        size_t dimension);
    
    // Merge multiple sub-indices
    Status MergeIndices(
        const std::vector<std::shared_ptr<ANNGraphSegment>>& sub_indices,
        std::shared_ptr<ANNGraphSegment>& merged_index);
    
    // Get build statistics
    IndexBuildStats GetStats() const { return stats_; }
    
private:
    // Partition data for parallel processing
    struct DataPartition {
        const float* data;
        size_t start_idx;
        size_t end_idx;
        size_t dimension;
        meta::MetricType metric_type;
    };
    
    // Build sub-index for a partition
    std::shared_ptr<index::NsgIndex> BuildPartitionIndex(
        const DataPartition& partition);
    
    // Parallel k-NN graph construction
    void ParallelKNNGraphConstruction(
        const float* vectors,
        size_t num_vectors,
        size_t dimension,
        size_t k,
        std::vector<std::vector<size_t>>& graph);
    
    // Parallel NSG construction from k-NN graph
    void ParallelNSGConstruction(
        const std::vector<std::vector<size_t>>& knn_graph,
        const float* vectors,
        size_t dimension,
        meta::MetricType metric_type,
        std::shared_ptr<index::NsgIndex>& nsg_index);
    
    // Distance computation using SIMD
    float ComputeDistance(
        const float* vec1,
        const float* vec2,
        size_t dimension,
        meta::MetricType metric_type);
    
    // Batch distance computation
    void BatchComputeDistances(
        const float* query,
        const float* vectors,
        size_t num_vectors,
        size_t dimension,
        meta::MetricType metric_type,
        std::vector<float>& distances);
    
    // Parallel sorting for k-NN
    void ParallelPartialSort(
        std::vector<std::pair<float, size_t>>& distances,
        size_t k);
    
    // Build index using divide-and-conquer
    std::shared_ptr<ANNGraphSegment> DivideAndConquerBuild(
        const float* vectors,
        size_t num_vectors,
        size_t dimension,
        meta::MetricType metric_type);
    
    // Merge two sub-graphs
    void MergeGraphs(
        const std::shared_ptr<index::NsgIndex>& graph1,
        const std::shared_ptr<index::NsgIndex>& graph2,
        std::shared_ptr<index::NsgIndex>& merged);
    
    // GPU acceleration (if available)
    Status BuildIndexGPU(
        const float* vectors,
        size_t num_vectors,
        size_t dimension,
        meta::MetricType metric_type,
        std::shared_ptr<ANNGraphSegment>& index_out);
    
    ParallelIndexConfig config_;
    ThreadPool thread_pool_;
    std::atomic<size_t> vectors_processed_;
    IndexBuildStats stats_;
    Logger logger_;
};

// Implementation of key methods

inline Status ParallelIndexBuilder::BuildIndex(
    const float* vectors,
    size_t num_vectors,
    size_t dimension,
    meta::MetricType metric_type,
    std::shared_ptr<ANNGraphSegment>& index_out) {
    
    auto start_time = std::chrono::steady_clock::now();
    
    logger_.Info("Starting parallel index build for " + std::to_string(num_vectors) + 
                " vectors of dimension " + std::to_string(dimension));
    
    // Check if GPU acceleration is available and requested
    if (config_.use_gpu) {
        auto gpu_status = BuildIndexGPU(vectors, num_vectors, dimension, metric_type, index_out);
        if (gpu_status.ok()) {
            return gpu_status;
        }
        logger_.Warn("GPU build failed, falling back to CPU");
    }
    
    // Use divide-and-conquer for large datasets
    if (num_vectors > config_.merge_batch_size) {
        index_out = DivideAndConquerBuild(vectors, num_vectors, dimension, metric_type);
    } else {
        // Build directly for smaller datasets
        
        // Step 1: Parallel k-NN graph construction
        std::vector<std::vector<size_t>> knn_graph(num_vectors);
        ParallelKNNGraphConstruction(vectors, num_vectors, dimension, 
                                   config_.max_neighbors, knn_graph);
        
        // Step 2: Parallel NSG construction
        auto nsg_index = std::make_shared<index::NsgIndex>(
            dimension, num_vectors, metric_type);
        
        ParallelNSGConstruction(knn_graph, vectors, dimension, metric_type, nsg_index);
        
        // Step 3: Create ANN graph segment
        index_out = std::make_shared<ANNGraphSegment>();
        // Note: This would need proper initialization with the NSG index
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    
    stats_.build_time_ms = duration.count();
    stats_.vectors_processed = num_vectors;
    
    logger_.Info("Index build complete in " + std::to_string(duration.count()) + "ms");
    
    return Status::OK();
}

inline void ParallelIndexBuilder::ParallelKNNGraphConstruction(
    const float* vectors,
    size_t num_vectors,
    size_t dimension,
    size_t k,
    std::vector<std::vector<size_t>>& graph) {
    
    // Divide work among threads
    size_t chunk_size = (num_vectors + config_.num_threads - 1) / config_.num_threads;
    std::vector<std::future<void>> futures;
    
    for (size_t thread_id = 0; thread_id < config_.num_threads; ++thread_id) {
        size_t start = thread_id * chunk_size;
        size_t end = std::min(start + chunk_size, num_vectors);
        
        if (start >= end) break;
        
        futures.push_back(thread_pool_.enqueue([this, &vectors, &graph, dimension, k, start, end, num_vectors]() {
            // Process vectors in this chunk
            for (size_t i = start; i < end; ++i) {
                const float* query = vectors + i * dimension;
                
                // Compute distances to all other vectors
                std::vector<std::pair<float, size_t>> distances;
                distances.reserve(num_vectors - 1);
                
                for (size_t j = 0; j < num_vectors; ++j) {
                    if (i == j) continue;
                    
                    const float* target = vectors + j * dimension;
                    float dist = ComputeDistance(query, target, dimension, meta::MetricType::EUCLIDEAN);
                    distances.emplace_back(dist, j);
                }
                
                // Find k nearest neighbors
                std::partial_sort(distances.begin(), 
                                distances.begin() + k, 
                                distances.end());
                
                // Store in graph
                graph[i].reserve(k);
                for (size_t j = 0; j < k; ++j) {
                    graph[i].push_back(distances[j].second);
                }
                
                vectors_processed_.fetch_add(1);
                
                // Log progress
                if (vectors_processed_ % 1000 == 0) {
                    logger_.Debug("Processed " + std::to_string(vectors_processed_) + 
                                " / " + std::to_string(num_vectors) + " vectors");
                }
            }
        }));
    }
    
    // Wait for all threads to complete
    for (auto& future : futures) {
        future.wait();
    }
}

inline std::shared_ptr<ANNGraphSegment> ParallelIndexBuilder::DivideAndConquerBuild(
    const float* vectors,
    size_t num_vectors,
    size_t dimension,
    meta::MetricType metric_type) {
    
    logger_.Info("Using divide-and-conquer strategy for " + std::to_string(num_vectors) + " vectors");
    
    // Divide into partitions
    size_t num_partitions = (num_vectors + config_.merge_batch_size - 1) / config_.merge_batch_size;
    std::vector<DataPartition> partitions;
    
    for (size_t i = 0; i < num_partitions; ++i) {
        DataPartition partition;
        partition.data = vectors;
        partition.start_idx = i * config_.merge_batch_size;
        partition.end_idx = std::min((i + 1) * config_.merge_batch_size, num_vectors);
        partition.dimension = dimension;
        partition.metric_type = metric_type;
        partitions.push_back(partition);
    }
    
    // Build sub-indices in parallel
    std::vector<std::future<std::shared_ptr<index::NsgIndex>>> futures;
    
    for (const auto& partition : partitions) {
        futures.push_back(thread_pool_.enqueue([this, partition]() {
            return BuildPartitionIndex(partition);
        }));
    }
    
    // Collect sub-indices
    std::vector<std::shared_ptr<ANNGraphSegment>> sub_indices;
    for (auto& future : futures) {
        auto sub_index = future.get();
        // Convert NsgIndex to ANNGraphSegment
        auto segment = std::make_shared<ANNGraphSegment>();
        // Initialize segment with sub_index
        sub_indices.push_back(segment);
    }
    
    // Merge sub-indices
    std::shared_ptr<ANNGraphSegment> merged_index;
    auto merge_status = MergeIndices(sub_indices, merged_index);
    
    if (!merge_status.ok()) {
        logger_.Error("Failed to merge sub-indices: " + merge_status.message());
        return nullptr;
    }
    
    stats_.num_partitions = num_partitions;
    
    return merged_index;
}

inline std::shared_ptr<index::NsgIndex> ParallelIndexBuilder::BuildPartitionIndex(
    const DataPartition& partition) {
    
    size_t partition_size = partition.end_idx - partition.start_idx;
    const float* partition_data = partition.data + partition.start_idx * partition.dimension;
    
    // Build k-NN graph for this partition
    std::vector<std::vector<size_t>> knn_graph(partition_size);
    
    // Simplified k-NN construction for partition
    for (size_t i = 0; i < partition_size; ++i) {
        const float* query = partition_data + i * partition.dimension;
        std::vector<std::pair<float, size_t>> distances;
        
        for (size_t j = 0; j < partition_size; ++j) {
            if (i == j) continue;
            const float* target = partition_data + j * partition.dimension;
            float dist = ComputeDistance(query, target, partition.dimension, partition.metric_type);
            distances.emplace_back(dist, j);
        }
        
        // Get k nearest neighbors
        size_t k = std::min(config_.max_neighbors, partition_size - 1);
        std::partial_sort(distances.begin(), 
                        distances.begin() + k,
                        distances.end());
        
        knn_graph[i].reserve(k);
        for (size_t j = 0; j < k; ++j) {
            knn_graph[i].push_back(distances[j].second);
        }
    }
    
    // Build NSG from k-NN graph
    auto nsg_index = std::make_shared<index::NsgIndex>(
        partition.dimension, partition_size, partition.metric_type);
    
    // Initialize NSG with k-NN graph
    // Note: Actual NSG construction would go here
    
    return nsg_index;
}

inline float ParallelIndexBuilder::ComputeDistance(
    const float* vec1,
    const float* vec2,
    size_t dimension,
    meta::MetricType metric_type) {
    
    float distance = 0.0f;
    
    switch (metric_type) {
        case meta::MetricType::EUCLIDEAN: {
            // Use SIMD for faster computation
            #ifdef __AVX2__
            size_t simd_dim = dimension - (dimension % 8);
            __m256 sum = _mm256_setzero_ps();
            
            for (size_t i = 0; i < simd_dim; i += 8) {
                __m256 v1 = _mm256_loadu_ps(vec1 + i);
                __m256 v2 = _mm256_loadu_ps(vec2 + i);
                __m256 diff = _mm256_sub_ps(v1, v2);
                sum = _mm256_fmadd_ps(diff, diff, sum);
            }
            
            float result[8];
            _mm256_storeu_ps(result, sum);
            for (int i = 0; i < 8; i++) {
                distance += result[i];
            }
            
            // Handle remaining dimensions
            for (size_t i = simd_dim; i < dimension; i++) {
                float diff = vec1[i] - vec2[i];
                distance += diff * diff;
            }
            #else
            for (size_t i = 0; i < dimension; i++) {
                float diff = vec1[i] - vec2[i];
                distance += diff * diff;
            }
            #endif
            
            return std::sqrt(distance);
        }
        
        case meta::MetricType::COSINE: {
            float dot = 0.0f, norm1 = 0.0f, norm2 = 0.0f;
            
            #ifdef __AVX2__
            // SIMD implementation for cosine similarity
            size_t simd_dim = dimension - (dimension % 8);
            __m256 dot_sum = _mm256_setzero_ps();
            __m256 norm1_sum = _mm256_setzero_ps();
            __m256 norm2_sum = _mm256_setzero_ps();
            
            for (size_t i = 0; i < simd_dim; i += 8) {
                __m256 v1 = _mm256_loadu_ps(vec1 + i);
                __m256 v2 = _mm256_loadu_ps(vec2 + i);
                dot_sum = _mm256_fmadd_ps(v1, v2, dot_sum);
                norm1_sum = _mm256_fmadd_ps(v1, v1, norm1_sum);
                norm2_sum = _mm256_fmadd_ps(v2, v2, norm2_sum);
            }
            
            float dot_result[8], norm1_result[8], norm2_result[8];
            _mm256_storeu_ps(dot_result, dot_sum);
            _mm256_storeu_ps(norm1_result, norm1_sum);
            _mm256_storeu_ps(norm2_result, norm2_sum);
            
            for (int i = 0; i < 8; i++) {
                dot += dot_result[i];
                norm1 += norm1_result[i];
                norm2 += norm2_result[i];
            }
            
            // Handle remaining dimensions
            for (size_t i = simd_dim; i < dimension; i++) {
                dot += vec1[i] * vec2[i];
                norm1 += vec1[i] * vec1[i];
                norm2 += vec2[i] * vec2[i];
            }
            #else
            for (size_t i = 0; i < dimension; i++) {
                dot += vec1[i] * vec2[i];
                norm1 += vec1[i] * vec1[i];
                norm2 += vec2[i] * vec2[i];
            }
            #endif
            
            return 1.0f - (dot / (std::sqrt(norm1) * std::sqrt(norm2)));
        }
        
        case meta::MetricType::DOT_PRODUCT: {
            float dot = 0.0f;
            
            #ifdef __AVX2__
            size_t simd_dim = dimension - (dimension % 8);
            __m256 sum = _mm256_setzero_ps();
            
            for (size_t i = 0; i < simd_dim; i += 8) {
                __m256 v1 = _mm256_loadu_ps(vec1 + i);
                __m256 v2 = _mm256_loadu_ps(vec2 + i);
                sum = _mm256_fmadd_ps(v1, v2, sum);
            }
            
            float result[8];
            _mm256_storeu_ps(result, sum);
            for (int i = 0; i < 8; i++) {
                dot += result[i];
            }
            
            // Handle remaining dimensions
            for (size_t i = simd_dim; i < dimension; i++) {
                dot += vec1[i] * vec2[i];
            }
            #else
            for (size_t i = 0; i < dimension; i++) {
                dot += vec1[i] * vec2[i];
            }
            #endif
            
            return -dot;  // Negative for similarity
        }
        
        default:
            return 0.0f;
    }
}

} // namespace engine
} // namespace vectordb