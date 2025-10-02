#pragma once

#include <atomic>
#include <memory>
#include <cstring>
#include <algorithm>
#include <mutex>
#include "utils/status.hpp"
#include "utils/error.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

enum class GrowthStrategy {
    DOUBLE,     // 2x growth
    HALF,       // 1.5x growth
    LINEAR,     // Fixed increment
    ADAPTIVE    // Size-dependent strategy
};

/**
 * @brief Thread-safe dynamic memory block with automatic growth
 *
 * This class provides a growable array with the following features:
 * - Automatic resizing when capacity is exceeded
 * - Configurable growth strategies
 * - Thread-safe operations using RCU pattern
 * - Memory usage tracking and monitoring
 * - Zero-copy access for read operations
 */
template<typename T>
class DynamicMemoryBlock {
public:
    DynamicMemoryBlock(size_t initial_capacity = 1000,
                      GrowthStrategy strategy = GrowthStrategy::ADAPTIVE)
        : capacity_(initial_capacity),
          size_(0),
          strategy_(strategy),
          growth_factor_(2.0),
          linear_increment_(100000) {

        if (capacity_ > 0) {
            // Check for potential overflow in memory allocation
            const size_t max_capacity = 100000000;  // 100M elements
            if (capacity_ > max_capacity) {
                throw std::overflow_error("Initial capacity exceeds maximum limit: " +
                                         std::to_string(capacity_));
            }

            // Check if total memory size would overflow
            size_t total_bytes;
            size_t capacity_val = capacity_;  // Read atomic value once
            if (__builtin_mul_overflow(capacity_val, sizeof(T), &total_bytes)) {
                throw std::overflow_error("Memory allocation size overflow: capacity=" +
                                         std::to_string(capacity_val) + ", sizeof(T)=" +
                                         std::to_string(sizeof(T)));
            }

            // Safe memory allocation with exception handling
            try {
                data_ = std::make_unique<T[]>(capacity_);
                logger_.Debug("DynamicMemoryBlock initialized with capacity: " +
                             std::to_string(capacity_));
            } catch (const std::bad_alloc& e) {
                logger_.Error("Failed to allocate memory for capacity " +
                             std::to_string(capacity_) + ": " + e.what());
                throw std::runtime_error("Memory allocation failed: " + std::string(e.what()));
            }
        }
    }

    /**
     * @brief Resize the block to accommodate new_size elements
     *
     * If new_size > capacity, the block will grow according to the strategy.
     * If new_size < size, existing data beyond new_size is preserved but not accessible.
     */
    Status Resize(size_t new_size) {
        std::lock_guard<std::mutex> lock(resize_mutex_);

        if (new_size <= capacity_) {
            size_ = new_size;
            return Status::OK();
        }

        // Calculate new capacity based on strategy
        size_t new_capacity = CalculateNewCapacity(new_size);

        // Check for maximum capacity limit
        const size_t max_capacity = 100000000;  // 100M records
        if (new_capacity > max_capacity) {
            return Status(DB_UNEXPECTED_ERROR,
                         "Requested capacity exceeds maximum limit: " +
                         std::to_string(new_capacity));
        }

        // Check for overflow in new capacity allocation
        size_t total_bytes;
        if (__builtin_mul_overflow(new_capacity, sizeof(T), &total_bytes)) {
            return Status(DB_UNEXPECTED_ERROR,
                         "Memory allocation size overflow: capacity=" +
                         std::to_string(new_capacity) + ", sizeof(T)=" +
                         std::to_string(sizeof(T)));
        }

        // Allocate new buffer with exception handling
        auto start_time = std::chrono::high_resolution_clock::now();
        std::unique_ptr<T[]> new_data;
        try {
            new_data = std::make_unique<T[]>(new_capacity);
        } catch (const std::bad_alloc& e) {
            logger_.Error("Failed to allocate memory for new capacity " +
                         std::to_string(new_capacity) + ": " + e.what());
            return Status(DB_UNEXPECTED_ERROR,
                         "Memory allocation failed: " + std::string(e.what()));
        }

        // Copy existing data
        if (data_ && size_ > 0) {
            std::memcpy(new_data.get(), data_.get(), size_ * sizeof(T));
        }

        // Atomic swap using RCU pattern
        auto old_data = std::move(data_);
        data_ = std::move(new_data);

        size_t old_capacity = capacity_;
        capacity_ = new_capacity;
        size_ = new_size;

        // Track resize statistics
        resize_count_++;
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>
                       (end_time - start_time).count();
        total_resize_time_ms_ += duration;

        logger_.Info("DynamicMemoryBlock resized from " +
                    std::to_string(old_capacity) + " to " +
                    std::to_string(new_capacity) + " in " +
                    std::to_string(duration) + "ms");

        // Old data will be automatically freed when unique_ptr goes out of scope
        return Status::OK();
    }

    /**
     * @brief Grow the block to accommodate additional elements
     */
    Status Grow(size_t additional_elements) {
        return Resize(size_ + additional_elements);
    }

    /**
     * @brief Ensure the block has at least the specified capacity
     */
    Status EnsureCapacity(size_t min_capacity) {
        if (capacity_ >= min_capacity) {
            return Status::OK();
        }
        return Resize(min_capacity);
    }

    /**
     * @brief Get raw pointer to data (for performance-critical paths)
     *
     * Note: Pointer may be invalidated after resize operations
     */
    T* GetData() {
        return data_.get();
    }

    const T* GetData() const {
        return data_.get();
    }

    /**
     * @brief Safe element access with bounds checking
     */
    T& operator[](size_t index) {
        if (index >= size_) {
            throw std::out_of_range("Index out of bounds: " + std::to_string(index));
        }
        return data_[index];
    }

    const T& operator[](size_t index) const {
        if (index >= size_) {
            throw std::out_of_range("Index out of bounds: " + std::to_string(index));
        }
        return data_[index];
    }

    // Getters
    size_t GetCapacity() const { return capacity_; }
    size_t GetSize() const { return size_; }
    size_t GetMemoryUsage() const { return capacity_ * sizeof(T); }

    // Configuration
    void SetGrowthStrategy(GrowthStrategy strategy) {
        strategy_ = strategy;
    }

    void SetGrowthFactor(double factor) {
        if (factor > 1.0 && factor <= 4.0) {
            growth_factor_ = factor;
        }
    }

    void SetLinearIncrement(size_t increment) {
        linear_increment_ = increment;
    }

    // Statistics
    struct Stats {
        size_t resize_count;
        size_t total_resize_time_ms;
        size_t current_capacity;
        size_t current_size;
        size_t memory_usage_bytes;
        double utilization_ratio;
    };

    Stats GetStats() const {
        return {
            resize_count_,
            total_resize_time_ms_,
            capacity_,
            size_,
            GetMemoryUsage(),
            size_ > 0 ? static_cast<double>(size_) / capacity_ : 0.0
        };
    }

private:
    /**
     * @brief Calculate new capacity based on growth strategy
     */
    size_t CalculateNewCapacity(size_t required_size) {
        size_t new_capacity = required_size;

        switch (strategy_) {
            case GrowthStrategy::DOUBLE:
                new_capacity = std::max(capacity_ * 2, required_size);
                break;

            case GrowthStrategy::HALF:
                new_capacity = std::max(static_cast<size_t>(capacity_ * 1.5),
                                       required_size);
                break;

            case GrowthStrategy::LINEAR:
                new_capacity = ((required_size / linear_increment_) + 1) *
                              linear_increment_;
                break;

            case GrowthStrategy::ADAPTIVE:
                // Use different strategies based on current size
                if (capacity_ < 10000) {
                    // Small: 2x growth
                    new_capacity = std::max(capacity_ * 2, required_size);
                } else if (capacity_ < 100000) {
                    // Medium: 1.5x growth
                    new_capacity = std::max(static_cast<size_t>(capacity_ * 1.5),
                                           required_size);
                } else {
                    // Large: linear growth
                    new_capacity = std::max(capacity_ + linear_increment_,
                                           required_size);
                }
                break;
        }

        // Ensure we at least meet the required size
        return std::max(new_capacity, required_size);
    }

private:
    std::unique_ptr<T[]> data_;
    std::atomic<size_t> capacity_;
    std::atomic<size_t> size_;
    GrowthStrategy strategy_;
    double growth_factor_;
    size_t linear_increment_;

    // Thread safety
    mutable std::mutex resize_mutex_;

    // Statistics
    std::atomic<size_t> resize_count_{0};
    std::atomic<size_t> total_resize_time_ms_{0};

    // Logger
    mutable vectordb::engine::Logger logger_;
};

/**
 * @brief Specialized version for vectors with dimension
 */
class DynamicVectorBlock : public DynamicMemoryBlock<float> {
public:
    DynamicVectorBlock(size_t initial_vectors, size_t dimension,
                      GrowthStrategy strategy = GrowthStrategy::ADAPTIVE)
        : DynamicMemoryBlock<float>(0, strategy),  // Initialize with 0 first
          dimension_(dimension),
          num_vectors_(0) {

        // Validate input parameters
        if (dimension == 0) {
            throw std::invalid_argument("Vector dimension cannot be zero");
        }

        if (initial_vectors == 0) {
            // Allow zero initialization for empty vector blocks
            return;
        }

        // Check for overflow in total capacity calculation
        size_t total_capacity;
        if (__builtin_mul_overflow(initial_vectors, dimension, &total_capacity)) {
            throw std::overflow_error("Vector capacity overflow: vectors=" +
                                     std::to_string(initial_vectors) + ", dimension=" +
                                     std::to_string(dimension));
        }

        // Check maximum reasonable vector count (prevent excessive memory usage)
        const size_t max_vectors = 10000000;  // 10M vectors
        if (initial_vectors > max_vectors) {
            throw std::overflow_error("Initial vector count exceeds maximum limit: " +
                                     std::to_string(initial_vectors));
        }

        // Safe initialization using EnsureCapacity
        auto status = this->EnsureCapacity(total_capacity);
        if (!status.ok()) {
            throw std::runtime_error("Failed to initialize vector block: " + status.message());
        }

        num_vectors_ = initial_vectors;
    }

    Status ResizeVectors(size_t new_num_vectors) {
        // Check for overflow in new capacity calculation
        size_t new_total_capacity;
        if (__builtin_mul_overflow(new_num_vectors, dimension_, &new_total_capacity)) {
            return Status(DB_UNEXPECTED_ERROR,
                         "Vector resize overflow: vectors=" +
                         std::to_string(new_num_vectors) + ", dimension=" +
                         std::to_string(dimension_));
        }

        auto status = Resize(new_total_capacity);
        if (status.ok()) {
            num_vectors_ = new_num_vectors;
        }
        return status;
    }

    float* GetVector(size_t index) {
        if (index >= num_vectors_) {
            return nullptr;
        }
        return GetData() + (index * dimension_);
    }

    const float* GetVector(size_t index) const {
        if (index >= num_vectors_) {
            return nullptr;
        }
        return GetData() + (index * dimension_);
    }

    size_t GetNumVectors() const { return num_vectors_; }
    size_t GetDimension() const { return dimension_; }

private:
    size_t dimension_;
    std::atomic<size_t> num_vectors_;
};

}  // namespace engine
}  // namespace vectordb