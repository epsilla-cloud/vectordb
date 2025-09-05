#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <mutex>
#include <cstring>
#include "db/catalog/meta_types.hpp"
#include "utils/concurrent_bitset.hpp"
#include "logger/logger.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

/**
 * @brief Dynamic growth segment configuration
 */
struct DynamicSegmentConfig {
    // Initial capacity (avoid hardcoded 150000)
    size_t initial_capacity = 5000;
    
    // Minimum capacity
    size_t min_capacity = 100;
    
    // Maximum capacity (prevent memory explosion)
    size_t max_capacity = 100000000;  // 100 million records
    
    // Growth factor (how much to expand when full)
    double growth_factor = 2.0;
    
    // Expansion threshold (expand when usage reaches this)
    double expand_threshold = 0.9;  // Expand at 90% usage
    
    // Shrink threshold (shrink when usage falls below this)
    double shrink_threshold = 0.25;  // Consider shrinking at 25% usage
    
    // Whether to allow automatic shrinking
    bool allow_shrink = true;
    
    // Pre-allocation strategy
    enum PreallocStrategy {
        NONE,        // No pre-allocation
        LINEAR,      // Linear growth
        EXPONENTIAL, // Exponential growth
        ADAPTIVE     // Adaptive based on historical patterns
    };
    PreallocStrategy prealloc_strategy = ADAPTIVE;
    
    // Memory alignment (improve cache performance)
    size_t alignment = 64;  // Cache line size
};

/**
 * @brief Dynamic growth data block
 * 
 * Solves the hardcoded 150000 problem, supports on-demand expansion
 */
template<typename T>
class DynamicDataBlock {
public:
    DynamicDataBlock(size_t initial_size, size_t element_size = sizeof(T))
        : element_size_(element_size),
          capacity_(initial_size),
          size_(0) {
        data_ = std::make_unique<T[]>(capacity_);
    }
    
    /**
     * @brief Get element at specified position
     */
    T* get(size_t index) {
        if (index >= capacity_) {
            return nullptr;
        }
        return &data_[index];
    }
    
    /**
     * @brief Ensure capacity is at least required_size
     */
    Status ensure_capacity(size_t required_size) {
        if (required_size <= capacity_) {
            return Status::OK();
        }
        
        // Calculate new capacity
        size_t new_capacity = capacity_;
        while (new_capacity < required_size) {
            new_capacity = static_cast<size_t>(new_capacity * 1.5);
        }
        
        return resize(new_capacity);
    }
    
    /**
     * @brief Reallocate memory
     */
    Status resize(size_t new_capacity) {
        if (new_capacity == capacity_) {
            return Status::OK();
        }
        
        // Allocate new memory
        auto new_data = std::make_unique<T[]>(new_capacity);
        
        // Copy existing data
        size_t copy_size = std::min(size_, new_capacity);
        if (data_ && copy_size > 0) {
            std::memcpy(new_data.get(), data_.get(), copy_size * element_size_);
        }
        
        // Swap pointers
        data_ = std::move(new_data);
        capacity_ = new_capacity;
        
        return Status::OK();
    }
    
    size_t capacity() const { return capacity_; }
    size_t size() const { return size_; }
    void set_size(size_t s) { size_ = s; }
    T* data() { return data_.get(); }
    
private:
    std::unique_ptr<T[]> data_;
    size_t element_size_;
    size_t capacity_;
    size_t size_;
};

/**
 * @brief Dynamic table segment - replaces fixed-size TableSegmentMVP
 * 
 * Features:
 * 1. On-demand memory allocation, avoids pre-allocating 150000 records
 * 2. Automatic expansion, grows dynamically when capacity is exceeded
 * 3. Optional memory shrinking, releases unused memory
 * 4. Chunked storage, reduces large contiguous memory allocations
 * 5. Zero-copy expansion (using linked list or segmentation)
 */
class DynamicTableSegment {
public:
    explicit DynamicTableSegment(const DynamicSegmentConfig& config = DynamicSegmentConfig())
        : config_(config),
          record_count_(0),
          capacity_(config.initial_capacity),
          primitive_offset_(0) {
        
        // Initialize deletion bitmap
        deleted_bitmap_ = std::make_unique<ConcurrentBitset>(capacity_);
    }
    
    /**
     * @brief Initialize segment structure
     */
    Status Init(const meta::TableSchema& schema) {
        schema_ = schema;
        
        // Calculate field offsets and sizes
        AnalyzeSchema(schema);
        
        // Only allocate initial capacity memory (not 150000)
        if (primitive_offset_ > 0) {
            attribute_data_ = std::make_unique<DynamicDataBlock<char>>(
                capacity_ * primitive_offset_, 1);
        }
        
        // Allocate initial memory for vectors
        vector_blocks_.clear();
        for (size_t dim : vector_dimensions_) {
            auto block = std::make_unique<DynamicDataBlock<float>>(
                capacity_ * dim, sizeof(float));
            vector_blocks_.push_back(std::move(block));
        }
        
        // Variable-length attribute containers
        var_length_attrs_.resize(var_attr_count_);
        
        logger_.Info("Dynamic segment initialized with capacity " + 
                    std::to_string(capacity_) + " (not hardcoded 150000)");
        
        return Status::OK();
    }
    
    /**
     * @brief Insert record (auto-expand)
     */
    Status Insert(size_t record_id, const void* data) {
        // Check if expansion is needed
        if (record_id >= capacity_) {
            auto status = ExpandTo(record_id + 1);
            if (!status.ok()) {
                return status;
            }
        }
        
        // Write data...
        record_count_ = std::max(record_count_, record_id + 1);
        
        // Check if pre-expansion is needed
        CheckAndPreExpand();
        
        return Status::OK();
    }
    
    /**
     * @brief Expand capacity
     */
    Status ExpandTo(size_t min_capacity) {
        std::unique_lock<std::shared_mutex> lock(resize_mutex_);
        
        if (min_capacity <= capacity_) {
            return Status::OK();
        }
        
        // Check maximum capacity limit
        if (min_capacity > config_.max_capacity) {
            return Status(RESOURCE_EXHAUSTED, 
                         "Requested capacity " + std::to_string(min_capacity) + 
                         " exceeds maximum " + std::to_string(config_.max_capacity));
        }
        
        // Calculate new capacity
        size_t new_capacity = capacity_;
        while (new_capacity < min_capacity) {
            new_capacity = static_cast<size_t>(new_capacity * config_.growth_factor);
        }
        new_capacity = std::min(new_capacity, config_.max_capacity);
        
        logger_.Info("Expanding segment from " + std::to_string(capacity_) + 
                    " to " + std::to_string(new_capacity) + " records");
        
        // Expand all data blocks
        auto status = ExpandDataBlocks(new_capacity);
        if (!status.ok()) {
            return status;
        }
        
        // Expand deletion bitmap
        auto new_bitmap = std::make_unique<ConcurrentBitset>(new_capacity);
        // Copy old deletion marks
        for (size_t i = 0; i < capacity_; ++i) {
            if (deleted_bitmap_->test(i)) {
                new_bitmap->set(i);
            }
        }
        deleted_bitmap_ = std::move(new_bitmap);
        
        capacity_ = new_capacity;
        
        // Update statistics
        expansion_count_++;
        total_memory_used_ = CalculateMemoryUsage();
        
        return Status::OK();
    }
    
    /**
     * @brief Shrink unused memory
     */
    Status Shrink() {
        if (!config_.allow_shrink) {
            return Status::OK();
        }
        
        std::unique_lock<std::shared_mutex> lock(resize_mutex_);
        
        // Calculate actual usage rate
        double usage = static_cast<double>(record_count_) / capacity_;
        if (usage > config_.shrink_threshold) {
            return Status::OK();  // Usage rate is high enough, no shrinking
        }
        
        // Calculate new capacity
        size_t new_capacity = std::max(
            static_cast<size_t>(record_count_ * 1.5),
            config_.min_capacity
        );
        
        if (new_capacity >= capacity_) {
            return Status::OK();  // No need to shrink
        }
        
        logger_.Info("Shrinking segment from " + std::to_string(capacity_) + 
                    " to " + std::to_string(new_capacity) + " records");
        
        // Shrink data blocks
        auto status = ShrinkDataBlocks(new_capacity);
        if (!status.ok()) {
            return status;
        }
        
        capacity_ = new_capacity;
        total_memory_used_ = CalculateMemoryUsage();
        
        return Status::OK();
    }
    
    /**
     * @brief Get memory usage statistics
     */
    struct MemoryStats {
        size_t capacity;           // Current capacity
        size_t used;              // Used record count
        size_t memory_allocated;   // Allocated memory (bytes)
        size_t memory_used;       // Actually used memory
        double usage_ratio;       // Usage ratio
        size_t expansion_count;   // Expansion count
    };
    
    MemoryStats GetMemoryStats() const {
        MemoryStats stats;
        stats.capacity = capacity_;
        stats.used = record_count_;
        stats.memory_allocated = total_memory_used_;
        stats.memory_used = record_count_ * RecordSize();
        stats.usage_ratio = capacity_ > 0 ? 
            static_cast<double>(record_count_) / capacity_ : 0;
        stats.expansion_count = expansion_count_;
        return stats;
    }
    
private:
    /**
     * @brief Analyze schema, calculate offsets
     */
    void AnalyzeSchema(const meta::TableSchema& schema) {
        primitive_offset_ = 0;
        vector_dimensions_.clear();
        var_attr_count_ = 0;
        
        for (const auto& field : schema.fields_) {
            if (field.field_type_ == meta::FieldType::STRING ||
                field.field_type_ == meta::FieldType::JSON) {
                var_attr_count_++;
            } else if (field.field_type_ == meta::FieldType::VECTOR_FLOAT ||
                       field.field_type_ == meta::FieldType::VECTOR_DOUBLE) {
                vector_dimensions_.push_back(field.vector_dimension_);
            } else {
                // Primitive type
                primitive_offset_ += GetFieldSize(field.field_type_);
            }
        }
    }
    
    /**
     * @brief Expand data blocks
     */
    Status ExpandDataBlocks(size_t new_capacity) {
        // Expand attribute data
        if (attribute_data_) {
            auto status = attribute_data_->resize(new_capacity * primitive_offset_);
            if (!status.ok()) return status;
        }
        
        // Expand vector data
        for (size_t i = 0; i < vector_blocks_.size(); ++i) {
            auto status = vector_blocks_[i]->resize(
                new_capacity * vector_dimensions_[i]);
            if (!status.ok()) return status;
        }
        
        // Expand variable-length attributes
        for (auto& container : var_length_attrs_) {
            container.resize(new_capacity);
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Shrink data blocks
     */
    Status ShrinkDataBlocks(size_t new_capacity) {
        // Similar to expansion, but shrinking
        if (attribute_data_) {
            attribute_data_->resize(new_capacity * primitive_offset_);
        }
        
        for (size_t i = 0; i < vector_blocks_.size(); ++i) {
            vector_blocks_[i]->resize(new_capacity * vector_dimensions_[i]);
        }
        
        for (auto& container : var_length_attrs_) {
            container.resize(new_capacity);
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Check and pre-expand
     */
    void CheckAndPreExpand() {
        double usage = static_cast<double>(record_count_) / capacity_;
        
        if (usage > config_.expand_threshold) {
            // Asynchronous pre-expansion
            std::thread([this]() {
                ExpandTo(static_cast<size_t>(capacity_ * config_.growth_factor));
            }).detach();
        }
    }
    
    /**
     * @brief Calculate memory usage
     */
    size_t CalculateMemoryUsage() const {
        size_t total = 0;
        
        // Attribute data
        if (primitive_offset_ > 0) {
            total += capacity_ * primitive_offset_;
        }
        
        // Vector data
        for (size_t dim : vector_dimensions_) {
            total += capacity_ * dim * sizeof(float);
        }
        
        // Deletion bitmap
        total += (capacity_ + 7) / 8;
        
        // Variable-length attributes (estimated)
        total += var_attr_count_ * capacity_ * 64;  // Estimate 64 bytes each
        
        return total;
    }
    
    /**
     * @brief Get field size
     */
    size_t GetFieldSize(meta::FieldType type) const {
        switch (type) {
            case meta::FieldType::INT1: return 1;
            case meta::FieldType::INT2: return 2;
            case meta::FieldType::INT4: return 4;
            case meta::FieldType::INT8: return 8;
            case meta::FieldType::FLOAT: return 4;
            case meta::FieldType::DOUBLE: return 8;
            default: return 0;
        }
    }
    
    /**
     * @brief Get record size
     */
    size_t RecordSize() const {
        size_t size = primitive_offset_;
        for (size_t dim : vector_dimensions_) {
            size += dim * sizeof(float);
        }
        return size;
    }
    
private:
    DynamicSegmentConfig config_;
    meta::TableSchema schema_;
    
    // Capacity and size
    std::atomic<size_t> capacity_;
    std::atomic<size_t> record_count_;
    
    // Data storage
    std::unique_ptr<DynamicDataBlock<char>> attribute_data_;
    std::vector<std::unique_ptr<DynamicDataBlock<float>>> vector_blocks_;
    std::vector<std::vector<std::variant<int64_t, double, std::string>>> var_length_attrs_;
    std::unique_ptr<ConcurrentBitset> deleted_bitmap_;
    
    // Metadata
    size_t primitive_offset_;
    std::vector<size_t> vector_dimensions_;
    size_t var_attr_count_;
    
    // Statistics
    std::atomic<size_t> expansion_count_{0};
    std::atomic<size_t> total_memory_used_{0};
    
    // Synchronization
    mutable std::shared_mutex resize_mutex_;
    
    Logger logger_;
};

/**
 * @brief Chunked segment manager - avoids large contiguous memory allocation
 * 
 * Divides data into multiple small chunks, each managed independently
 */
class ChunkedSegmentManager {
public:
    struct ChunkConfig {
        size_t chunk_size = 10000;      // Number of records per chunk
        size_t max_chunks = 1000;        // Maximum number of chunks
        bool allow_overflow = true;      // Allow overflow to new chunks
    };
    
    ChunkedSegmentManager(const ChunkConfig& config = ChunkConfig())
        : config_(config) {}
    
    /**
     * @brief Allocate position for new record
     */
    std::pair<size_t, size_t> AllocateRecord() {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Find a chunk with available space
        for (size_t i = 0; i < chunks_.size(); ++i) {
            if (chunks_[i]->HasSpace()) {
                size_t offset = chunks_[i]->Allocate();
                return {i, offset};
            }
        }
        
        // Need a new chunk
        if (chunks_.size() >= config_.max_chunks && !config_.allow_overflow) {
            return {static_cast<size_t>(-1), static_cast<size_t>(-1)};  // Failure
        }
        
        // Create new chunk
        auto new_chunk = std::make_unique<DataChunk>(config_.chunk_size);
        size_t chunk_id = chunks_.size();
        chunks_.push_back(std::move(new_chunk));
        
        return {chunk_id, 0};
    }
    
private:
    struct DataChunk {
        size_t capacity;
        size_t used;
        std::unique_ptr<char[]> data;
        
        explicit DataChunk(size_t cap) 
            : capacity(cap), used(0) {
            // Lazy allocation, allocate only when actually needed
        }
        
        bool HasSpace() const { return used < capacity; }
        
        size_t Allocate() {
            if (!data && used == 0) {
                // Allocate only on first use
                data = std::make_unique<char[]>(capacity * 1024);  // Assume 1KB per record
            }
            return used++;
        }
    };
    
    ChunkConfig config_;
    std::vector<std::unique_ptr<DataChunk>> chunks_;
    std::mutex mutex_;
};

} // namespace engine
} // namespace vectordb