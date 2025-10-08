#pragma once

#include <memory>
#include <cstring>
#include <vector>
#include <variant>

namespace vectordb {
namespace engine {

/**
 * @brief Safe variable length attribute data with proper RAII
 * 
 * This class properly implements the Rule of Five to prevent
 * memory leaks and double-free errors
 */
class SafeVariableLenAttrData {
public:
    /**
     * @brief Default constructor
     */
    SafeVariableLenAttrData() : length_(0) {}
    
    /**
     * @brief Construct with specified length
     */
    explicit SafeVariableLenAttrData(size_t len) 
        : length_(len), data_(std::make_unique<char[]>(len)) {}
    
    /**
     * @brief Copy constructor - deep copy
     */
    SafeVariableLenAttrData(const SafeVariableLenAttrData& other) 
        : length_(other.length_) {
        if (length_ > 0 && other.data_) {
            data_ = std::make_unique<char[]>(length_);
            std::memcpy(data_.get(), other.data_.get(), length_);
        }
    }
    
    /**
     * @brief Move constructor
     */
    SafeVariableLenAttrData(SafeVariableLenAttrData&& other) noexcept
        : length_(other.length_), data_(std::move(other.data_)) {
        other.length_ = 0;
    }
    
    /**
     * @brief Copy assignment - deep copy
     */
    SafeVariableLenAttrData& operator=(const SafeVariableLenAttrData& other) {
        if (this != &other) {
            length_ = other.length_;
            if (length_ > 0 && other.data_) {
                data_ = std::make_unique<char[]>(length_);
                std::memcpy(data_.get(), other.data_.get(), length_);
            } else {
                data_.reset();
            }
        }
        return *this;
    }
    
    /**
     * @brief Move assignment
     */
    SafeVariableLenAttrData& operator=(SafeVariableLenAttrData&& other) noexcept {
        if (this != &other) {
            length_ = other.length_;
            data_ = std::move(other.data_);
            other.length_ = 0;
        }
        return *this;
    }
    
    /**
     * @brief Destructor - automatic with unique_ptr
     */
    ~SafeVariableLenAttrData() = default;
    
    /**
     * @brief Get data pointer
     */
    char* data() { return data_.get(); }
    const char* data() const { return data_.get(); }
    
    /**
     * @brief Get length
     */
    size_t length() const { return length_; }
    
    /**
     * @brief Check if empty
     */
    bool empty() const { return length_ == 0 || !data_; }
    
    /**
     * @brief Resize the buffer
     */
    void resize(size_t new_len) {
        if (new_len != length_) {
            auto new_data = std::make_unique<char[]>(new_len);
            if (data_ && length_ > 0) {
                size_t copy_len = std::min(length_, new_len);
                std::memcpy(new_data.get(), data_.get(), copy_len);
            }
            data_ = std::move(new_data);
            length_ = new_len;
        }
    }
    
    /**
     * @brief Clear the data
     */
    void clear() {
        data_.reset();
        length_ = 0;
    }
    
private:
    size_t length_;
    std::unique_ptr<char[]> data_;
};

/**
 * @brief Safe attribute table with proper RAII
 */
class SafeAttributeTable {
public:
    /**
     * @brief Default constructor
     */
    SafeAttributeTable() : length_(0) {}
    
    /**
     * @brief Construct with specified length
     */
    explicit SafeAttributeTable(size_t len)
        : length_(len), data_(std::make_unique<char[]>(len)) {
        // Zero-initialize for safety
        std::memset(data_.get(), 0, len);
    }
    
    /**
     * @brief Copy constructor - deep copy
     */
    SafeAttributeTable(const SafeAttributeTable& other)
        : length_(other.length_) {
        if (length_ > 0 && other.data_) {
            data_ = std::make_unique<char[]>(length_);
            std::memcpy(data_.get(), other.data_.get(), length_);
        }
    }
    
    /**
     * @brief Move constructor
     */
    SafeAttributeTable(SafeAttributeTable&& other) noexcept
        : length_(other.length_), data_(std::move(other.data_)) {
        other.length_ = 0;
    }
    
    /**
     * @brief Copy assignment - deep copy
     */
    SafeAttributeTable& operator=(const SafeAttributeTable& other) {
        if (this != &other) {
            length_ = other.length_;
            if (length_ > 0 && other.data_) {
                data_ = std::make_unique<char[]>(length_);
                std::memcpy(data_.get(), other.data_.get(), length_);
            } else {
                data_.reset();
            }
        }
        return *this;
    }
    
    /**
     * @brief Move assignment
     */
    SafeAttributeTable& operator=(SafeAttributeTable&& other) noexcept {
        if (this != &other) {
            length_ = other.length_;
            data_ = std::move(other.data_);
            other.length_ = 0;
        }
        return *this;
    }
    
    /**
     * @brief Destructor - automatic with unique_ptr
     */
    ~SafeAttributeTable() = default;
    
    /**
     * @brief Get data pointer
     */
    char* data() { return data_.get(); }
    const char* data() const { return data_.get(); }
    
    /**
     * @brief Get length
     */
    size_t length() const { return length_; }
    
    /**
     * @brief Check if empty
     */
    bool empty() const { return length_ == 0 || !data_; }
    
    /**
     * @brief Access element at offset
     */
    template<typename T>
    T* at(size_t offset) {
        if (offset + sizeof(T) > length_) {
            throw std::out_of_range("Attribute table access out of bounds");
        }
        return reinterpret_cast<T*>(data_.get() + offset);
    }
    
    template<typename T>
    const T* at(size_t offset) const {
        if (offset + sizeof(T) > length_) {
            throw std::out_of_range("Attribute table access out of bounds");
        }
        return reinterpret_cast<const T*>(data_.get() + offset);
    }
    
    /**
     * @brief Write data at offset
     */
    void write(size_t offset, const void* src, size_t len) {
        if (offset + len > length_) {
            throw std::out_of_range("Attribute table write out of bounds");
        }
        std::memcpy(data_.get() + offset, src, len);
    }
    
    /**
     * @brief Read data from offset
     */
    void read(size_t offset, void* dst, size_t len) const {
        if (offset + len > length_) {
            throw std::out_of_range("Attribute table read out of bounds");
        }
        std::memcpy(dst, data_.get() + offset, len);
    }
    
private:
    size_t length_;
    std::unique_ptr<char[]> data_;
};

/**
 * @brief Safe vector table for float vectors
 */
class SafeVectorTable {
public:
    /**
     * @brief Default constructor
     */
    SafeVectorTable() : num_vectors_(0), dimension_(0) {}
    
    /**
     * @brief Construct with specified dimensions
     */
    SafeVectorTable(size_t num_vectors, size_t dimension)
        : num_vectors_(num_vectors), dimension_(dimension) {
        if (num_vectors > 0 && dimension > 0) {
            data_ = std::make_unique<float[]>(num_vectors * dimension);
            // Zero-initialize for safety
            std::memset(data_.get(), 0, num_vectors * dimension * sizeof(float));
        }
    }
    
    /**
     * @brief Copy constructor - deep copy
     */
    SafeVectorTable(const SafeVectorTable& other)
        : num_vectors_(other.num_vectors_), dimension_(other.dimension_) {
        if (num_vectors_ > 0 && dimension_ > 0 && other.data_) {
            size_t total_size = num_vectors_ * dimension_;
            data_ = std::make_unique<float[]>(total_size);
            std::memcpy(data_.get(), other.data_.get(), total_size * sizeof(float));
        }
    }
    
    /**
     * @brief Move constructor
     */
    SafeVectorTable(SafeVectorTable&& other) noexcept
        : num_vectors_(other.num_vectors_), 
          dimension_(other.dimension_),
          data_(std::move(other.data_)) {
        other.num_vectors_ = 0;
        other.dimension_ = 0;
    }
    
    /**
     * @brief Copy assignment - deep copy
     */
    SafeVectorTable& operator=(const SafeVectorTable& other) {
        if (this != &other) {
            num_vectors_ = other.num_vectors_;
            dimension_ = other.dimension_;
            if (num_vectors_ > 0 && dimension_ > 0 && other.data_) {
                size_t total_size = num_vectors_ * dimension_;
                data_ = std::make_unique<float[]>(total_size);
                std::memcpy(data_.get(), other.data_.get(), total_size * sizeof(float));
            } else {
                data_.reset();
            }
        }
        return *this;
    }
    
    /**
     * @brief Move assignment
     */
    SafeVectorTable& operator=(SafeVectorTable&& other) noexcept {
        if (this != &other) {
            num_vectors_ = other.num_vectors_;
            dimension_ = other.dimension_;
            data_ = std::move(other.data_);
            other.num_vectors_ = 0;
            other.dimension_ = 0;
        }
        return *this;
    }
    
    /**
     * @brief Destructor - automatic with unique_ptr
     */
    ~SafeVectorTable() = default;
    
    /**
     * @brief Get data pointer
     */
    float* data() { return data_.get(); }
    const float* data() const { return data_.get(); }
    
    /**
     * @brief Get vector at index
     */
    float* get_vector(size_t index) {
        if (index >= num_vectors_) {
            throw std::out_of_range("Vector index out of bounds");
        }
        return data_.get() + index * dimension_;
    }
    
    const float* get_vector(size_t index) const {
        if (index >= num_vectors_) {
            throw std::out_of_range("Vector index out of bounds");
        }
        return data_.get() + index * dimension_;
    }
    
    /**
     * @brief Set vector at index
     */
    void set_vector(size_t index, const float* vector) {
        if (index >= num_vectors_) {
            throw std::out_of_range("Vector index out of bounds");
        }
        std::memcpy(data_.get() + index * dimension_, vector, dimension_ * sizeof(float));
    }
    
    /**
     * @brief Get number of vectors
     */
    size_t num_vectors() const { return num_vectors_; }
    
    /**
     * @brief Get dimension
     */
    size_t dimension() const { return dimension_; }
    
    /**
     * @brief Check if empty
     */
    bool empty() const { return num_vectors_ == 0 || dimension_ == 0 || !data_; }
    
    /**
     * @brief Resize the table
     */
    void resize(size_t new_num_vectors) {
        if (new_num_vectors != num_vectors_ && dimension_ > 0) {
            auto new_data = std::make_unique<float[]>(new_num_vectors * dimension_);
            
            if (data_ && num_vectors_ > 0) {
                size_t copy_vectors = std::min(num_vectors_, new_num_vectors);
                std::memcpy(new_data.get(), data_.get(), 
                           copy_vectors * dimension_ * sizeof(float));
            }
            
            // Zero-initialize new vectors if expanding
            if (new_num_vectors > num_vectors_) {
                size_t new_vectors = new_num_vectors - num_vectors_;
                std::memset(new_data.get() + num_vectors_ * dimension_, 0, 
                           new_vectors * dimension_ * sizeof(float));
            }
            
            data_ = std::move(new_data);
            num_vectors_ = new_num_vectors;
        }
    }
    
private:
    size_t num_vectors_;
    size_t dimension_;
    std::unique_ptr<float[]> data_;
};

/**
 * @brief Thread-safe wrapper for variable length attributes
 */
using SafeVarLenAttr = std::variant<int64_t, double, std::string>;
using SafeVarLenAttrContainer = std::vector<SafeVarLenAttr>;

} // namespace engine
} // namespace vectordb