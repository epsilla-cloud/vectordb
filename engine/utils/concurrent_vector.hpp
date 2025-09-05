#pragma once

#include <mutex>
#include <shared_mutex>
#include <vector>
#include <stdexcept>

namespace vectordb {

template <typename T>
class ThreadSafeVector {
public:
    ThreadSafeVector() = default;
    
    explicit ThreadSafeVector(size_t initial_size) 
        : data_(initial_size) {}
    
    ThreadSafeVector(size_t initial_size, const T& value) 
        : data_(initial_size, value) {}

    // Thread-safe push_back
    void push_back(const T& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_.push_back(value);
    }
    
    void push_back(T&& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_.push_back(std::move(value));
    }

    // Thread-safe element access with bounds checking
    T at(size_t index) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (index >= data_.size()) {
            throw std::out_of_range("ThreadSafeVector::at: index " + 
                                   std::to_string(index) + " >= size " + 
                                   std::to_string(data_.size()));
        }
        return data_[index];
    }

    // Thread-safe element modification with bounds checking
    void set(size_t index, const T& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (index >= data_.size()) {
            throw std::out_of_range("ThreadSafeVector::set: index " + 
                                   std::to_string(index) + " >= size " + 
                                   std::to_string(data_.size()));
        }
        data_[index] = value;
    }
    
    void set(size_t index, T&& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (index >= data_.size()) {
            throw std::out_of_range("ThreadSafeVector::set: index " + 
                                   std::to_string(index) + " >= size " + 
                                   std::to_string(data_.size()));
        }
        data_[index] = std::move(value);
    }

    // Get size
    size_t size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return data_.size();
    }

    // Check if empty
    bool empty() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return data_.empty();
    }

    // Clear all elements
    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_.clear();
    }

    // Reserve capacity
    void reserve(size_t new_capacity) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_.reserve(new_capacity);
    }

    // Resize vector
    void resize(size_t new_size) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_.resize(new_size);
    }
    
    void resize(size_t new_size, const T& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        data_.resize(new_size, value);
    }

    // Apply function to all elements (read-only)
    template<typename Func>
    void for_each(Func func) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        for (const auto& item : data_) {
            func(item);
        }
    }

    // Apply function to all elements (modifying)
    template<typename Func>
    void for_each_mut(Func func) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        for (auto& item : data_) {
            func(item);
        }
    }

    // Get a snapshot copy of the vector
    std::vector<T> snapshot() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return data_;
    }

    // Swap contents with another vector
    void swap(ThreadSafeVector& other) {
        if (this == &other) return;
        
        // Lock both mutexes in a consistent order to avoid deadlock
        std::unique_lock<std::shared_mutex> lock1(mutex_, std::defer_lock);
        std::unique_lock<std::shared_mutex> lock2(other.mutex_, std::defer_lock);
        std::lock(lock1, lock2);
        
        data_.swap(other.data_);
    }

private:
    std::vector<T> data_;
    mutable std::shared_mutex mutex_;  // Allow multiple readers, single writer
};

} // namespace vectordb