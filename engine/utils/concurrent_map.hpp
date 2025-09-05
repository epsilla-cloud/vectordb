#pragma once

#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <vector>
#include <functional>

namespace vectordb {
namespace utils {

/**
 * @brief Thread-safe wrapper for std::unordered_map
 * 
 * This class provides thread-safe access to an unordered map using
 * reader-writer locks to allow concurrent reads while protecting writes.
 * 
 * @tparam K Key type
 * @tparam V Value type
 */
template <typename K, typename V>
class ConcurrentUnorderedMap {
public:
    using Map = std::unordered_map<K, V>;
    using SharedLock = std::shared_lock<std::shared_mutex>;
    using UniqueLock = std::unique_lock<std::shared_mutex>;

    ConcurrentUnorderedMap() = default;
    ~ConcurrentUnorderedMap() = default;

    // Disable copy operations
    ConcurrentUnorderedMap(const ConcurrentUnorderedMap&) = delete;
    ConcurrentUnorderedMap& operator=(const ConcurrentUnorderedMap&) = delete;

    // Enable move operations
    ConcurrentUnorderedMap(ConcurrentUnorderedMap&& other) noexcept {
        UniqueLock lock(other.mutex_);
        map_ = std::move(other.map_);
    }

    ConcurrentUnorderedMap& operator=(ConcurrentUnorderedMap&& other) noexcept {
        if (this != &other) {
            UniqueLock lock1(mutex_, std::defer_lock);
            UniqueLock lock2(other.mutex_, std::defer_lock);
            std::lock(lock1, lock2);
            map_ = std::move(other.map_);
        }
        return *this;
    }

    /**
     * @brief Insert or update a key-value pair
     * @return true if inserted, false if updated
     */
    bool insert_or_update(const K& key, const V& value) {
        UniqueLock lock(mutex_);
        auto [it, inserted] = map_.insert_or_assign(key, value);
        return inserted;
    }

    /**
     * @brief Insert a key-value pair only if key doesn't exist
     * @return true if inserted, false if key already exists
     */
    bool try_insert(const K& key, const V& value) {
        UniqueLock lock(mutex_);
        auto [it, inserted] = map_.try_emplace(key, value);
        return inserted;
    }

    /**
     * @brief Emplace a new element
     * @return pair of iterator and bool indicating if insertion happened
     */
    template<typename... Args>
    bool emplace(const K& key, Args&&... args) {
        UniqueLock lock(mutex_);
        auto [it, inserted] = map_.try_emplace(key, std::forward<Args>(args)...);
        return inserted;
    }

    /**
     * @brief Find an element by key
     * @return Optional containing the value if found
     */
    std::optional<V> find(const K& key) const {
        SharedLock lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    /**
     * @brief Check if a key exists
     */
    bool contains(const K& key) const {
        SharedLock lock(mutex_);
        return map_.find(key) != map_.end();
    }

    /**
     * @brief Get value by key (throws if not found)
     */
    V at(const K& key) const {
        SharedLock lock(mutex_);
        return map_.at(key);
    }

    /**
     * @brief Get value by key with default value
     */
    V get_or_default(const K& key, const V& default_value) const {
        SharedLock lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            return it->second;
        }
        return default_value;
    }

    /**
     * @brief Erase an element by key
     * @return true if erased, false if key not found
     */
    bool erase(const K& key) {
        UniqueLock lock(mutex_);
        return map_.erase(key) > 0;
    }

    /**
     * @brief Clear all elements
     */
    void clear() {
        UniqueLock lock(mutex_);
        map_.clear();
    }

    /**
     * @brief Get the size of the map
     */
    size_t size() const {
        SharedLock lock(mutex_);
        return map_.size();
    }

    /**
     * @brief Check if the map is empty
     */
    bool empty() const {
        SharedLock lock(mutex_);
        return map_.empty();
    }

    /**
     * @brief Get all keys
     */
    std::vector<K> keys() const {
        SharedLock lock(mutex_);
        std::vector<K> result;
        result.reserve(map_.size());
        for (const auto& [key, value] : map_) {
            result.push_back(key);
        }
        return result;
    }

    /**
     * @brief Get all values
     */
    std::vector<V> values() const {
        SharedLock lock(mutex_);
        std::vector<V> result;
        result.reserve(map_.size());
        for (const auto& [key, value] : map_) {
            result.push_back(value);
        }
        return result;
    }

    /**
     * @brief Apply a function to each element
     */
    void for_each(const std::function<void(const K&, const V&)>& func) const {
        SharedLock lock(mutex_);
        for (const auto& [key, value] : map_) {
            func(key, value);
        }
    }

    /**
     * @brief Apply a function to each element (mutable version)
     */
    void for_each_mut(const std::function<void(const K&, V&)>& func) {
        UniqueLock lock(mutex_);
        for (auto& [key, value] : map_) {
            func(key, value);
        }
    }

    /**
     * @brief Update a value if key exists
     * @return true if updated, false if key not found
     */
    bool update(const K& key, const std::function<void(V&)>& updater) {
        UniqueLock lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            updater(it->second);
            return true;
        }
        return false;
    }

    /**
     * @brief Conditionally update a value
     * @return true if updated, false otherwise
     */
    bool update_if(const K& key, const std::function<bool(const V&)>& condition, 
                   const std::function<void(V&)>& updater) {
        UniqueLock lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end() && condition(it->second)) {
            updater(it->second);
            return true;
        }
        return false;
    }

    /**
     * @brief Get or insert with lazy value creation
     */
    V get_or_insert_with(const K& key, const std::function<V()>& factory) {
        {
            SharedLock lock(mutex_);
            auto it = map_.find(key);
            if (it != map_.end()) {
                return it->second;
            }
        }
        
        // Upgrade to unique lock for insertion
        UniqueLock lock(mutex_);
        // Double-check after acquiring write lock
        auto it = map_.find(key);
        if (it != map_.end()) {
            return it->second;
        }
        
        V value = factory();
        map_[key] = value;
        return value;
    }

    /**
     * @brief Swap contents with another map
     */
    void swap(ConcurrentUnorderedMap& other) {
        if (this == &other) return;
        
        UniqueLock lock1(mutex_, std::defer_lock);
        UniqueLock lock2(other.mutex_, std::defer_lock);
        std::lock(lock1, lock2);
        map_.swap(other.map_);
    }

    /**
     * @brief Create a snapshot of the current map
     */
    Map snapshot() const {
        SharedLock lock(mutex_);
        return map_;
    }

    /**
     * @brief Load from a regular map
     */
    void load(const Map& new_map) {
        UniqueLock lock(mutex_);
        map_ = new_map;
    }

    /**
     * @brief Load from a regular map (move version)
     */
    void load(Map&& new_map) {
        UniqueLock lock(mutex_);
        map_ = std::move(new_map);
    }

private:
    mutable std::shared_mutex mutex_;
    Map map_;
};

} // namespace utils
} // namespace vectordb