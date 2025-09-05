#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "db/execution/vec_search_executor.hpp"
#include "logger/logger.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {
namespace execution {

/**
 * @brief Configuration for the executor pool
 */
struct ExecutorPoolConfig {
    // Initial number of executors
    size_t initial_size = 4;
    
    // Maximum number of executors
    size_t max_size = 16;
    
    // Minimum number of executors
    size_t min_size = 2;
    
    // Enable dynamic scaling
    bool enable_dynamic_scaling = true;
    
    // Time to wait before scaling down (seconds)
    int scale_down_delay_seconds = 60;
    
    // Time to wait before timing out acquire (milliseconds)
    int acquire_timeout_ms = 5000;
    
    // Enable health checking
    bool enable_health_check = true;
    
    // Health check interval (seconds)
    int health_check_interval_seconds = 30;
    
    // Enable statistics tracking
    bool enable_stats = true;
    
    // Maximum queue size for waiting requests
    size_t max_waiting_requests = 100;
    
    // Load balancing strategy
    enum LoadBalancingStrategy {
        ROUND_ROBIN,
        LEAST_USED,
        RANDOM,
        WEIGHTED
    };
    LoadBalancingStrategy load_balancing = LEAST_USED;
};

/**
 * @brief Statistics for executor pool monitoring
 */
struct ExecutorPoolStats {
    std::atomic<size_t> total_acquisitions{0};
    std::atomic<size_t> total_releases{0};
    std::atomic<size_t> current_active{0};
    std::atomic<size_t> current_available{0};
    std::atomic<size_t> total_created{0};
    std::atomic<size_t> total_destroyed{0};
    std::atomic<size_t> wait_time_ms{0};
    std::atomic<size_t> max_wait_time_ms{0};
    std::atomic<size_t> timeouts{0};
    std::atomic<size_t> failed_acquisitions{0};
    
    double GetAverageWaitTime() const {
        size_t acquisitions = total_acquisitions.load();
        return acquisitions > 0 ? 
            static_cast<double>(wait_time_ms.load()) / acquisitions : 0.0;
    }
    
    double GetUtilization() const {
        size_t total = current_active.load() + current_available.load();
        return total > 0 ? 
            static_cast<double>(current_active.load()) / total : 0.0;
    }
};

/**
 * @brief Executor wrapper with metadata
 */
class ExecutorWrapper {
public:
    ExecutorWrapper(std::shared_ptr<VecSearchExecutor> executor, size_t id)
        : executor_(executor), id_(id), created_time_(std::chrono::steady_clock::now()) {}
    
    std::shared_ptr<VecSearchExecutor> executor_;
    size_t id_;
    std::chrono::steady_clock::time_point created_time_;
    std::chrono::steady_clock::time_point last_used_time_;
    std::atomic<size_t> use_count_{0};
    std::atomic<bool> healthy_{true};
};

/**
 * @brief Optimized executor pool with dynamic scaling and load balancing
 */
class OptimizedExecutorPool : public std::enable_shared_from_this<OptimizedExecutorPool> {
public:
    using ExecutorFactory = std::function<std::shared_ptr<VecSearchExecutor>()>;
    
    OptimizedExecutorPool(ExecutorFactory factory, 
                          const ExecutorPoolConfig& config = ExecutorPoolConfig())
        : factory_(factory), config_(config), running_(true), next_executor_id_(0) {
        
        // Initialize pool with initial size
        for (size_t i = 0; i < config_.initial_size; ++i) {
            CreateExecutor();
        }
        
        // Start background threads if enabled
        if (config_.enable_dynamic_scaling) {
            scaling_thread_ = std::thread([this]() { ScalingLoop(); });
        }
        
        if (config_.enable_health_check) {
            health_check_thread_ = std::thread([this]() { HealthCheckLoop(); });
        }
        
        logger_.Info("Executor pool initialized with " + 
                    std::to_string(config_.initial_size) + " executors");
    }
    
    ~OptimizedExecutorPool() {
        Shutdown();
    }
    
    /**
     * @brief Acquire an executor from the pool
     */
    std::shared_ptr<VecSearchExecutor> Acquire(int timeout_ms = -1) {
        auto start_time = std::chrono::steady_clock::now();
        
        if (timeout_ms < 0) {
            timeout_ms = config_.acquire_timeout_ms;
        }
        
        std::unique_lock<std::mutex> lock(pool_mutex_);
        
        // Check waiting queue size
        if (waiting_count_ >= config_.max_waiting_requests) {
            stats_.failed_acquisitions++;
            return nullptr;
        }
        
        waiting_count_++;
        
        // Wait for available executor
        bool acquired = pool_cv_.wait_for(lock, 
            std::chrono::milliseconds(timeout_ms),
            [this]() { return !available_executors_.empty() || !running_; });
        
        waiting_count_--;
        
        if (!acquired || !running_) {
            stats_.timeouts++;
            stats_.failed_acquisitions++;
            return nullptr;
        }
        
        // Select executor based on load balancing strategy
        auto wrapper = SelectExecutor();
        if (!wrapper) {
            stats_.failed_acquisitions++;
            return nullptr;
        }
        
        // Move to active set
        active_executors_[wrapper->id_] = wrapper;
        
        // Update statistics
        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time).count();
        
        stats_.total_acquisitions++;
        stats_.current_active++;
        stats_.current_available--;
        stats_.wait_time_ms += wait_time;
        
        if (wait_time > stats_.max_wait_time_ms) {
            stats_.max_wait_time_ms = wait_time;
        }
        
        // Update wrapper metadata
        wrapper->last_used_time_ = std::chrono::steady_clock::now();
        wrapper->use_count_++;
        
        // Signal scaling thread if pool is getting low
        if (available_executors_.size() < 2 && config_.enable_dynamic_scaling) {
            scaling_cv_.notify_one();
        }
        
        return wrapper->executor_;
    }
    
    /**
     * @brief Release an executor back to the pool
     */
    void Release(const std::shared_ptr<VecSearchExecutor>& executor) {
        if (!executor) {
            return;
        }
        
        std::unique_lock<std::mutex> lock(pool_mutex_);
        
        // Find wrapper in active set
        std::shared_ptr<ExecutorWrapper> wrapper;
        for (auto& [id, w] : active_executors_) {
            if (w->executor_ == executor) {
                wrapper = w;
                active_executors_.erase(id);
                break;
            }
        }
        
        if (!wrapper) {
            logger_.Warning("Released executor not found in active set");
            return;
        }
        
        // Check executor health
        if (!wrapper->healthy_) {
            logger_.Info("Destroying unhealthy executor " + std::to_string(wrapper->id_));
            DestroyExecutor(wrapper);
            CreateExecutor();  // Replace with new executor
        } else {
            // Return to available pool
            available_executors_.push_back(wrapper);
            stats_.current_available++;
        }
        
        stats_.total_releases++;
        stats_.current_active--;
        
        lock.unlock();
        pool_cv_.notify_one();
    }
    
    /**
     * @brief Get pool statistics
     */
    ExecutorPoolStats GetStats() const {
        return stats_;
    }
    
    /**
     * @brief Get pool status
     */
    Status GetStatus() const {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        
        if (!running_) {
            return Status(DB_UNEXPECTED_ERROR, "Pool is shut down");
        }
        
        size_t total = available_executors_.size() + active_executors_.size();
        
        if (total < config_.min_size) {
            return Status(DB_UNEXPECTED_ERROR, "Pool below minimum size");
        }
        
        if (waiting_count_ > config_.max_waiting_requests / 2) {
            return Status(DB_UNEXPECTED_ERROR, "High contention on pool");
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Resize the pool
     */
    Status Resize(size_t new_size) {
        if (new_size < config_.min_size || new_size > config_.max_size) {
            return Status(INVALID_ARGUMENT, "Invalid pool size");
        }
        
        std::lock_guard<std::mutex> lock(pool_mutex_);
        
        size_t current_size = available_executors_.size() + active_executors_.size();
        
        if (new_size > current_size) {
            // Scale up
            for (size_t i = current_size; i < new_size; ++i) {
                CreateExecutor();
            }
        } else if (new_size < current_size) {
            // Scale down
            size_t to_remove = current_size - new_size;
            while (to_remove > 0 && !available_executors_.empty()) {
                auto wrapper = available_executors_.back();
                available_executors_.pop_back();
                DestroyExecutor(wrapper);
                to_remove--;
            }
        }
        
        return Status::OK();
    }
    
    /**
     * @brief Shutdown the pool
     */
    void Shutdown() {
        if (!running_.exchange(false)) {
            return;  // Already shut down
        }
        
        // Wake up all waiting threads
        pool_cv_.notify_all();
        scaling_cv_.notify_all();
        
        // Join background threads
        if (scaling_thread_.joinable()) {
            scaling_thread_.join();
        }
        
        if (health_check_thread_.joinable()) {
            health_check_thread_.join();
        }
        
        // Destroy all executors
        std::lock_guard<std::mutex> lock(pool_mutex_);
        
        for (auto& wrapper : available_executors_) {
            DestroyExecutor(wrapper);
        }
        available_executors_.clear();
        
        for (auto& [id, wrapper] : active_executors_) {
            DestroyExecutor(wrapper);
        }
        active_executors_.clear();
        
        logger_.Info("Executor pool shut down");
    }
    
private:
    /**
     * @brief Create a new executor
     */
    void CreateExecutor() {
        try {
            auto executor = factory_();
            if (!executor) {
                logger_.Error("Failed to create executor");
                return;
            }
            
            auto wrapper = std::make_shared<ExecutorWrapper>(executor, next_executor_id_++);
            available_executors_.push_back(wrapper);
            
            stats_.total_created++;
            stats_.current_available++;
            
            logger_.Debug("Created executor " + std::to_string(wrapper->id_));
            
        } catch (const std::exception& e) {
            logger_.Error("Exception creating executor: " + std::string(e.what()));
        }
    }
    
    /**
     * @brief Destroy an executor
     */
    void DestroyExecutor(std::shared_ptr<ExecutorWrapper> wrapper) {
        if (!wrapper) {
            return;
        }
        
        stats_.total_destroyed++;
        stats_.current_available--;
        
        logger_.Debug("Destroyed executor " + std::to_string(wrapper->id_));
    }
    
    /**
     * @brief Select an executor based on load balancing strategy
     */
    std::shared_ptr<ExecutorWrapper> SelectExecutor() {
        if (available_executors_.empty()) {
            return nullptr;
        }
        
        switch (config_.load_balancing) {
            case ExecutorPoolConfig::ROUND_ROBIN: {
                auto wrapper = available_executors_.front();
                available_executors_.pop_front();
                return wrapper;
            }
            
            case ExecutorPoolConfig::LEAST_USED: {
                auto it = std::min_element(available_executors_.begin(), 
                                          available_executors_.end(),
                    [](const auto& a, const auto& b) {
                        return a->use_count_ < b->use_count_;
                    });
                
                auto wrapper = *it;
                available_executors_.erase(it);
                return wrapper;
            }
            
            case ExecutorPoolConfig::RANDOM: {
                size_t index = std::rand() % available_executors_.size();
                auto it = available_executors_.begin();
                std::advance(it, index);
                auto wrapper = *it;
                available_executors_.erase(it);
                return wrapper;
            }
            
            case ExecutorPoolConfig::WEIGHTED:
            default: {
                // For now, use LEAST_USED as default
                return SelectExecutor();  // Will use LEAST_USED
            }
        }
    }
    
    /**
     * @brief Dynamic scaling loop
     */
    void ScalingLoop() {
        while (running_) {
            std::unique_lock<std::mutex> lock(scaling_mutex_);
            scaling_cv_.wait_for(lock, std::chrono::seconds(10));
            
            if (!running_) {
                break;
            }
            
            // Check if scaling is needed
            CheckAndScale();
        }
    }
    
    /**
     * @brief Check and perform scaling
     */
    void CheckAndScale() {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        
        size_t current_size = available_executors_.size() + active_executors_.size();
        double utilization = stats_.GetUtilization();
        
        // Scale up if high utilization
        if (utilization > 0.8 && current_size < config_.max_size) {
            size_t to_add = std::min(2UL, config_.max_size - current_size);
            for (size_t i = 0; i < to_add; ++i) {
                CreateExecutor();
            }
            logger_.Info("Scaled up pool to " + std::to_string(current_size + to_add));
        }
        
        // Scale down if low utilization for extended period
        if (utilization < 0.3 && current_size > config_.min_size) {
            auto now = std::chrono::steady_clock::now();
            
            // Check if executors have been idle for scale down delay
            std::vector<std::shared_ptr<ExecutorWrapper>> to_remove;
            for (auto& wrapper : available_executors_) {
                auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(
                    now - wrapper->last_used_time_).count();
                
                if (idle_time > config_.scale_down_delay_seconds && 
                    current_size - to_remove.size() > config_.min_size) {
                    to_remove.push_back(wrapper);
                }
            }
            
            for (auto& wrapper : to_remove) {
                auto it = std::find(available_executors_.begin(), 
                                   available_executors_.end(), wrapper);
                if (it != available_executors_.end()) {
                    available_executors_.erase(it);
                    DestroyExecutor(wrapper);
                    current_size--;
                }
            }
            
            if (!to_remove.empty()) {
                logger_.Info("Scaled down pool to " + std::to_string(current_size));
            }
        }
    }
    
    /**
     * @brief Health check loop
     */
    void HealthCheckLoop() {
        while (running_) {
            std::this_thread::sleep_for(
                std::chrono::seconds(config_.health_check_interval_seconds));
            
            if (!running_) {
                break;
            }
            
            PerformHealthCheck();
        }
    }
    
    /**
     * @brief Perform health check on executors
     */
    void PerformHealthCheck() {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        
        for (auto& wrapper : available_executors_) {
            // Simple health check - could be extended
            wrapper->healthy_ = (wrapper->executor_ != nullptr);
        }
        
        // Remove unhealthy executors
        available_executors_.erase(
            std::remove_if(available_executors_.begin(), available_executors_.end(),
                [this](const auto& wrapper) {
                    if (!wrapper->healthy_) {
                        logger_.Warning("Removing unhealthy executor " + 
                                      std::to_string(wrapper->id_));
                        DestroyExecutor(wrapper);
                        CreateExecutor();  // Replace
                        return true;
                    }
                    return false;
                }),
            available_executors_.end());
    }
    
    ExecutorFactory factory_;
    ExecutorPoolConfig config_;
    
    std::deque<std::shared_ptr<ExecutorWrapper>> available_executors_;
    std::unordered_map<size_t, std::shared_ptr<ExecutorWrapper>> active_executors_;
    
    mutable std::mutex pool_mutex_;
    std::condition_variable pool_cv_;
    
    std::mutex scaling_mutex_;
    std::condition_variable scaling_cv_;
    
    std::atomic<bool> running_;
    std::atomic<size_t> next_executor_id_;
    std::atomic<size_t> waiting_count_{0};
    
    std::thread scaling_thread_;
    std::thread health_check_thread_;
    
    ExecutorPoolStats stats_;
    Logger logger_;
};

/**
 * @brief RAII wrapper for executor acquisition
 */
class ScopedExecutor {
public:
    ScopedExecutor(std::shared_ptr<OptimizedExecutorPool> pool, 
                   std::shared_ptr<VecSearchExecutor> executor)
        : pool_(pool), executor_(executor) {}
    
    ~ScopedExecutor() {
        if (executor_ && pool_) {
            pool_->Release(executor_);
        }
    }
    
    // Disable copy
    ScopedExecutor(const ScopedExecutor&) = delete;
    ScopedExecutor& operator=(const ScopedExecutor&) = delete;
    
    // Enable move
    ScopedExecutor(ScopedExecutor&& other) noexcept
        : pool_(std::move(other.pool_)), executor_(std::move(other.executor_)) {
        other.executor_ = nullptr;
    }
    
    ScopedExecutor& operator=(ScopedExecutor&& other) noexcept {
        if (this != &other) {
            if (executor_ && pool_) {
                pool_->Release(executor_);
            }
            pool_ = std::move(other.pool_);
            executor_ = std::move(other.executor_);
            other.executor_ = nullptr;
        }
        return *this;
    }
    
    std::shared_ptr<VecSearchExecutor> operator->() const { return executor_; }
    std::shared_ptr<VecSearchExecutor> get() const { return executor_; }
    explicit operator bool() const { return executor_ != nullptr; }
    
private:
    std::shared_ptr<OptimizedExecutorPool> pool_;
    std::shared_ptr<VecSearchExecutor> executor_;
};

} // namespace execution
} // namespace engine
} // namespace vectordb