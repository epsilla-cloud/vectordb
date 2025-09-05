#pragma once

#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <queue>
#include <thread>
#include <functional>
#include <future>
#include "db/execution/vec_search_executor.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

/**
 * @brief Thread-safe executor pool with proper synchronization
 * 
 * Fixes thread safety issues in the original executor pool:
 * - Proper work queue synchronization
 * - Safe executor lifecycle management
 * - Graceful shutdown handling
 */
class ThreadSafeExecutorPool {
public:
    using Task = std::function<void()>;
    
    explicit ThreadSafeExecutorPool(size_t num_threads = 0) 
        : stop_(false), 
          active_tasks_(0) {
        
        // Use hardware concurrency if not specified
        if (num_threads == 0) {
            num_threads = std::thread::hardware_concurrency();
            if (num_threads == 0) {
                num_threads = 4;  // Fallback to 4 threads
            }
        }
        
        logger_.Info("Initializing thread-safe executor pool with " + 
                    std::to_string(num_threads) + " threads");
        
        // Create worker threads
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this, i] {
                this->workerLoop(i);
            });
        }
    }
    
    ~ThreadSafeExecutorPool() {
        shutdown();
    }
    
    /**
     * @brief Submit a task to the pool
     * @return Future to get the result
     */
    template<typename Func, typename... Args>
    auto submit(Func&& func, Args&&... args) 
        -> std::future<typename std::result_of<Func(Args...)>::type> {
        
        using ReturnType = typename std::result_of<Func(Args...)>::type;
        
        // Create packaged task
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<Func>(func), std::forward<Args>(args)...)
        );
        
        std::future<ReturnType> result = task->get_future();
        
        // Add to queue with thread safety
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            
            if (stop_) {
                throw std::runtime_error("Cannot submit to stopped executor pool");
            }
            
            tasks_.emplace([task]() { (*task)(); });
        }
        
        // Notify one waiting worker
        condition_.notify_one();
        
        return result;
    }
    
    /**
     * @brief Execute search with proper synchronization
     */
    Status executeSearch(
        const std::vector<std::shared_ptr<VecSearchExecutor>>& executors,
        const SearchContext& context,
        std::vector<SearchResult>& results) {
        
        if (executors.empty()) {
            return Status(DB_UNEXPECTED_ERROR, "No executors provided");
        }
        
        // Atomic counter for completed tasks
        std::atomic<size_t> completed(0);
        std::mutex results_mutex;
        std::vector<std::future<Status>> futures;
        
        // Submit tasks
        for (size_t i = 0; i < executors.size(); ++i) {
            futures.push_back(submit([this, &executors, &context, &results, 
                                     &results_mutex, &completed, i]() -> Status {
                try {
                    SearchResult local_result;
                    auto status = executors[i]->execute(context, local_result);
                    
                    if (status.ok()) {
                        std::lock_guard<std::mutex> lock(results_mutex);
                        results[i] = std::move(local_result);
                    }
                    
                    completed.fetch_add(1, std::memory_order_release);
                    return status;
                } catch (const std::exception& e) {
                    completed.fetch_add(1, std::memory_order_release);
                    return Status(DB_UNEXPECTED_ERROR, e.what());
                }
            }));
        }
        
        // Wait for all tasks to complete
        Status final_status = Status::OK();
        for (auto& future : futures) {
            auto status = future.get();
            if (!status.ok() && final_status.ok()) {
                final_status = status;  // Record first error
            }
        }
        
        return final_status;
    }
    
    /**
     * @brief Wait for all tasks to complete
     */
    void wait() {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        completion_condition_.wait(lock, [this] {
            return tasks_.empty() && active_tasks_ == 0;
        });
    }
    
    /**
     * @brief Get number of pending tasks
     */
    size_t pendingTasks() const {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return tasks_.size();
    }
    
    /**
     * @brief Get number of active threads
     */
    size_t activeThreads() const {
        return active_tasks_.load(std::memory_order_acquire);
    }
    
private:
    /**
     * @brief Worker thread loop
     */
    void workerLoop(size_t thread_id) {
        logger_.Debug("Worker thread " + std::to_string(thread_id) + " started");
        
        while (true) {
            Task task;
            
            // Wait for task
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                
                condition_.wait(lock, [this] {
                    return stop_ || !tasks_.empty();
                });
                
                if (stop_ && tasks_.empty()) {
                    break;
                }
                
                task = std::move(tasks_.front());
                tasks_.pop();
                active_tasks_.fetch_add(1, std::memory_order_acquire);
            }
            
            // Execute task
            try {
                task();
            } catch (const std::exception& e) {
                logger_.Error("Task execution failed: " + std::string(e.what()));
            } catch (...) {
                logger_.Error("Task execution failed with unknown error");
            }
            
            // Update counters
            active_tasks_.fetch_sub(1, std::memory_order_release);
            
            // Notify if all tasks completed
            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                if (tasks_.empty() && active_tasks_ == 0) {
                    completion_condition_.notify_all();
                }
            }
        }
        
        logger_.Debug("Worker thread " + std::to_string(thread_id) + " stopped");
    }
    
    /**
     * @brief Shutdown the pool gracefully
     */
    void shutdown() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (stop_) {
                return;  // Already stopped
            }
            stop_ = true;
        }
        
        // Wake up all workers
        condition_.notify_all();
        
        // Join all threads
        for (std::thread& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        
        workers_.clear();
        
        // Clear remaining tasks
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            while (!tasks_.empty()) {
                tasks_.pop();
            }
        }
        
        logger_.Info("Executor pool shutdown complete");
    }
    
private:
    std::vector<std::thread> workers_;
    std::queue<Task> tasks_;
    
    mutable std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::condition_variable completion_condition_;
    
    std::atomic<bool> stop_;
    std::atomic<size_t> active_tasks_;
    
    Logger logger_;
};

/**
 * @brief Global executor pool singleton
 */
class GlobalExecutorPool {
public:
    static ThreadSafeExecutorPool& getInstance() {
        static ThreadSafeExecutorPool instance;
        return instance;
    }
    
private:
    GlobalExecutorPool() = default;
    ~GlobalExecutorPool() = default;
    GlobalExecutorPool(const GlobalExecutorPool&) = delete;
    GlobalExecutorPool& operator=(const GlobalExecutorPool&) = delete;
};

} // namespace engine
} // namespace vectordb