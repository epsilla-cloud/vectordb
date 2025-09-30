#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

/**
 * @brief Monitor insertion rate and predict future capacity needs
 *
 * This class tracks the rate of insertions over time and can predict
 * when the table will need to be resized, allowing for pre-emptive
 * expansion to avoid resize during peak load.
 */
class InsertionRateMonitor {
public:
    struct InsertionStats {
        size_t count;
        std::chrono::steady_clock::time_point timestamp;
    };

    InsertionRateMonitor(size_t window_size = 10)
        : window_size_(window_size),
          total_insertions_(0),
          peak_rate_(0.0) {
        start_time_ = std::chrono::steady_clock::now();
    }

    /**
     * @brief Record an insertion event
     * @param count Number of records inserted
     */
    void RecordInsertion(size_t count = 1) {
        auto now = std::chrono::steady_clock::now();

        std::lock_guard<std::mutex> lock(mutex_);

        total_insertions_ += count;
        insertion_history_.push_back({count, now});

        // Keep only recent history (within window)
        while (insertion_history_.size() > window_size_) {
            insertion_history_.pop_front();
        }

        // Update current rate
        UpdateRate();
    }

    /**
     * @brief Get the current insertion rate (records per second)
     */
    double GetCurrentRate() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return current_rate_;
    }

    /**
     * @brief Get the peak insertion rate observed
     */
    double GetPeakRate() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return peak_rate_;
    }

    /**
     * @brief Predict when capacity will be exhausted based on current rate
     * @param current_usage Current number of records
     * @param current_capacity Current table capacity
     * @return Estimated seconds until capacity exhausted (or -1 if rate is 0)
     */
    double PredictTimeToCapacity(size_t current_usage, size_t current_capacity) const {
        std::lock_guard<std::mutex> lock(mutex_);

        if (current_rate_ <= 0 || current_usage >= current_capacity) {
            return -1;
        }

        size_t remaining = current_capacity - current_usage;
        return static_cast<double>(remaining) / current_rate_;
    }

    /**
     * @brief Check if we should pre-expand based on current rate
     * @param current_usage Current number of records
     * @param current_capacity Current table capacity
     * @param threshold_seconds If capacity will be exhausted within this time, suggest expansion
     * @return true if pre-expansion is recommended
     */
    bool ShouldPreExpand(size_t current_usage, size_t current_capacity,
                         double threshold_seconds = 30.0) const {
        double time_to_capacity = PredictTimeToCapacity(current_usage, current_capacity);

        // Pre-expand if we'll hit capacity within threshold
        if (time_to_capacity > 0 && time_to_capacity < threshold_seconds) {
            logger_.Info("Predictive expansion recommended: capacity will be exhausted in " +
                        std::to_string(time_to_capacity) + " seconds at current rate of " +
                        std::to_string(current_rate_) + " records/sec");
            return true;
        }

        // Also pre-expand if we're above 80% capacity and seeing high insertion rate
        double usage_ratio = static_cast<double>(current_usage) / current_capacity;
        if (usage_ratio > 0.8 && current_rate_ > 100) {  // More than 100 inserts/sec
            logger_.Info("Predictive expansion recommended: high usage (" +
                        std::to_string(usage_ratio * 100) + "%) with high rate (" +
                        std::to_string(current_rate_) + " records/sec)");
            return true;
        }

        return false;
    }

    /**
     * @brief Calculate recommended new capacity based on predicted growth
     * @param current_capacity Current table capacity
     * @param prediction_window_seconds How far ahead to predict (default 60 seconds)
     * @return Recommended new capacity
     */
    size_t GetRecommendedCapacity(size_t current_capacity,
                                  double prediction_window_seconds = 60.0) const {
        std::lock_guard<std::mutex> lock(mutex_);

        // Use peak rate for conservative estimation
        double rate_to_use = std::max(current_rate_, peak_rate_ * 0.8);

        // Predict how many records we'll need in the prediction window
        size_t predicted_insertions = static_cast<size_t>(rate_to_use * prediction_window_seconds);

        // Add 20% buffer
        size_t recommended = current_capacity + static_cast<size_t>(predicted_insertions * 1.2);

        // Ensure at least 2x growth for efficiency
        return std::max(recommended, current_capacity * 2);
    }

    /**
     * @brief Reset all statistics
     */
    void Reset() {
        std::lock_guard<std::mutex> lock(mutex_);
        insertion_history_.clear();
        total_insertions_ = 0;
        current_rate_ = 0;
        peak_rate_ = 0;
        start_time_ = std::chrono::steady_clock::now();
    }

    /**
     * @brief Get summary statistics
     */
    std::string GetStats() const {
        std::lock_guard<std::mutex> lock(mutex_);

        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - start_time_).count();

        std::string stats = "Insertion Rate Statistics:\n";
        stats += "  Total insertions: " + std::to_string(total_insertions_) + "\n";
        stats += "  Duration: " + std::to_string(duration) + " seconds\n";
        stats += "  Current rate: " + std::to_string(current_rate_) + " records/sec\n";
        stats += "  Peak rate: " + std::to_string(peak_rate_) + " records/sec\n";
        stats += "  Average rate: " + std::to_string(
            duration > 0 ? static_cast<double>(total_insertions_) / duration : 0) + " records/sec\n";

        return stats;
    }

private:
    void UpdateRate() {
        if (insertion_history_.size() < 2) {
            current_rate_ = 0;
            return;
        }

        // Calculate rate over the history window
        auto& first = insertion_history_.front();
        auto& last = insertion_history_.back();

        auto time_diff = std::chrono::duration_cast<std::chrono::milliseconds>(
            last.timestamp - first.timestamp).count();

        if (time_diff > 0) {
            size_t total_in_window = 0;
            for (const auto& stat : insertion_history_) {
                total_in_window += stat.count;
            }

            // Convert to records per second
            current_rate_ = (total_in_window * 1000.0) / time_diff;

            // Update peak rate
            if (current_rate_ > peak_rate_) {
                peak_rate_ = current_rate_;
            }
        }
    }

    mutable std::mutex mutex_;
    std::deque<InsertionStats> insertion_history_;
    size_t window_size_;
    std::atomic<size_t> total_insertions_;
    double current_rate_;
    double peak_rate_;
    std::chrono::steady_clock::time_point start_time_;
    mutable vectordb::engine::Logger logger_;
};

}  // namespace engine
}  // namespace vectordb