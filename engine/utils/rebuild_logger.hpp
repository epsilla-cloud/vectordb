#pragma once

#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <atomic>

// Use iostream for logging to avoid circular dependency
#include <iostream>
#include <mutex>

namespace vectordb {
namespace utils {

/**
 * Enhanced logging for rebuild operations with clear progress tracking
 */
class RebuildLogger {
public:
    enum class Phase {
        PREPARATION = 0,
        DATA_SAVE = 1,
        INDEX_BUILD = 2,
        EXECUTOR_SWAP = 3,
        VERIFICATION = 4,
        CLEANUP = 5,
        COMPLETED = 6
    };
    
    struct RebuildContext {
        std::string database_name;
        std::string table_name;
        std::string index_name;
        size_t total_records = 0;
        size_t old_index_records = 0;
        size_t table_count = 0;
        size_t index_count = 0;
        bool is_incremental = false;
        int thread_count = 1;
    };

public:
    RebuildLogger(const std::string& component = "Rebuild")
        : component_(component), start_time_(std::chrono::steady_clock::now()) {}
    
    // Start rebuild logging
    void StartRebuild(const RebuildContext& context) {
        context_ = context;
        start_time_ = std::chrono::steady_clock::now();
        phase_start_time_ = start_time_;
        
        std::stringstream ss;
        ss << "\n" << GetSeparator() << "\n";
        ss << "🔄 REBUILD STARTED\n";
        ss << GetSeparator() << "\n";
        
        if (!context.database_name.empty()) {
            ss << "📁 Database: " << context.database_name << "\n";
        }
        if (!context.table_name.empty()) {
            ss << "📊 Table: " << context.table_name << "\n";
        }
        if (!context.index_name.empty()) {
            ss << "📍 Index: " << context.index_name << "\n";
        }
        
        ss << "📈 Records: " << FormatNumber(context.total_records);
        if (context.old_index_records > 0) {
            double change_percent = ((double)(context.total_records - context.old_index_records) / 
                                   context.old_index_records) * 100.0;
            ss << " (+" << FormatNumber(context.total_records - context.old_index_records) 
               << ", " << std::fixed << std::setprecision(1) << change_percent << "% change)";
        }
        ss << "\n";
        
        ss << "⚙️  Mode: " << (context.is_incremental ? "Incremental" : "Full") << " Rebuild\n";
        ss << "🔧 Threads: " << context.thread_count << "\n";
        ss << "🕐 Started: " << GetCurrentTime() << "\n";
        ss << GetSeparator();
        
        LogInfo(ss.str());
    }
    
    // Log phase transition
    void StartPhase(Phase phase, const std::string& details = "") {
        phase_start_time_ = std::chrono::steady_clock::now();
        current_phase_ = phase;
        
        std::stringstream ss;
        ss << GetPhaseIcon(phase) << " " << GetPhaseName(phase);
        
        if (!details.empty()) {
            ss << " - " << details;
        }
        
        LogInfo(ss.str());
    }
    
    // Log phase completion
    void CompletePhase(Phase phase, const std::string& result = "") {
        auto duration = std::chrono::steady_clock::now() - phase_start_time_;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        
        std::stringstream ss;
        ss << "  ✓ " << GetPhaseName(phase) << " completed";
        if (!result.empty()) {
            ss << " - " << result;
        }
        ss << " (" << FormatDuration(ms) << ")";
        
        LogInfo(ss.str());
    }
    
    // Log progress within a phase
    void LogProgress(size_t current, size_t total, const std::string& item = "") {
        if (total == 0) return;
        
        double percent = (double)current / total * 100.0;
        
        // Only log at certain intervals to avoid spam
        if (percent < last_progress_percent_ + 10.0 && current < total) {
            return;
        }
        last_progress_percent_ = percent;
        
        std::stringstream ss;
        ss << "  📊 Progress: " << GetProgressBar(percent) 
           << " " << std::fixed << std::setprecision(1) << percent << "%"
           << " (" << current << "/" << total << ")";
        
        if (!item.empty()) {
            ss << " - " << item;
        }
        
        // Estimate time remaining
        if (current > 0 && current < total) {
            auto elapsed = std::chrono::steady_clock::now() - phase_start_time_;
            auto estimated_total = elapsed * total / current;
            auto remaining = estimated_total - elapsed;
            auto remaining_ms = std::chrono::duration_cast<std::chrono::milliseconds>(remaining).count();
            ss << " - ETA: " << FormatDuration(remaining_ms);
        }
        
        LogInfo(ss.str());
    }
    
    // Log warning
    void LogWarning(const std::string& message) {
        LogWarning("  ⚠️  " + message);
    }
    
    // Log skipped operation
    void LogSkip(const std::string& what, const std::string& reason) {
        std::stringstream ss;
        ss << "  ⏭️  Skipped " << what;
        if (!reason.empty()) {
            ss << " - " << reason;
        }
        LogInfo(ss.str());
    }
    
    // Complete rebuild logging
    void CompleteRebuild(bool success = true, const std::string& summary = "") {
        auto total_duration = std::chrono::steady_clock::now() - start_time_;
        auto total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(total_duration).count();
        
        std::stringstream ss;
        ss << "\n" << GetSeparator() << "\n";
        
        if (success) {
            ss << "✅ REBUILD COMPLETED SUCCESSFULLY\n";
        } else {
            ss << "❌ REBUILD FAILED\n";
        }
        
        ss << GetSeparator() << "\n";
        
        if (!summary.empty()) {
            ss << "📝 Summary: " << summary << "\n";
        }
        
        // Statistics
        if (success && context_.total_records > 0) {
            double records_per_second = (context_.total_records * 1000.0) / total_ms;
            ss << "📊 Performance:\n";
            ss << "  • Records processed: " << FormatNumber(context_.total_records) << "\n";
            ss << "  • Total time: " << FormatDuration(total_ms) << "\n";
            ss << "  • Throughput: " << FormatNumber(records_per_second) << " records/sec\n";
            
            if (context_.is_incremental) {
                size_t new_records = context_.total_records - context_.old_index_records;
                if (new_records > 0 && total_ms > 0) {
                    double incremental_throughput = (new_records * 1000.0) / total_ms;
                    ss << "  • Incremental throughput: " << FormatNumber(incremental_throughput) 
                       << " new records/sec\n";
                }
            }
        }
        
        ss << "🕐 Completed: " << GetCurrentTime() << "\n";
        ss << GetSeparator();
        
        LogInfo(ss.str());
    }
    
    // Log error with context
    void LogError(const std::string& error, const std::string& context = "") {
        std::stringstream ss;
        ss << "❌ ERROR: " << error;
        if (!context.empty()) {
            ss << " (Context: " << context << ")";
        }
        LogError(ss.str());
    }

private:
    std::string GetSeparator() const {
        return "════════════════════════════════════════════════════════════════";
    }
    
    std::string GetProgressBar(double percent) const {
        int filled = static_cast<int>(percent / 5);  // 20 segments
        std::string bar = "[";
        for (int i = 0; i < 20; ++i) {
            if (i < filled) {
                bar += "█";
            } else {
                bar += "░";
            }
        }
        bar += "]";
        return bar;
    }
    
    std::string GetPhaseIcon(Phase phase) const {
        switch (phase) {
            case Phase::PREPARATION: return "🔧";
            case Phase::DATA_SAVE: return "💾";
            case Phase::INDEX_BUILD: return "🏗️";
            case Phase::EXECUTOR_SWAP: return "🔄";
            case Phase::VERIFICATION: return "✔️";
            case Phase::CLEANUP: return "🧹";
            case Phase::COMPLETED: return "✅";
            default: return "▶️";
        }
    }
    
    std::string GetPhaseName(Phase phase) const {
        switch (phase) {
            case Phase::PREPARATION: return "Preparing rebuild";
            case Phase::DATA_SAVE: return "Saving data to disk";
            case Phase::INDEX_BUILD: return "Building index";
            case Phase::EXECUTOR_SWAP: return "Swapping executors";
            case Phase::VERIFICATION: return "Verifying rebuild";
            case Phase::CLEANUP: return "Cleaning up";
            case Phase::COMPLETED: return "Rebuild completed";
            default: return "Processing";
        }
    }
    
    std::string FormatDuration(int64_t milliseconds) const {
        if (milliseconds < 1000) {
            return std::to_string(milliseconds) + "ms";
        } else if (milliseconds < 60000) {
            double seconds = milliseconds / 1000.0;
            std::stringstream ss;
            ss << std::fixed << std::setprecision(1) << seconds << "s";
            return ss.str();
        } else {
            int minutes = milliseconds / 60000;
            int seconds = (milliseconds % 60000) / 1000;
            return std::to_string(minutes) + "m " + std::to_string(seconds) + "s";
        }
    }
    
    std::string FormatNumber(double number) const {
        std::stringstream ss;
        if (number >= 1000000) {
            ss << std::fixed << std::setprecision(2) << (number / 1000000.0) << "M";
        } else if (number >= 1000) {
            ss << std::fixed << std::setprecision(1) << (number / 1000.0) << "K";
        } else {
            ss << static_cast<int>(number);
        }
        return ss.str();
    }
    
    std::string GetCurrentTime() const {
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S");
        return ss.str();
    }

private:
    void LogInfo(const std::string& msg) {
        std::lock_guard<std::mutex> lock(log_mutex_);
        std::cout << "[INFO] [" << component_ << "] " << msg << std::endl;
    }
    
    void LogWarning(const std::string& msg) {
        std::lock_guard<std::mutex> lock(log_mutex_);
        std::cout << "[WARN] [" << component_ << "] " << msg << std::endl;
    }
    
    void LogError(const std::string& msg) {
        std::lock_guard<std::mutex> lock(log_mutex_);
        std::cerr << "[ERROR] [" << component_ << "] " << msg << std::endl;
    }

private:
    std::string component_;
    mutable std::mutex log_mutex_;
    RebuildContext context_;
    Phase current_phase_;
    std::chrono::steady_clock::time_point start_time_;
    std::chrono::steady_clock::time_point phase_start_time_;
    double last_progress_percent_ = 0.0;
};

} // namespace utils
} // namespace vectordb