/**
 * @file worker_pool_stats_handler.hpp
 * @brief Handler for worker pool statistics API endpoint
 */

#pragma once

#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <string>

#include "db/execution/worker_pool.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace server {
namespace web {

/**
 * @brief Handler for worker pool statistics endpoint
 */
class WorkerPoolStatsHandler {
public:
    /**
     * @brief Get worker pool statistics as JSON
     * @return JSON object containing CPU and IO pool statistics
     */
    static std::string GetStats() {
        vectordb::Json response;
        response.LoadFromString("{}");

        try {
            auto& manager = engine::execution::WorkerPoolManager::GetInstance();

            if (!manager.IsInitialized()) {
                vectordb::Json error;
                error.LoadFromString("{}");
                error.SetString("error", "Worker pool not initialized");
                error.SetInt("statusCode", 503);
                return error.DumpToString();
            }

            // Get CPU pool statistics
            auto cpu_stats = manager.GetCpuStats();
            vectordb::Json cpu_json;
            cpu_json.LoadFromString("{}");
            cpu_json.SetString("pool_name", "CPU");
            cpu_json.SetInt("tasks_submitted", cpu_stats.tasks_submitted);
            cpu_json.SetInt("tasks_completed", cpu_stats.tasks_completed);
            cpu_json.SetInt("tasks_failed", cpu_stats.tasks_failed);
            cpu_json.SetInt("tasks_rejected", cpu_stats.tasks_rejected);
            cpu_json.SetInt("current_queue_size", cpu_stats.current_queue_size);
            cpu_json.SetInt("peak_queue_size", cpu_stats.peak_queue_size);
            cpu_json.SetInt("active_workers", cpu_stats.active_workers);
            cpu_json.SetDouble("avg_wait_time_us", cpu_stats.GetAverageWaitTime());
            cpu_json.SetDouble("avg_exec_time_us", cpu_stats.GetAverageExecTime());
            cpu_json.SetDouble("throughput_ops_per_sec", cpu_stats.GetThroughput());

            // Get IO pool statistics
            auto io_stats = manager.GetIoStats();
            vectordb::Json io_json;
            io_json.LoadFromString("{}");
            io_json.SetString("pool_name", "IO");
            io_json.SetInt("tasks_submitted", io_stats.tasks_submitted);
            io_json.SetInt("tasks_completed", io_stats.tasks_completed);
            io_json.SetInt("tasks_failed", io_stats.tasks_failed);
            io_json.SetInt("tasks_rejected", io_stats.tasks_rejected);
            io_json.SetInt("current_queue_size", io_stats.current_queue_size);
            io_json.SetInt("peak_queue_size", io_stats.peak_queue_size);
            io_json.SetInt("active_workers", io_stats.active_workers);
            io_json.SetDouble("avg_wait_time_us", io_stats.GetAverageWaitTime());
            io_json.SetDouble("avg_exec_time_us", io_stats.GetAverageExecTime());
            io_json.SetDouble("throughput_ops_per_sec", io_stats.GetThroughput());

            // Combine into response
            response.SetInt("statusCode", 200);
            response.SetString("message", "Worker pool statistics retrieved successfully");

            // Create pools array
            std::vector<Json> pools_array;
            pools_array.push_back(cpu_json);
            pools_array.push_back(io_json);
            response.SetArray("pools", pools_array);

            // Add summary
            vectordb::Json summary;
            summary.LoadFromString("{}");
            summary.SetInt("total_tasks_completed",
                          cpu_stats.tasks_completed + io_stats.tasks_completed);
            summary.SetInt("total_tasks_failed",
                          cpu_stats.tasks_failed + io_stats.tasks_failed);
            summary.SetInt("total_active_workers",
                          cpu_stats.active_workers + io_stats.active_workers);

            response.SetObject("summary", summary);

            return response.DumpToString();

        } catch (const std::exception& e) {
            vectordb::Json error;
            error.LoadFromString("{}");
            error.SetString("error", std::string("Failed to retrieve worker pool statistics: ") + e.what());
            error.SetInt("statusCode", 500);
            return error.DumpToString();
        }
    }

    /**
     * @brief Get worker pool statistics in human-readable format
     * @return Human-readable statistics string
     */
    static std::string GetStatsText() {
        try {
            auto& manager = engine::execution::WorkerPoolManager::GetInstance();

            if (!manager.IsInitialized()) {
                return "Worker pool not initialized\n";
            }

            std::ostringstream oss;

            auto cpu_stats = manager.GetCpuStats();
            oss << "=== CPU Worker Pool ===\n";
            oss << "Tasks submitted:  " << cpu_stats.tasks_submitted << "\n";
            oss << "Tasks completed:  " << cpu_stats.tasks_completed << "\n";
            oss << "Tasks failed:     " << cpu_stats.tasks_failed << "\n";
            oss << "Tasks rejected:   " << cpu_stats.tasks_rejected << "\n";
            oss << "Current queue:    " << cpu_stats.current_queue_size << "\n";
            oss << "Peak queue:       " << cpu_stats.peak_queue_size << "\n";
            oss << "Active workers:   " << cpu_stats.active_workers << "\n";
            oss << "Avg wait time:    " << std::fixed << std::setprecision(2)
                << cpu_stats.GetAverageWaitTime() << " us\n";
            oss << "Avg exec time:    " << std::fixed << std::setprecision(2)
                << cpu_stats.GetAverageExecTime() << " us\n";
            oss << "Throughput:       " << std::fixed << std::setprecision(0)
                << cpu_stats.GetThroughput() << " ops/sec\n";

            auto io_stats = manager.GetIoStats();
            oss << "\n=== IO Worker Pool ===\n";
            oss << "Tasks submitted:  " << io_stats.tasks_submitted << "\n";
            oss << "Tasks completed:  " << io_stats.tasks_completed << "\n";
            oss << "Tasks failed:     " << io_stats.tasks_failed << "\n";
            oss << "Tasks rejected:   " << io_stats.tasks_rejected << "\n";
            oss << "Current queue:    " << io_stats.current_queue_size << "\n";
            oss << "Peak queue:       " << io_stats.peak_queue_size << "\n";
            oss << "Active workers:   " << io_stats.active_workers << "\n";
            oss << "Avg wait time:    " << std::fixed << std::setprecision(2)
                << io_stats.GetAverageWaitTime() << " us\n";
            oss << "Avg exec time:    " << std::fixed << std::setprecision(2)
                << io_stats.GetAverageExecTime() << " us\n";
            oss << "Throughput:       " << std::fixed << std::setprecision(0)
                << io_stats.GetThroughput() << " ops/sec\n";

            return oss.str();

        } catch (const std::exception& e) {
            return std::string("Error: ") + e.what() + "\n";
        }
    }
};

} // namespace web
} // namespace server
} // namespace vectordb
