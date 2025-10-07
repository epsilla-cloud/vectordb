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
        response.SetObject();

        try {
            auto& manager = engine::execution::WorkerPoolManager::GetInstance();

            if (!manager.IsInitialized()) {
                vectordb::Json error;
                error.SetObject();
                error.AddMember("error", "Worker pool not initialized", error.GetAllocator());
                error.AddMember("statusCode", 503, error.GetAllocator());
                return error.DumpToString();
            }

            // Get CPU pool statistics
            auto cpu_stats = manager.GetCpuStats();
            vectordb::Json cpu_json;
            cpu_json.SetObject();
            cpu_json.AddMember("pool_name", "CPU", cpu_json.GetAllocator());
            cpu_json.AddMember("tasks_submitted", cpu_stats.tasks_submitted, cpu_json.GetAllocator());
            cpu_json.AddMember("tasks_completed", cpu_stats.tasks_completed, cpu_json.GetAllocator());
            cpu_json.AddMember("tasks_failed", cpu_stats.tasks_failed, cpu_json.GetAllocator());
            cpu_json.AddMember("tasks_rejected", cpu_stats.tasks_rejected, cpu_json.GetAllocator());
            cpu_json.AddMember("current_queue_size", cpu_stats.current_queue_size, cpu_json.GetAllocator());
            cpu_json.AddMember("peak_queue_size", cpu_stats.peak_queue_size, cpu_json.GetAllocator());
            cpu_json.AddMember("active_workers", cpu_stats.active_workers, cpu_json.GetAllocator());
            cpu_json.AddMember("avg_wait_time_us", cpu_stats.GetAverageWaitTime(), cpu_json.GetAllocator());
            cpu_json.AddMember("avg_exec_time_us", cpu_stats.GetAverageExecTime(), cpu_json.GetAllocator());
            cpu_json.AddMember("throughput_ops_per_sec", cpu_stats.GetThroughput(), cpu_json.GetAllocator());

            // Get IO pool statistics
            auto io_stats = manager.GetIoStats();
            vectordb::Json io_json;
            io_json.SetObject();
            io_json.AddMember("pool_name", "IO", io_json.GetAllocator());
            io_json.AddMember("tasks_submitted", io_stats.tasks_submitted, io_json.GetAllocator());
            io_json.AddMember("tasks_completed", io_stats.tasks_completed, io_json.GetAllocator());
            io_json.AddMember("tasks_failed", io_stats.tasks_failed, io_json.GetAllocator());
            io_json.AddMember("tasks_rejected", io_stats.tasks_rejected, io_json.GetAllocator());
            io_json.AddMember("current_queue_size", io_stats.current_queue_size, io_json.GetAllocator());
            io_json.AddMember("peak_queue_size", io_stats.peak_queue_size, io_json.GetAllocator());
            io_json.AddMember("active_workers", io_stats.active_workers, io_json.GetAllocator());
            io_json.AddMember("avg_wait_time_us", io_stats.GetAverageWaitTime(), io_json.GetAllocator());
            io_json.AddMember("avg_exec_time_us", io_stats.GetAverageExecTime(), io_json.GetAllocator());
            io_json.AddMember("throughput_ops_per_sec", io_stats.GetThroughput(), io_json.GetAllocator());

            // Combine into response
            response.AddMember("statusCode", 200, response.GetAllocator());
            response.AddMember("message", "Worker pool statistics retrieved successfully", response.GetAllocator());

            vectordb::Json pools_array;
            pools_array.SetArray();
            pools_array.PushBack(cpu_json, response.GetAllocator());
            pools_array.PushBack(io_json, response.GetAllocator());

            response.AddMember("pools", pools_array, response.GetAllocator());

            // Add summary
            vectordb::Json summary;
            summary.SetObject();
            summary.AddMember("total_tasks_completed",
                            cpu_stats.tasks_completed + io_stats.tasks_completed,
                            summary.GetAllocator());
            summary.AddMember("total_tasks_failed",
                            cpu_stats.tasks_failed + io_stats.tasks_failed,
                            summary.GetAllocator());
            summary.AddMember("total_active_workers",
                            cpu_stats.active_workers + io_stats.active_workers,
                            summary.GetAllocator());

            response.AddMember("summary", summary, response.GetAllocator());

            return response.DumpToString();

        } catch (const std::exception& e) {
            vectordb::Json error;
            error.SetObject();
            error.AddMember("error", std::string("Failed to retrieve worker pool statistics: ") + e.what(),
                          error.GetAllocator());
            error.AddMember("statusCode", 500, error.GetAllocator());
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
