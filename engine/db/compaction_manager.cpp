#include "db/compaction_manager.hpp"
#include "db/table.hpp"
#include "db/table_segment.hpp"
#include "db/wal_compaction_coordinator.hpp"
#include "db/execution/worker_pool.hpp"
#include "config/config.hpp"
#include "utils/common_util.hpp"
#include <chrono>
#include <thread>
#include <cstring>

namespace vectordb {
namespace engine {

CompactionManager::CompactionManager()
    : tables_(16),
      table_stats_(16),
      logger_() {
  // Load configuration from global config
  auto_compaction_enabled_ = globalConfig.AutoCompaction.load();
  compaction_threshold_ = globalConfig.CompactionThreshold.load();
  compaction_interval_ = globalConfig.CompactionInterval.load();
  min_vectors_for_compaction_ = globalConfig.MinVectorsForCompaction.load();
}

CompactionManager::~CompactionManager() {
  Stop();
}

Status CompactionManager::Start() {
  if (!auto_compaction_enabled_.load()) {
    logger_.Info("Auto compaction is disabled");
    return Status::OK();
  }

  if (worker_thread_) {
    return Status(DB_UNEXPECTED_ERROR, "Compaction worker already running");
  }

  stop_worker_ = false;
  worker_thread_ = std::make_unique<std::thread>(&CompactionManager::CompactionWorker, this);

  logger_.Info("Compaction manager started with threshold=" +
               std::to_string(compaction_threshold_.load() * 100) + "%, interval=" +
               std::to_string(compaction_interval_.load()) + "s");

  return Status::OK();
}

Status CompactionManager::Stop() {
  if (!worker_thread_) {
    return Status::OK();
  }

  stop_worker_ = true;
  compaction_cv_.notify_all();

  if (worker_thread_->joinable()) {
    worker_thread_->join();
  }

  worker_thread_.reset();
  logger_.Info("Compaction manager stopped");

  return Status::OK();
}

void CompactionManager::CompactionWorker() {
  logger_.Info("Compaction worker thread started");

  while (!stop_worker_.load()) {
    // Wait for the next check interval
    {
      std::unique_lock<std::mutex> lock(compaction_mutex_);
      compaction_cv_.wait_for(
        lock,
        std::chrono::seconds(compaction_interval_.load()),
        [this] { return stop_worker_.load(); }
      );
    }

    if (stop_worker_.load()) {
      break;
    }

    // Check all registered tables for compaction need
    std::vector<std::string> tables_to_compact;

    // Iterate over tracked table names
    {
      std::lock_guard<std::mutex> lock(table_names_mutex_);
      for (const auto& table_name : table_names_) {
        // Get the table and check if it needs compaction
        auto table_result = tables_.Get(table_name);
        if (table_result.first && table_result.second != nullptr) {
          std::shared_ptr<Table> table = table_result.second;
          if (table->NeedsCompaction(compaction_threshold_.load())) {
            tables_to_compact.push_back(table_name);
            // Update stats while we have the table
            UpdateStats(table_name, table->GetRecordCount(),
                       static_cast<size_t>(table->GetRecordCount() *
                       (table->table_segment_ ? table->table_segment_->GetDeletedRatio() : 0.0)));
          }
        }
      }
    }

    // Perform compaction on tables that need it
    for (const auto& table_name : tables_to_compact) {
      // Check if it's a good time to compact
      if (!IsLowTrafficPeriod() && !IsMemoryPressureHigh()) {
        logger_.Debug("Skipping compaction - not low traffic period and no memory pressure");
        continue;
      }

      logger_.Info("Starting compaction for table: " + table_name);
      auto status = DoCompaction(table_name);

      if (status.ok()) {
        logger_.Info("Compaction completed successfully for table: " + table_name);
      } else {
        logger_.Warning("Compaction failed for table " + table_name + ": " + status.message());
      }
    }
  }

  logger_.Info("Compaction worker thread stopped");
}

Status CompactionManager::CompactTable(const std::string& table_name) {
  logger_.Info("Manual compaction requested for table: " + table_name);
  return DoCompaction(table_name);
}

Status CompactionManager::CompactAllTables() {
  logger_.Info("Manual compaction requested for all tables");

  std::vector<std::string> tables_to_compact;
  {
    std::lock_guard<std::mutex> lock(table_names_mutex_);
    tables_to_compact = std::vector<std::string>(table_names_.begin(), table_names_.end());
  }

  for (const auto& table_name : tables_to_compact) {
    auto status = DoCompaction(table_name);
    if (!status.ok()) {
      logger_.Warning("Compaction failed for table " + table_name + ": " + status.message());
    }
  }

  return Status::OK();
}

Status CompactionManager::DoCompaction(const std::string& table_name) {
  // Prevent concurrent compactions
  if (is_compacting_.exchange(true)) {
    return Status(DB_UNEXPECTED_ERROR, "Another compaction is in progress");
  }

  // Use a simple RAII class to ensure cleanup
  struct CompactionGuard {
    std::atomic<bool>& flag;
    ~CompactionGuard() { flag = false; }
  } guard{is_compacting_};

  // Get the table
  auto table_result = tables_.Get(table_name);
  if (!table_result.first) {
    return Status(DB_UNEXPECTED_ERROR, "Table not found: " + table_name);
  }
  std::shared_ptr<Table> table = table_result.second;

  auto start_time = std::chrono::steady_clock::now();
  ScopedCompactionTracker tracker(table_name);

  // Get current stats from the actual table
  size_t total_records = table->GetRecordCount();
  double deleted_ratio = 0.0;
  if (table->table_segment_) {
    deleted_ratio = table->table_segment_->GetDeletedRatio();
  }
  size_t initial_deleted = static_cast<size_t>(total_records * deleted_ratio);

  CompactionStats stats = GetTableStats(table_name);
  stats.total_vectors = total_records;
  stats.deleted_vectors = initial_deleted;

  logger_.Info("Starting compaction for " + table_name +
               " (deleted_ratio=" + std::to_string(stats.GetDeletedRatio() * 100) + "%)");

  // Coordinate with WAL before compaction
  auto& wal_coordinator = WALCompactionCoordinator::GetInstance();
  ScopedWALCompactionGuard wal_guard(table_name);

  // Perform actual compaction using hybrid CPU/IO worker pools
  // CPU pool: For data processing and compression
  // IO pool: For disk I/O operations
  auto& pool_manager = execution::WorkerPoolManager::GetInstance();

  try {
    // Submit compaction task to IO pool with NORMAL priority (lower than WAL)
    auto future = pool_manager.SubmitIoTaskWithPriority(
      execution::TaskPriority::NORMAL,
      [table, this]() -> Status {
        return table->Compact(this->compaction_threshold_.load());
      }
    );

    // Wait for compaction to complete
    auto compaction_status = future.get();

    if (!compaction_status.ok()) {
      logger_.Error("Table compaction failed for " + table_name + ": " + compaction_status.message());
      return compaction_status;
    }
  } catch (const std::exception& e) {
    logger_.Error("Compaction task submission failed for " + table_name + ": " + e.what());
    return Status(DB_UNEXPECTED_ERROR, "Compaction task failed: " + std::string(e.what()));
  }

  // Mark WAL coordination as successful
  // In real implementation, would get actual last WAL ID from table
  int64_t last_wal_id = table->table_segment_ ? table->table_segment_->wal_global_id_.load() : 0;
  wal_guard.SetSuccess(last_wal_id);

  // Update statistics
  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

  // Get post-compaction stats
  size_t post_records = table->GetRecordCount();
  size_t records_freed = (initial_deleted > 0) ? initial_deleted : 0;

  stats.compaction_count++;
  stats.last_compaction_time = end_time;
  stats.last_compaction_duration = duration;
  stats.total_vectors = post_records;
  stats.deleted_vectors = 0;  // Reset after compaction
  stats.memory_freed_bytes += records_freed * 1000;  // Estimate memory freed

  table_stats_.Insert(table_name, stats);

  // Update global stats
  global_stats_.compaction_count++;
  global_stats_.memory_freed_bytes += initial_deleted * 1000;

  logger_.Info("Compaction completed for " + table_name +
               " in " + std::to_string(duration.count()) + "ms, freed ~" +
               std::to_string(initial_deleted * 1000 / 1024 / 1024) + "MB");

  return Status::OK();
}

void CompactionManager::RegisterTable(const std::string& table_name, std::shared_ptr<Table> table) {
  // CRITICAL FIX: Lock table_names_mutex_ FIRST to ensure consistent lock ordering
  // This prevents deadlock with CompactionWorker which also acquires table_names_mutex_ first
  {
    std::lock_guard<std::mutex> lock(table_names_mutex_);
    table_names_.insert(table_name);
  }

  // Then update concurrent hash maps (these have internal locks)
  tables_.Insert(table_name, table);

  // Initialize stats if not exists
  CompactionStats stats;
  table_stats_.Insert(table_name, stats);

  logger_.Debug("Registered table for compaction: " + table_name);
}

void CompactionManager::UnregisterTable(const std::string& table_name) {
  // CRITICAL FIX: Lock table_names_mutex_ FIRST to maintain consistent lock ordering
  {
    std::lock_guard<std::mutex> lock(table_names_mutex_);
    table_names_.erase(table_name);
  }

  // Then update concurrent hash maps (these have internal locks)
  // Note: ConcurrentHashMap doesn't have Erase, so we'll mark as invalid
  // by inserting nullptr for tables and empty stats
  tables_.Insert(table_name, nullptr);
  table_stats_.Insert(table_name, CompactionStats());

  logger_.Debug("Unregistered table from compaction: " + table_name);
}

void CompactionManager::UpdateStats(const std::string& table_name, size_t total_vectors, size_t deleted_vectors) {
  CompactionStats stats;
  auto stats_result = table_stats_.Get(table_name);
  if (stats_result.first) {
    stats = stats_result.second;
  }

  stats.total_vectors = total_vectors;
  stats.deleted_vectors = deleted_vectors;
  table_stats_.Insert(table_name, stats);

  // Update global stats (should be total, not additive)
  // For simplicity, just track the latest values
  global_stats_.total_vectors = total_vectors;
  global_stats_.deleted_vectors = deleted_vectors;
}

bool CompactionManager::ShouldCompact(const std::string& table_name) const {
  auto stats_result = table_stats_.Get(table_name);
  if (!stats_result.first) {
    return false;
  }
  CompactionStats stats = stats_result.second;

  // Check minimum vector count
  if (stats.total_vectors < min_vectors_for_compaction_.load()) {
    return false;
  }

  // Check deletion threshold
  double deleted_ratio = stats.GetDeletedRatio();
  if (deleted_ratio >= compaction_threshold_.load()) {
    return true;
  }

  // Check memory pressure
  if (IsMemoryPressureHigh() && deleted_ratio > 0.1) {
    return true;
  }

  return false;
}

bool CompactionManager::IsLowTrafficPeriod() const {
  // Get current hour
  auto now = std::chrono::system_clock::now();
  auto time_t = std::chrono::system_clock::to_time_t(now);
  struct tm* tm_info = localtime(&time_t);
  int hour = tm_info->tm_hour;

  // Consider 2-5 AM as low traffic period
  return (hour >= 2 && hour <= 5);
}

bool CompactionManager::IsMemoryPressureHigh() const {
  // Get memory usage (simplified - in production would use actual memory metrics)
  // This is a placeholder implementation
  FILE* file = fopen("/proc/meminfo", "r");
  if (!file) {
    return false;
  }

  char line[128];
  long total_mem = 0, available_mem = 0;

  while (fgets(line, sizeof(line), file)) {
    if (sscanf(line, "MemTotal: %ld kB", &total_mem) == 1) {
      continue;
    }
    if (sscanf(line, "MemAvailable: %ld kB", &available_mem) == 1) {
      break;
    }
  }
  fclose(file);

  if (total_mem > 0 && available_mem > 0) {
    double usage_ratio = 1.0 - (static_cast<double>(available_mem) / total_mem);
    return usage_ratio > 0.8;  // Consider high pressure if >80% memory used
  }

  return false;
}

CompactionStats CompactionManager::GetGlobalStats() const {
  return global_stats_;
}

CompactionStats CompactionManager::GetTableStats(const std::string& table_name) const {
  auto stats_result = table_stats_.Get(table_name);
  if (stats_result.first) {
    return stats_result.second;
  }
  return CompactionStats();
}

Json CompactionManager::GetAllStatsAsJson() const {
  Json result;
  Json global = global_stats_.ToJson();
  result.SetObject("global", global);

  Json tables;
  // Iterate over tracked table names
  {
    std::lock_guard<std::mutex> lock(table_names_mutex_);
    for (const auto& name : table_names_) {
      auto stats_result = table_stats_.Get(name);
      if (stats_result.first) {
        Json table_stats = stats_result.second.ToJson();
        tables.SetObject(name.c_str(), table_stats);
      }
    }
  }
  result.SetObject("tables", tables);

  return result;
}

void CompactionManager::SetAutoCompaction(bool enabled) {
  auto_compaction_enabled_ = enabled;
  if (enabled && !worker_thread_) {
    Start();
  } else if (!enabled && worker_thread_) {
    Stop();
  }
}

void CompactionManager::SetCompactionThreshold(double threshold) {
  if (threshold >= 0.05 && threshold <= 0.5) {
    compaction_threshold_ = threshold;
  }
}

void CompactionManager::SetCompactionInterval(int seconds) {
  if (seconds >= 60 && seconds <= 86400) {
    compaction_interval_ = seconds;
  }
}

}  // namespace engine
}  // namespace vectordb