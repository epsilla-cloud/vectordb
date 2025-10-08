#pragma once

#include <omp.h>
#include <unistd.h>
#include <unordered_map>

#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>

#include "db/catalog/meta_types.hpp"
#include "db/table_segment.hpp"
#include "utils/atomic_counter.hpp"
#include "utils/common_util.hpp"
#include "utils/json.hpp"

#include "query/expr/expr_evaluator.hpp"
#include "query/expr/expr_types.hpp"
#include "query/expr/expr.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {
// const std::chrono::minutes ROTATION_INTERVAL(10);
// const std::chrono::days LOG_RETENTION(7);
const std::chrono::seconds ROTATION_INTERVAL(600);
const std::chrono::seconds LOG_RETENTION(3600 * 24 * 7);

enum LogEntryType {
  INSERT = 1,
  DELETE = 2,
  UPSERT = 3,
};

class WriteAheadLog {
 public:
  WriteAheadLog(std::string base_path, int64_t table_id, bool is_leader)
      : logs_folder_(base_path + "/" + std::to_string(table_id) + "/wal/"),
        last_rotation_time_(std::chrono::system_clock::now()),
        is_leader_(is_leader) {
    // Load the last ID from the disk
    std::ifstream id_file(logs_folder_ + "/last_id.txt");
    if (id_file.is_open()) {
      int64_t last_global_id;
      id_file >> last_global_id;
      global_counter_.SetValue(last_global_id);
      id_file.close();
    }
    if (is_leader_) {
      auto mkdir_status = server::CommonUtil::CreateDirectory(logs_folder_);
      if (!mkdir_status.ok()) {
        throw mkdir_status.message();
      }
      RotateFile();
    }
  }

  ~WriteAheadLog() {
    if (file_ != nullptr) {
      fclose(file_);
    }
    if (is_leader_) {
      // Save the last ID to the disk
      std::ofstream id_file(logs_folder_ + "/last_id.txt");
      id_file << global_counter_.Get();
      id_file.close();
    }
  }

  int64_t WriteEntry(LogEntryType type, const std::string &entry) {
    // Skip WAL for realtime scenario
    if (!enabled_ || !is_leader_) {
      return global_counter_.Get();
    }

    auto now = std::chrono::system_clock::now();
    if (now - last_rotation_time_ > ROTATION_INTERVAL) {
      RotateFile();
    }

    // CRITICAL FIX: Increment counter AFTER successful write, not before
    // This prevents ID holes when fsync fails
    int64_t next = global_counter_.Get() + 1;

#ifdef __APPLE__
    fprintf(file_, "%lld %d %s\n", next, type, entry.c_str());
#else
    fprintf(file_, "%ld %d %s\n", next, type, entry.c_str());
#endif
    fflush(file_);

    // CRITICAL FIX: Ensure data is written to disk BEFORE incrementing counter
    // This guarantees durability and prevents data loss
    if (fsync(fileno(file_)) != 0) {
      std::string error_msg = "Critical: WAL fsync failed: " + std::string(strerror(errno));
      logger_.Error(error_msg);
      // For critical operations, throw exception to prevent data loss
      // Counter is NOT incremented, so retry will use the same ID
      throw std::runtime_error(error_msg);
    }

    // Only increment counter after successful fsync
    global_counter_.IncrementAndGet();
    return next;
  }

  void Replay(
    meta::TableSchema &table_schema,
    std::unordered_map<std::string, meta::FieldType>& field_name_type_map,
    std::shared_ptr<TableSegment> segment,
    std::unordered_map<std::string, std::string> &headers
  ) {
    std::vector<std::filesystem::path> files;
    GetSortedLogFiles(files);
    for (auto pt = 0; pt < files.size(); ++pt) {
      auto file = files[pt];
      bool update = false;
      std::ifstream in(file.string());
      std::string line;
      while (std::getline(in, line)) {
        // Entry ID
        size_t first_space = line.find(' ');
        int64_t global_id = std::stoll(line.substr(0, first_space));
        if (global_counter_.Get() < global_id) {
          global_counter_.SetValue(global_id);
        }
        // If the entry ID is less than or equal to the consumed ID, ignore it
        if (global_id <= segment->wal_global_id_) {
          continue;
        }
        update = true;
        // Entry type
        size_t second_space = line.find(' ', first_space + 1);
        LogEntryType type = static_cast<LogEntryType>(std::stoi(line.substr(first_space + 1, second_space - first_space - 1)));
        // Entry content
        std::string content = line.substr(second_space + 1);

        // Otherwise, replay the entry
        ApplyEntry(table_schema, field_name_type_map, segment, global_id, type, content, headers);
      }
      // Close the file.
      in.close();
      if (is_leader_) {
        // Delete the file if the whole file is already in table.
        if (!update && pt < files.size() - 1) {
          server::CommonUtil::RemoveFile(file.string());
        }
      }
    }
    if (is_leader_) {
      // Save the last ID to the disk
      std::ofstream id_file(logs_folder_ + "/last_id.txt");
      id_file << global_counter_.Get();
      id_file.close();
    }
  }

  void CleanUpOldFiles() {
    // Get the current time
    auto now = std::chrono::system_clock::now();
    // Convert LOG_RETENTION to seconds for comparison
    auto retention_period_seconds = std::chrono::duration_cast<std::chrono::seconds>(LOG_RETENTION).count();

    // Get all log files
    std::vector<std::filesystem::path> files;
    GetSortedLogFiles(files);

    for (const auto &file : files) {
      // Extract the timestamp from the filename
      auto filename = file.filename().string();
      auto pos = filename.find_last_of('.');
      auto timestamp_str = filename.substr(0, pos);
      auto timestamp = std::stoll(timestamp_str);

      // Convert now to seconds since epoch
      auto now_in_seconds = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

      // If the file is older than LOG_RETENTION, delete it
      if (now_in_seconds - timestamp > retention_period_seconds) {
        server::CommonUtil::RemoveFile(file.string());
      } else {
        // Since the files are sorted, we can break as soon as we encounter a file that's not old enough to be deleted
        break;
      }
    }
  }

  void SetEnabled(bool enabled) {
    enabled_ = enabled;
  }

  void Flush() {
    if (file_ != nullptr) {
      fflush(file_);
      // Force sync to disk - critical for data safety
      if (fsync(fileno(file_)) != 0) {
        logger_.Error("Critical: WAL fsync failed during flush: " + std::string(strerror(errno)));
        throw std::runtime_error("WAL flush failed: " + std::string(strerror(errno)));
      }
    }
  }

  void SetLeader(bool is_leader) {
    is_leader_ = is_leader;
    if (is_leader) {
      RotateFile();
    }
  }

 private:
  vectordb::engine::Logger logger_;
  void ApplyEntry(
    meta::TableSchema &table_schema,
    std::unordered_map<std::string, meta::FieldType>& field_name_type_map,
    std::shared_ptr<TableSegment> segment,
    int64_t global_id,
    LogEntryType &type,
    std::string &content,
    std::unordered_map<std::string, std::string> &headers
  ) {
    vectordb::Json record;
    record.LoadFromString(content);
    switch (type) {
      case LogEntryType::INSERT: {
        auto status = segment->Insert(table_schema, record, global_id, headers);
        if (!status.ok()) {
          logger_.Error("Fail to apply wal entry: " + status.message());
        }
        break;
      }
      case LogEntryType::UPSERT: {
        auto status = segment->Insert(table_schema, record, global_id, headers, true);
        if (!status.ok()) {
          logger_.Error("Fail to apply wal entry: " + status.message());
        }
        break;
      }
      case LogEntryType::DELETE: {
        vectordb::Json pks;
        std::string filter;
        if (content[0] == '{') {
          // New version: supports filter
          pks = record.GetArray("pk");
          filter = record.GetString("filter");
        } else {
          // Old version: supports only pk list.
          pks = record;
        }
        std::vector<query::expr::ExprNodePtr> expr_nodes;
        vectordb::query::expr::Expr::ParseNodeFromStr(filter, expr_nodes, field_name_type_map);
  
        auto status = segment->Delete(pks, expr_nodes, global_id);
        if (!status.ok()) {
          logger_.Error("Fail to apply wal entry: " + status.message());
        }
        break;
      }
      default: {
        break;
      }
    }
  }

  // void GetSortedLogFiles(std::vector<std::filesystem::path>& files) {
  //   std::cout << "Get shorted log files by thread: " << omp_get_thread_num() << std::endl;

  //   std::filesystem::directory_iterator end_itr;  // Default ctor yields past-the-end
  //   for (std::filesystem::directory_iterator i(logs_folder_); i != end_itr; ++i) {
  //     if (i->path().extension() == ".log") {
  //       files.push_back(i->path());
  //     }
  //   }
  //   std::sort(files.begin(), files.end());
  // }

  void GetSortedLogFiles(std::vector<std::filesystem::path> &files) {
    // Check if logs_folder_ exists and is a directory.
    if (!std::filesystem::exists(logs_folder_) || !std::filesystem::is_directory(logs_folder_)) {
      logger_.Error("Directory " + logs_folder_ + " does not exist or is not a directory.");
      return;
    }

    try {
      std::filesystem::directory_iterator end_itr;  // Default ctor yields past-the-end
      for (std::filesystem::directory_iterator i(logs_folder_); i != end_itr; ++i) {
        // Print out the path of each file being processed.
        logger_.Info("Processing file: " + i->path().string());

        if (i->path().extension() == ".log") {
          files.push_back(i->path());
        }
      }
    } catch (const std::filesystem::filesystem_error &ex) {
      logger_.Error(std::string("Error in getting sorted WAL log files: ") + ex.what());
    }

    std::sort(files.begin(), files.end());
  }

  void RotateFile() {
    if (file_ != nullptr) {
      fclose(file_);
    }

    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    std::string filename = logs_folder_ + std::to_string(time) + ".log";
    file_ = fopen(filename.c_str(), "a");

    last_rotation_time_ = now;
  }

  std::string logs_folder_;
  std::chrono::time_point<std::chrono::system_clock> last_rotation_time_;
  FILE *file_ = nullptr;
  AtomicCounter global_counter_;
  bool enabled_ = true;
  std::atomic<bool> is_leader_;
};
}  // namespace engine
}  // namespace vectordb
