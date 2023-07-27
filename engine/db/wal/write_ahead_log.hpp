#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <unistd.h>
#include <boost/filesystem.hpp>
#include "db/table_segment_mvp.hpp"
#include "utils/json.hpp"
#include "utils/common_util.hpp"
#include "db/catalog/meta_types.hpp"

namespace vectordb {
namespace engine {
const std::chrono::minutes ROTATION_INTERVAL(1);

enum LogEntryType {
  INSERT = 1,
  DELETE = 2
};

class WriteAheadLog {
 public:
  WriteAheadLog(std::string base_path, int64_t table_id)
      : logs_folder_(base_path + "/" + std::to_string(table_id) + "/wal/"),
        last_rotation_time_(std::chrono::system_clock::now()) {
    global_counter_.SetValue(0);
    server::CommonUtil::CreateDirectory(logs_folder_);
    RotateFile();
  }

  ~WriteAheadLog() {
    if (file_ != nullptr) {
      fclose(file_);
    }
  }

  int64_t WriteEntry(LogEntryType type, const std::string& entry) {
    auto now = std::chrono::system_clock::now();
    if (now - last_rotation_time_ > ROTATION_INTERVAL) {
      RotateFile();
    }
    int64_t next = global_counter_.IncrementAndGet();
    fprintf(file_, "%lld %d %s\n", next, type, entry.c_str());
    fflush(file_);
    // Tradeoff of data consistency. We use fflush for now.
    // fsync(fileno(file_));
    return next;
  }

  void Replay(meta::TableSchema& table_schema, std::shared_ptr<TableSegmentMVP> segment) {
    std::vector<boost::filesystem::path> files;
    GetSortedLogFiles(files);
    for (const auto& file : files) {
      std::ifstream in(file.string());
      std::string line;
      while (std::getline(in, line)) {
        // Entry ID
        size_t first_space = line.find(' ');
        int64_t global_id = std::stoll(line.substr(0, first_space));
        // If the entry ID is less than or equal to the consumed ID, ignore it
        if (global_id <= segment->wal_global_id_) {
          continue;
        }

        // Entry type
        size_t second_space = line.find(' ', first_space + 1);
        LogEntryType type = static_cast<LogEntryType>(std::stoi(line.substr(first_space + 1, second_space - first_space - 1)));
        // Entry content
        std::string content = line.substr(second_space + 1);

        // Otherwise, replay the entry
        ApplyEntry(table_schema, segment, global_id, type, content);

        global_counter_.SetValue(global_id);
      }
      // Close the file.
      in.close();
    }
  }

 private:
  void ApplyEntry(meta::TableSchema& table_schema, std::shared_ptr<TableSegmentMVP> segment, int64_t global_id, LogEntryType& type, std::string& content) {
    switch (type) {
      case LogEntryType::INSERT: {
        Json record;
        record.LoadFromString(content);
        auto status = segment->Insert(table_schema, record, global_id);
        if (!status.ok()) {
          std::cout << "Fail to apply wal entry." << std::endl;
        } else {
          std::cout << "Applied " << global_id << std::endl;
        }
        break;
      }
      case LogEntryType::DELETE: {
        // TODO: to be implemented.
        break;
      }
      default: {
        break;
      }
    }
  }

  void GetSortedLogFiles(std::vector<boost::filesystem::path>& files) {
    boost::filesystem::directory_iterator end_itr; // Default ctor yields past-the-end
    for (boost::filesystem::directory_iterator i(logs_folder_); i != end_itr; ++i) {
      if (i->path().extension() == ".log") {
        files.push_back(i->path());
      }
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
  FILE* file_ = nullptr;
  AtomicCounter global_counter_;
};
}  // namespace engine
}  // namespace vectordb
