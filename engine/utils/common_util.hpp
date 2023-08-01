#pragma once

#include <time.h>
#include <string>

#include "utils/status.hpp"

namespace vectordb {
namespace server {

class CommonUtil {
 public:
  // static bool GetSystemMemInfo(int64_t& total_mem, int64_t& free_mem);
  static bool GetSysCgroupMemLimit(int64_t& limit_in_bytes);
  static bool GetSystemAvailableThreads(int64_t& thread_count);

  static bool IsFileExist(const std::string& path);
  static uint64_t GetFileSize(const std::string& path);
  static bool IsDirectoryExist(const std::string& path);
  static Status CreateDirectory(const std::string& path);
  static Status DeleteDirectory(const std::string& path);
  static Status RemoveFile(const std::string& path);

  static std::string GetFileName(const std::string& filename);
  static std::string GetExePath();
  static std::string ReadContentFromFile(const std::string& file_path);
  static Status AtomicWriteToFile(const std::string& path, const std::string& content);

  static bool TimeStrToTime(const std::string& time_str, time_t& time_integer, tm& time_struct,
                            const std::string& format = "%d-%d-%d %d:%d:%d");

  static void ConvertTime(time_t time_integer, tm& time_struct);
  static void ConvertTime(tm time_struct, time_t& time_integer);

  static bool IsValidName(const std::string& name);
};

}  // namespace server
}  // namespace vectordb
