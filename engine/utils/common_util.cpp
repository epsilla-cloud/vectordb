#include "utils/common_util.hpp"
// TODO: to be implemented
// #include "utils/log.hpp"
#include <dirent.h>
#include <pwd.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <cstdio>
#include <thread>
#include <vector>

#if defined(__x86_64__)
#define THREAD_MULTIPLY_CPU 1
#elif defined(__powerpc64__)
#define THREAD_MULTIPLY_CPU 4
#else
#define THREAD_MULTIPLY_CPU 1
#endif

namespace vectordb {
namespace server {

// bool CommonUtil::GetSystemMemInfo(int64_t& total_mem, int64_t& free_mem) {
//   struct sysinfo info;
//   int ret = sysinfo(&info);
//   total_mem = info.totalram;
//   free_mem = info.freeram;

//   return ret == 0;  // succeed 0, failed -1
// }

bool CommonUtil::GetSysCgroupMemLimit(int64_t& limit_in_bytes) {
  try {
    std::ifstream file("/sys/fs/cgroup/memory/memory.limit_in_bytes");
    file >> limit_in_bytes;
    return true;
  } catch (std::exception& ex) {
    std::string msg =
        "Failed to read /sys/fs/cgroup/memory/memory.limit_in_bytes, reason: " + std::string(ex.what());
    // LOG_SERVER_ERROR_ << msg;
    return false;
  }
}

bool CommonUtil::GetSystemAvailableThreads(int64_t& thread_count) {
  // threadCnt = std::thread::hardware_concurrency();
  thread_count = sysconf(_SC_NPROCESSORS_CONF);
  thread_count *= THREAD_MULTIPLY_CPU;
  // fiu_do_on("CommonUtil.GetSystemAvailableThreads.zero_thread", thread_count = 0);

  if (thread_count == 0) {
    thread_count = 8;
  }

  return true;
}

bool CommonUtil::IsDirectoryExist(const std::string& path) {
  DIR* dp = nullptr;
  if ((dp = opendir(path.c_str())) == nullptr) {
    return false;
  }

  closedir(dp);
  return true;
}

Status CommonUtil::CreateDirectory(const std::string& path) {
  if (path.empty()) {
    return Status::OK();
  }

  struct stat directory_stat;
  int status = stat(path.c_str(), &directory_stat);
  if (status == 0) {
    return Status::OK();  // already exist
  }

  size_t separator_pos = path.find_last_of('/');  // get the parent directory path
  if (separator_pos != std::string::npos) {
    std::string parent_path = path.substr(0, separator_pos);
    if (!parent_path.empty()) {
      Status err_status = CreateDirectory(parent_path);
      if (!err_status.ok()) {
        return err_status;
      }
    }
  }

  status = stat(path.c_str(), &directory_stat);
  if (status == 0) {
    return Status::OK();  // already exist
  }

  int makeOK = mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IROTH);
  if (makeOK != 0) {
    return Status(INFRA_UNEXPECTED_ERROR, "failed to create directory: " + path);
  }

  return Status::OK();
}

namespace {
void RemoveDirectory(const std::string& path) {
  DIR* dir = nullptr;
  struct dirent* dmsg;
  const int32_t buf_size = 256;
  char file_name[buf_size];

  std::string folder_name = path + "/%s";
  if ((dir = opendir(path.c_str())) != nullptr) {
    while ((dmsg = readdir(dir)) != nullptr) {
      if (strcmp(dmsg->d_name, ".") != 0 && strcmp(dmsg->d_name, "..") != 0) {
        snprintf(file_name, buf_size, folder_name.c_str(), dmsg->d_name);
        std::string tmp = file_name;
        if (tmp.find(".") == std::string::npos) {
          RemoveDirectory(file_name);
        }
        remove(file_name);
      }
    }
  }

  if (dir != nullptr) {
    closedir(dir);
  }
  remove(path.c_str());
}
}  // namespace

Status CommonUtil::DeleteDirectory(const std::string& path) {
  if (path.empty()) {
    return Status::OK();
  }

  struct stat directory_stat;
  int statOK = stat(path.c_str(), &directory_stat);
  if (statOK != 0) {
    return Status::OK();
  }

  RemoveDirectory(path);
  return Status::OK();
}

bool CommonUtil::IsFileExist(const std::string& path) {
  return (access(path.c_str(), F_OK) == 0);
}

uint64_t CommonUtil::GetFileSize(const std::string& path) {
  struct stat file_info;
  if (stat(path.c_str(), &file_info) < 0) {
    return 0;
  }

  return static_cast<uint64_t>(file_info.st_size);
}

std::string CommonUtil::GetFileName(const std::string& filename) {
  int pos = filename.find_last_of('/');
  return filename.substr(pos + 1);
}

std::string CommonUtil::GetExePath() {
  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t cnt = readlink("/proc/self/exe", buf, buf_len);
  // fiu_do_on("CommonUtil.GetExePath.readlink_fail", cnt = -1);
  if (cnt < 0 || cnt >= buf_len) {
    return "";
  }

  buf[cnt] = '\0';

  std::string exe_path = buf;
  // fiu_do_on("CommonUtil.GetExePath.exe_path_error", exe_path = "/");
  if (exe_path.rfind('/') != exe_path.length() - 1) {
    std::string sub_str = exe_path.substr(0, exe_path.rfind('/'));
    return sub_str + "/";
  }
  return exe_path;
}

std::string CommonUtil::ReadContentFromFile(const std::string& file_path) {
  std::ifstream file(file_path);
  if (!file.is_open()) {
    // LOG_SERVER_ERROR_ << "Failed to open file: " << file_path;
    return "";
  }
  std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  file.close();
  return content;
}

// Status CommonUtil::AtomicWriteToFile(const std::string& path, const std::string& content) {
//   std::string temp_path = path + ".tmp";
//   std::ofstream ofs(temp_path);
//   if (!ofs.is_open()) {
//     // LOG_SERVER_ERROR_ << "Failed to open temp file: " << temp_path;
//     return Status(INFRA_UNEXPECTED_ERROR, "Failed to open temp file: " + temp_path);
//   }

//   ofs << content;
//   ofs.close();

//   if (std::rename(temp_path.c_str(), path.c_str()) != 0) {
//     // LOG_SERVER_ERROR_ << "Failed to rename temp file: " << temp_path << " to " << path;
//     return Status(INFRA_UNEXPECTED_ERROR, "Failed to rename temp file: " + temp_path + " to " + path);
//   }

//   return Status::OK();
// }

Status CommonUtil::AtomicWriteToFile(const std::string& path, const std::string& content) {
  std::string temp_path = path + ".tmp";
  FILE* file = fopen(temp_path.c_str(), "w");
  if (file == nullptr) {
    // LOG_SERVER_ERROR_ << "Failed to open temp file: " << temp_path;
    return Status(INFRA_UNEXPECTED_ERROR, "Failed to open temp file: " + temp_path);
  }

  size_t num_written = fwrite(content.c_str(), 1, content.size(), file);
  if (num_written != content.size()) {
    // TODO: Handle error...
  }

  fflush(file);  // Ensures the data is written to OS buffers
  fsync(fileno(file));  // Ensures the data is written to the disk

  fclose(file);

  if (std::rename(temp_path.c_str(), path.c_str()) != 0) {
    // LOG_SERVER_ERROR_ << "Failed to rename temp file: " << temp_path << " to " << path;
    return Status(INFRA_UNEXPECTED_ERROR, "Failed to rename temp file: " + temp_path + " to " + path);
  }

  return Status::OK();
}

bool CommonUtil::TimeStrToTime(const std::string& time_str, time_t& time_integer, tm& time_struct,
                               const std::string& format) {
  time_integer = 0;
  memset(&time_struct, 0, sizeof(tm));

  int ret = sscanf(time_str.c_str(), format.c_str(), &(time_struct.tm_year), &(time_struct.tm_mon),
                   &(time_struct.tm_mday), &(time_struct.tm_hour), &(time_struct.tm_min), &(time_struct.tm_sec));
  if (ret <= 0) {
    return false;
  }

  time_struct.tm_year -= 1900;
  time_struct.tm_mon--;
  time_integer = mktime(&time_struct);

  return true;
}

void CommonUtil::ConvertTime(time_t time_integer, tm& time_struct) {
  localtime_r(&time_integer, &time_struct);
}

void CommonUtil::ConvertTime(tm time_struct, time_t& time_integer) {
  time_integer = mktime(&time_struct);
}

}  // namespace server
}  // namespace vectordb
