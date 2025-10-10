#include "server/fulltext/implementations/quickwit/quickwit_process.hpp"

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>
#include <chrono>
#include <thread>
#include <cstdlib>

namespace vectordb {
namespace server {
namespace fulltext {
namespace quickwit {

QuickwitProcess::~QuickwitProcess() {
  if (running_.load()) {
    Stop();
  }
}

Status QuickwitProcess::Start(const std::string& binary_path,
                                const std::string& config_path,
                                const std::string& data_dir,
                                int port) {
  if (running_.load()) {
    return Status(DB_ALREADY_EXIST, "Quickwit is already running");
  }

  binary_path_ = binary_path;
  config_path_ = config_path;
  data_dir_ = data_dir;
  port_ = port;

  // 检查二进制文件是否存在
  if (access(binary_path.c_str(), X_OK) != 0) {
    std::string error_msg =
      "Failed to start Quickwit full-text search engine\n"
      "Reason: Binary not found or not executable\n"
      "Path: " + binary_path + "\n"
      "Troubleshooting:\n"
      "  1. Check if binary exists: ls -l " + binary_path + "\n"
      "  2. Check permissions: chmod +x " + binary_path + "\n"
      "  3. Install Quickwit: https://quickwit.io/docs/get-started/installation\n"
      "  4. Set correct path: export VECTORDB_QUICKWIT_BINARY=/path/to/quickwit";
    logger_.Error(error_msg);
    return Status(DB_UNEXPECTED_ERROR, error_msg);
  }

  logger_.Info("[Quickwit] Starting subprocess [binary=" + binary_path +
               ", config=" + config_path + ", data_dir=" + data_dir +
               ", port=" + std::to_string(port) + "]");

  // Fork 子进程
  pid_ = fork();

  if (pid_ < 0) {
    return Status(DB_UNEXPECTED_ERROR, "Failed to fork process");
  }

  if (pid_ == 0) {
    // 子进程：执行 Quickwit

    // 设置 QW_DATA_DIR 环境变量，让 Quickwit 进程继承
    if (!data_dir.empty()) {
      setenv("QW_DATA_DIR", data_dir.c_str(), 1);  // 1 = overwrite if exists
    }

    // 构造命令行参数
    std::string cmd = binary_path + " run";

    if (!config_path.empty()) {
      cmd += " --config " + config_path;
    }

    // 使用 execl 替换当前进程
    execl("/bin/sh", "sh", "-c", cmd.c_str(), (char*)nullptr);

    // 如果 execl 返回，说明失败了
    exit(1);
  }

  // 父进程：等待 Quickwit 启动
  logger_.Info("[Quickwit] Subprocess started [pid=" + std::to_string(pid_) + "]");

  running_ = true;
  restart_attempts_ = 0;

  // 等待就绪
  if (!WaitForReady(30)) {
    logger_.Error("[Quickwit] Failed to become ready [timeout=30s]");
    Stop();
    return Status(DB_UNEXPECTED_ERROR, "Quickwit startup timeout");
  }

  logger_.Info("[Quickwit] Engine ready [endpoint=" + GetEndpoint() + "]");

  // 启动健康检查线程
  StartHealthCheckThread();

  return Status::OK();
}

Status QuickwitProcess::Stop() {
  if (!running_.load()) {
    return Status::OK();
  }

  logger_.Info("[Quickwit] Stopping subprocess [pid=" + std::to_string(pid_) + "]");

  // 停止健康检查
  StopHealthCheckThread();

  // 发送 SIGTERM 优雅关闭
  if (pid_ > 0) {
    kill(pid_, SIGTERM);

    // 等待最多 10 秒
    int wait_secs = 0;
    while (wait_secs < 10) {
      int status;
      pid_t result = waitpid(pid_, &status, WNOHANG);

      if (result == pid_) {
        // 进程已退出
        logger_.Info("[Quickwit] Stopped gracefully");
        running_ = false;
        pid_ = -1;
        return Status::OK();
      }

      std::this_thread::sleep_for(std::chrono::seconds(1));
      wait_secs++;
    }

    // 强制杀死
    logger_.Warning("[Quickwit] Did not stop gracefully, sending SIGKILL");
    kill(pid_, SIGKILL);
    waitpid(pid_, nullptr, 0);
  }

  running_ = false;
  pid_ = -1;

  logger_.Info("[Quickwit] Stopped");
  return Status::OK();
}

bool QuickwitProcess::WaitForReady(int timeout_secs) {
  auto start = std::chrono::steady_clock::now();

  while (true) {
    // 检查进程是否还活着
    int status;
    pid_t result = waitpid(pid_, &status, WNOHANG);

    if (result == pid_) {
      // 进程已退出
      std::string error_msg =
        "Failed to start Quickwit full-text search engine\n"
        "Reason: Process exited unexpectedly during startup\n"
        "Configuration:\n"
        "  Binary: " + binary_path_ + "\n"
        "  Data Dir: " + data_dir_ + "\n"
        "  Port: " + std::to_string(port_) + "\n"
        "Troubleshooting:\n"
        "  1. Check data directory permissions:\n"
        "     ls -ld " + data_dir_ + "\n"
        "     mkdir -p " + data_dir_ + " && chmod 755 " + data_dir_ + "\n"
        "  2. Check port availability:\n"
        "     netstat -tuln | grep " + std::to_string(port_) + "\n"
        "     lsof -i :" + std::to_string(port_) + "\n"
        "  3. Check Quickwit logs:\n"
        "     " + binary_path_ + " --version\n"
        "     tail -f " + data_dir_ + "/*.log\n"
        "  4. Test Quickwit manually:\n"
        "     QW_DATA_DIR=" + data_dir_ + " " + binary_path_ + " run";
      logger_.Error(error_msg);
      running_ = false;
      return false;
    }

    // 尝试健康检查
    if (PerformHealthCheck()) {
      logger_.Info("[Quickwit] Health check passed");
      return true;
    }

    // 检查超时
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - start).count();

    if (elapsed >= timeout_secs) {
      std::string error_msg =
        "Failed to start Quickwit full-text search engine\n"
        "Reason: Startup timeout (process running but not responding)\n"
        "Timeout: " + std::to_string(timeout_secs) + " seconds\n"
        "Configuration:\n"
        "  Binary: " + binary_path_ + "\n"
        "  Data Dir: " + data_dir_ + "\n"
        "  Port: " + std::to_string(port_) + "\n"
        "Troubleshooting:\n"
        "  1. Check if process is running:\n"
        "     ps aux | grep quickwit\n"
        "  2. Check port connectivity:\n"
        "     curl -v http://127.0.0.1:" + std::to_string(port_) + "/health\n"
        "  3. Check system resources:\n"
        "     free -h  # Available memory\n"
        "     df -h " + data_dir_ + "  # Disk space\n"
        "  4. Increase timeout (if needed):\n"
        "     This may indicate slow system or heavy load";
      logger_.Error(error_msg);
      return false;
    }

    // 等待后重试
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
}

bool QuickwitProcess::PerformHealthCheck() {
  // 简单的健康检查：尝试连接到 Quickwit 端口
  // 这里我们使用一个简单的 TCP 连接测试
  // 实际生产环境应该调用 Quickwit 的 health endpoint

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return false;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port_);
  server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

  // 设置连接超时
  struct timeval timeout;
  timeout.tv_sec = 1;
  timeout.tv_usec = 0;
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  int result = connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr));
  close(sock);

  return result == 0;
}

void QuickwitProcess::StartHealthCheckThread() {
  health_check_running_ = true;
  health_check_thread_ = std::thread([this]() {
    HealthCheckLoop();
  });
}

void QuickwitProcess::StopHealthCheckThread() {
  health_check_running_ = false;
  if (health_check_thread_.joinable()) {
    health_check_thread_.join();
  }
}

void QuickwitProcess::HealthCheckLoop() {
  logger_.Debug("[Quickwit] Health check thread started");

  while (health_check_running_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(5));

    if (!running_.load()) {
      break;
    }

    // 执行健康检查
    bool healthy = PerformHealthCheck();

    if (!healthy) {
      failed_health_checks_++;
      logger_.Warning("[Quickwit] Health check failed [attempt=" +
                     std::to_string(failed_health_checks_.load()) + "]");

      // 连续失败 3 次且启用自动重启
      if (failed_health_checks_.load() >= 3 && auto_restart_enabled_) {
        logger_.Error("[Quickwit] Process appears to be down, attempting restart");

        // 尝试重启
        if (restart_attempts_ < max_restart_attempts_) {
          restart_attempts_++;
          Restart();
        } else {
          logger_.Error("[Quickwit] Max restart attempts reached [max=" +
                       std::to_string(max_restart_attempts_) + "]");
          health_check_running_ = false;
        }
      }
    } else {
      // 健康检查通过，重置失败计数
      if (failed_health_checks_.load() > 0) {
        logger_.Info("[Quickwit] Recovered");
        failed_health_checks_ = 0;
      }
    }
  }

  logger_.Debug("[Quickwit] Health check thread stopped");
}

Status QuickwitProcess::Restart() {
  logger_.Info("[Quickwit] Restarting process");

  auto stop_status = Stop();
  if (!stop_status.ok()) {
    return stop_status;
  }

  // 等待一会儿再启动
  std::this_thread::sleep_for(std::chrono::seconds(2));

  auto start_status = Start(binary_path_, config_path_, data_dir_, port_);
  if (start_status.ok()) {
    logger_.Info("[Quickwit] Restarted successfully");
  }

  return start_status;
}

} // namespace quickwit
} // namespace fulltext
} // namespace server
} // namespace vectordb
