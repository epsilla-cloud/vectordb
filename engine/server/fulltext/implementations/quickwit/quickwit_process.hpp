#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace server {
namespace fulltext {
namespace quickwit {

using vectordb::engine::Logger;

/**
 * @brief QuickwitProcess - 管理 Quickwit 子进程的生命周期
 *
 * 功能：
 * - 启动/停止 Quickwit 二进制文件作为子进程
 * - 健康检查和自动重启
 * - 提供 Quickwit endpoint URL
 *
 * 注意：这是 Quickwit 特定的实现，不对外暴露
 */
class QuickwitProcess {
public:
  QuickwitProcess() = default;
  ~QuickwitProcess();

  /**
   * @brief 启动 Quickwit 子进程
   * @param binary_path Quickwit 二进制文件路径
   * @param config_path Quickwit 配置文件路径
   * @param data_dir 数据目录
   * @param port HTTP 端口（默认 7280）
   * @return Status
   */
  Status Start(const std::string& binary_path,
               const std::string& config_path,
               const std::string& data_dir,
               int port = 7280);

  /**
   * @brief 停止 Quickwit 子进程
   * @return Status
   */
  Status Stop();

  /**
   * @brief 检查 Quickwit 是否正在运行
   * @return bool
   */
  bool IsRunning() const {
    return running_.load(std::memory_order_acquire);
  }

  /**
   * @brief 获取 Quickwit HTTP endpoint
   * @return std::string (e.g., "http://localhost:7280")
   */
  std::string GetEndpoint() const {
    return "http://localhost:" + std::to_string(port_);
  }

  /**
   * @brief 获取进程 ID
   * @return pid_t
   */
  pid_t GetPid() const {
    return pid_;
  }

  /**
   * @brief 等待 Quickwit 启动并就绪
   * @param timeout_secs 超时秒数
   * @return bool 是否就绪
   */
  bool WaitForReady(int timeout_secs = 30);

  /**
   * @brief 重启 Quickwit
   * @return Status
   */
  Status Restart();

private:
  /**
   * @brief 健康检查循环（后台线程）
   */
  void HealthCheckLoop();

  /**
   * @brief 执行一次健康检查
   * @return bool 是否健康
   */
  bool PerformHealthCheck();

  /**
   * @brief 启动健康检查线程
   */
  void StartHealthCheckThread();

  /**
   * @brief 停止健康检查线程
   */
  void StopHealthCheckThread();

  // Quickwit 进程信息
  pid_t pid_ = -1;
  std::atomic<bool> running_{false};

  // 配置
  std::string binary_path_;
  std::string config_path_;
  std::string data_dir_;
  int port_ = 7280;

  // 健康检查
  std::thread health_check_thread_;
  std::atomic<bool> health_check_running_{false};
  std::atomic<int> failed_health_checks_{0};

  // 自动重启配置
  bool auto_restart_enabled_ = true;
  int max_restart_attempts_ = 3;
  int restart_attempts_ = 0;

  // 日志
  Logger logger_;
};

} // namespace quickwit
} // namespace fulltext
} // namespace server
} // namespace vectordb
