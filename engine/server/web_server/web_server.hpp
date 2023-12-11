#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "server/web_server/web_component.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace server {
namespace web {

class WebServer {
 private:
  std::atomic_bool try_stop_;
  std::shared_ptr<std::thread> thread_ptr_;
  uint16_t port_;
  bool rebuild_;
  std::string embedding_service_url_;
  std::atomic<bool> is_leader_;

 private:
  WebServer() {
    try_stop_.store(false);
  }

  ~WebServer() = default;

  Status StartService();
  Status StopService();

 public:
  static WebServer& GetInstance() {
    static WebServer web_server;
    return web_server;
  }

  void Start();

  void Stop();

  void SetPort(uint16_t port) {
    port_ = port;
  }

  void SetRebuild(bool rebuild) {
    rebuild_ = rebuild;
  }

  void SetLeader(bool is_leader) {
    is_leader_ = is_leader;
  }

  void SetEmbeddingServiceUrl(const std::string& url) {
    embedding_service_url_ = url;
  }
};
}  // namespace web
}  // namespace server
}  // namespace vectordb
