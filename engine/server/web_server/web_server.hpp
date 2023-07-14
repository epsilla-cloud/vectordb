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

  private:
    WebServer() {
        try_stop_.store(false);
    }

    ~WebServer() = default;

    Status
    StartWebServer();

    Status
    StopWebServer();

  public:
    static WebServer&
    GetInstance() {
      static WebServer web_server;
      return web_server;
    }

    void
    Start();

    void
    Stop();
  };
}
}
}
