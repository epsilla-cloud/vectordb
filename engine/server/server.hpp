#pragma once

#include <string>
#include "utils/status.hpp"

namespace vectordb {
namespace server {

class Server {
 public:
  static Server& GetInstance();

  void Init(const std::string& config_filename);

  Status Start(uint16_t port, bool rebuild, bool is_leader, std::string &embedding_service_url);
  void Stop();

 private:
  Server() = default;
  ~Server() = default;

  Status LoadConfig();

  Status StartService();
  void StopService();

 private:
  std::string config_filename_;
};  // Server

}  // namespace server
}  // namespace vectordb
