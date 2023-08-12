
#include "server/server.hpp"
#include "server/web_server/web_server.hpp"
#include "server/db_server/db_server.hpp"

#include <cstring>
#include <iostream>

namespace vectordb {
namespace server {

Server& Server::GetInstance() {
  static Server server;
  return server;
}

void Server::Init(const std::string& config_filename) {
  config_filename_ = config_filename;
}

Status Server::Start(uint16_t port, bool rebuild) {
  try {
    web::WebServer::GetInstance().SetPort(port);
    web::WebServer::GetInstance().SetRebuild(rebuild);
    return StartService();
  } catch (std::exception& ex) {
    std::string str = "Epsilla VectorDB server encounter exception: " + std::string(ex.what());
    return Status(INFRA_UNEXPECTED_ERROR, str);
  }
}

void Server::Stop() {
  std::cerr << "Epsilla VectorDB server is going to shutdown ..." << std::endl;

  StopService();

  std::cerr << "Epsilla VectorDB server exit..." << std::endl;
}

Status Server::LoadConfig() {
  return vectordb::Status::OK();
}

Status Server::StartService() {
  web::WebServer::GetInstance().Start();
  db::DBServer::GetInstance().Start();
  return Status::OK();
}

void Server::StopService() {
}

}  // namespace server
}  // namespace vectordb
