#include "web_server.hpp"

#include <chrono>
#include <oatpp/network/Server.hpp>

#include "request_interceptor.hpp"
#include "web_controller.hpp"

#include "services/embedding_service.hpp"

namespace vectordb {
namespace server {
namespace web {

void WebServer::Start() {
  bool enable = true;
  if (enable && nullptr == thread_ptr_) {
    thread_ptr_ = std::make_shared<std::thread>(&WebServer::StartService, this);
  }
}

void WebServer::Stop() {
  StopService();

  if (thread_ptr_ != nullptr) {
    thread_ptr_->join();
    thread_ptr_ = nullptr;
  }
}

Status WebServer::StartService() {
  oatpp::base::Environment::init();
  {
    WebComponent components = WebComponent(port_);

    /* create ApiControllers and add endpoints to router */
    auto user_controller = WebController::createShared();

    // Inject dependency services.
    user_controller->db_server->InjectEmbeddingService(embedding_service_url_);

    // Start rebuild thread
    if (rebuild_) {
      user_controller->db_server->StartRebuild();
    }
    user_controller->db_server->SetLeader(is_leader_);
    auto router = components.http_router.getObject();
    router->addController(user_controller);

    /* Get connection handler component */
    auto connection_handler =
        oatpp::web::server::HttpConnectionHandler::createShared(router);
    connection_handler->addRequestInterceptor(
        std::make_shared<EpsillaRequestInterceptor>());

    /* Get connection provider component */
    auto connection_provider =
        components.server_connection_provider.getObject();

    /* create server */
    oatpp::network::Server server(connection_provider, connection_handler);

    std::thread stop_thread([&server, this] {
      while (!this->try_stop_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      }

      server.stop();
      OATPP_COMPONENT(
          std::shared_ptr<oatpp::network::tcp::client::ConnectionProvider>,
          client_provider);
      client_provider->get();
    });

    // start synchronously
    server.run();
    connection_handler->stop();
    stop_thread.join();
  }
  oatpp::base::Environment::destroy();

  return Status::OK();
}

Status WebServer::StopService() {
  try_stop_.store(true);

  return Status::OK();
}

}  // namespace web
}  // namespace server
}  // namespace vectordb
