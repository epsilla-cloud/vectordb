#pragma once

#include <iostream>

#include <oatpp/core/macro/component.hpp>
#include <oatpp/network/tcp/client/ConnectionProvider.hpp>
#include <oatpp/network/tcp/server/ConnectionProvider.hpp>
#include <oatpp/parser/json/mapping/Deserializer.hpp>
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include <oatpp/parser/json/mapping/Serializer.hpp>
#include <oatpp/web/server/HttpConnectionHandler.hpp>
#include <oatpp/web/server/HttpRouter.hpp>

namespace vectordb {
namespace server {
namespace web {

class WebComponent {

  public:
    explicit WebComponent(uint16_t port): port_(port) {}

  private:
    const uint16_t port_;

  public:
    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::ServerConnectionProvider>, server_connection_provider)
    ([this] {
      try {
        return oatpp::network::tcp::server::ConnectionProvider::createShared({"0.0.0.0", this->port_, oatpp::network::Address::IP_4});
      } catch (std::exception& e) {
        std::string error_msg = "Cannot bind http port " + std::to_string(this->port_) + ". " + e.what() +
          "(errno: " + std::to_string(errno) + ", details: " + strerror(errno) + ")";
        std::cerr << error_msg << std::endl;
        throw std::runtime_error(error_msg);
      }
    }());

    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::ClientConnectionProvider>, client_connection_provider)
    ([this] {
      return oatpp::network::tcp::client::ConnectionProvider::createShared({"0.0.0.0", this->port_, oatpp::network::Address::IP_4});
    }());

    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::web::server::HttpRouter>, http_router)([] {
        return oatpp::web::server::HttpRouter::createShared();
    }());

    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::network::ConnectionHandler>, server_connection_handler)
    ([] {
        OATPP_COMPONENT(std::shared_ptr<oatpp::web::server::HttpRouter>, router);
        return oatpp::web::server::HttpConnectionHandler::createShared(router);
    }());

    OATPP_CREATE_COMPONENT(std::shared_ptr<oatpp::data::mapping::ObjectMapper>, api_object_mapper_)
    ([] {
        auto serializerConfig = oatpp::parser::json::mapping::Serializer::Config::createShared();
        auto deserializerConfig = oatpp::parser::json::mapping::Deserializer::Config::createShared();
        deserializerConfig->allowUnknownFields = false;
        return oatpp::parser::json::mapping::ObjectMapper::createShared(serializerConfig, deserializerConfig);
    }());
};

} // name space web
} // namespace server
} // namespace vectordb
