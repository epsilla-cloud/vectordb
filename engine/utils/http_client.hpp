#pragma once

#include "oatpp-curl/RequestExecutor.hpp"
#include "oatpp/web/client/ApiClient.hpp"
#include "oatpp/core/macro/component.hpp"
#include "oatpp/core/Types.hpp"
#include "oatpp/codegen/ApiClient_define.hpp"
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>

#include <string>

namespace vectordb {

class HttpClient {
 public:
  HttpClient(const std::string& baseUrl);
  std::shared_ptr<oatpp::web::protocol::http::incoming::Response> Get(const std::string& path);
  std::shared_ptr<oatpp::web::protocol::http::incoming::Response> Post(const std::string& path, const oatpp::String& body);
  std::shared_ptr<oatpp::web::protocol::http::incoming::Response> Put(const std::string& path, const oatpp::String& body);
  std::shared_ptr<oatpp::web::protocol::http::incoming::Response> Delete(const std::string& path);

 private:
  class MyApiClient;

  std::shared_ptr<MyApiClient> m_client;
};

}  // namespace vectordb
