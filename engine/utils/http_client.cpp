#include "http_client.hpp"

namespace vectordb {

class HttpClient::MyApiClient : public oatpp::web::client::ApiClient {
 #include OATPP_CODEGEN_BEGIN(ApiClient) //<- Begin codegen

  API_CLIENT_INIT(MyApiClient)

  API_CALL("GET", "{path}", get, PATH(String, path))
  API_CALL("POST", "{path}", post, PATH(String, path), BODY_STRING(String, body), HEADER(String, contentType))
  API_CALL("PUT", "{path}", put, PATH(String, path), BODY_STRING(String, body), HEADER(String, contentType))
  API_CALL("DELETE", "{path}", deleteRequest, PATH(String, path))

 #include OATPP_CODEGEN_END(ApiClient) //<- End codegen
};

HttpClient::HttpClient(const std::string& baseUrl) {
  // auto requestExecutor = oatpp::curl::RequestExecutor::createShared(baseUrl);
  // m_client = MyApiClient::createShared(requestExecutor);
  auto requestExecutor = oatpp::curl::RequestExecutor::createShared(baseUrl);
  auto objectMapper = oatpp::parser::json::mapping::ObjectMapper::createShared();
  m_client = MyApiClient::createShared(requestExecutor, objectMapper);
}

std::shared_ptr<oatpp::web::protocol::http::incoming::Response> HttpClient::Get(const std::string& path) {
  return m_client->get(path);
}

std::shared_ptr<oatpp::web::protocol::http::incoming::Response> HttpClient::Post(const std::string& path, const oatpp::String& body) {
  return m_client->post(path, body, "application/json");
}

std::shared_ptr<oatpp::web::protocol::http::incoming::Response> HttpClient::Put(const std::string& path, const oatpp::String& body) {
  return m_client->put(path, body, "application/json");
}

std::shared_ptr<oatpp::web::protocol::http::incoming::Response> HttpClient::Delete(const std::string& path) {
  return m_client->deleteRequest(path);
}

}  // namespace vectordb
