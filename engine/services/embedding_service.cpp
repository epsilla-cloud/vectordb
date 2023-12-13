#include "embedding_service.hpp"
#include <iostream>
#include <curl/curl.h>

namespace vectordb {
namespace engine {

EmbeddingService::EmbeddingService(const std::string& base_url) : m_httpClient(base_url) {}

Status EmbeddingService::getSupportedModels(std::vector<EmbeddingModel> &models) {
  try {
    auto response = m_httpClient.Get("/v1/embeddings");
    // Parse JSON response and convert to std::vector<EmbeddingModel>
    auto responseBody = response->readBodyToString();
    vectordb::Json json;
    json.LoadFromString(responseBody->c_str());
    auto models_json = json.GetArray("result");
    auto size = models_json.GetSize();
    for (auto i = 0; i < size; i++) {
      auto model_json = models_json.GetArrayElement(i);
      EmbeddingModel model;
      model.model = model_json.GetString("model");
      model.dim = model_json.GetInt("dim");
      model.dense = model_json.GetBool("dense");
      // model.description = model_json.GetString("description");
      // model.size_in_GB = model_json.GetDouble("size_in_GB");
      models.emplace_back(model);
    }
    return Status::OK();
  } catch (const std::exception& e) {
    std::cerr << "Exception in getSupportedModels: " << e.what() << std::endl;
    return Status(INFRA_UNEXPECTED_ERROR, "Failed to load supported embedding models.");
  }
}

Status EmbeddingService::denseEmbedDocuments(
  const std::string& model_name,
  VariableLenAttrColumnContainer& attr_column_container,
  float* vector_table,
  size_t start_record,
  size_t end_record,
  size_t dimension
) {
  std::cout << &m_httpClient << std::endl;
  // try {
  //   vectordb::Json request;
  //   request.LoadFromString("{\"model\": \"" + model_name + "\", \"documents\": []}");
  //   for (auto idx = start_record; idx < end_record; ++idx) {
  //     request.AddStringToArray("documents", std::get<std::string>(attr_column_container[idx]));
  //   }

  //   std::cout << request.DumpToString() << std::endl;
  //   std::cout << &m_httpClient << std::endl;
  //   std::string req_str = request.DumpToString();
  //   oatpp::String req_payload = oatpp::String(req_str.c_str(), req_str.size());





  //   // CURL *curl;
  //   // CURLcode res;

  //   // curl_global_init(CURL_GLOBAL_ALL);

  //   // curl = curl_easy_init();
  //   // if(curl) {
  //   //     // Set the URL for the POST request
  //   //     curl_easy_setopt(curl, CURLOPT_URL, "http://runner.epsilla.com:9999/v1/embeddings");

  //   //     // Set the payload (data to send)
  //   //     std::string payload = "{\"key1\":\"value1\",\"key2\":\"value2\"}";

  //   //     // If posting JSON, you also need to set the content type header
  //   //     struct curl_slist *headers = NULL;
  //   //     headers = curl_slist_append(headers, "Content-Type: application/json");
  //   //     curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  //   //     // Perform the request, res will get the return code
  //   //     res = curl_easy_perform(curl);

  //   //     // Check for errors
  //   //     if(res != CURLE_OK)
  //   //         fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));

  //   //     // Clean up headers if used
  //   //     curl_slist_free_all(headers);

  //   //     // Cleanup
  //   //     curl_easy_cleanup(curl);
  //   // }

  //   // curl_global_cleanup();





  //   std::cout << "here" << std::endl;
  //   auto response = m_httpClient.Get("/v1/embeddings");
  //   // Parse JSON response and convert to std::vector<EmbeddingModel>
  //   auto responseBody = response->readBodyToString();
  //   std::cout << "here2" << std::endl;
    

  //   // auto response = m_httpClient.Post("/v1/embeddings", req_payload);
  //   // auto responseBody = response->readBodyToString();
  //   // std::cout << responseBody->c_str() << std::endl;
  //   // vectordb::Json json;
  //   // json.LoadFromString(responseBody->c_str());
  //   // if (json.GetInt("statusCode") == 200) {
  //   //   auto embeddings = json.GetArray("result");
  //   //   for (auto idx = start_record; idx < end_record; ++idx) {
  //   //     auto embedding = embeddings.GetArrayElement(idx);
  //   //     for (auto i = 0; i < dimension; i++) {
  //   //       vector_table[idx * dimension + i] = static_cast<float>((float)(embedding.GetArrayElement(i).GetDouble()));
  //   //     }
  //   //   }
  //   //   return Status::OK();
  //   // }
  // } catch (const std::exception& e) {
  //   std::cerr << "Exception in embedDocuments: " << e.what() << std::endl;
  // }
  return Status(INFRA_UNEXPECTED_ERROR, "Failed to embbed the documents.");
}

}  // namespace engine
}  // namespace vectordb