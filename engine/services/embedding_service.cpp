#include "embedding_service.hpp"
#include <iostream>
#include <curl/curl.h>

#include <chrono>
#include <thread>
#include <random>

namespace vectordb {
namespace engine {

EmbeddingService::EmbeddingService(const std::string& base_url) {
  auto requestExecutor = oatpp::curl::RequestExecutor::createShared(base_url);
  auto objectMapper = oatpp::parser::json::mapping::ObjectMapper::createShared();
  m_client = MyApiClient::createShared(requestExecutor, objectMapper);
}

Status EmbeddingService::getSupportedModels(std::vector<EmbeddingModel> &models) {
  try {
    auto response = m_client->getEmbeddings("/v1/embeddings");
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
  int attempt = 0;
  while (attempt < EmbeddingRetry) {
    try {
      auto requestBody = EmbeddingRequestBody::createShared();
      requestBody->model = model_name;
      // Constructing documents list from attr_column_container
      requestBody->documents = oatpp::List<oatpp::String>({});
      for (size_t idx = start_record; idx < end_record; ++idx) {
        // Assuming attr_column_container[idx] returns a string or can be converted to string
        requestBody->documents->push_back(oatpp::String(std::get<std::string>(attr_column_container[idx]).c_str()));
      }
      auto response = m_client->denseEmbedDocuments("/v1/embeddings", requestBody);
      auto responseBody = response->readBodyToString();
      // std::cout << "Embedding response: " << responseBody->c_str() << std::endl;
      vectordb::Json json;
      json.LoadFromString(responseBody->c_str());
      if (json.GetInt("statusCode") == 200) {
        auto embeddings = json.GetArray("result");
        for (auto idx = start_record; idx < end_record; ++idx) {
          auto embedding = embeddings.GetArrayElement(idx - start_record);
          for (auto i = 0; i < dimension; i++) {
            vector_table[idx * dimension + i] = static_cast<float>((float)(embedding.GetArrayElement(i).GetDouble()));
          }
        }
        return Status::OK();
      }
    } catch (const std::exception& e) {
      std::cerr << "Exception in embedDocuments: " << e.what() << std::endl;
    }
    attempt++;
    if (attempt >= EmbeddingRetry) {
      break;
    }
    // Exponential backoff logic
    int delaySec = EmbeddingBackoffInitialDelaySec * std::pow(EmbeddingBackoffExpBase, attempt);
    std::this_thread::sleep_for(std::chrono::seconds(delaySec));
    std::cout << "Retry embedding documents." << std::endl;
  }
  return Status(INFRA_UNEXPECTED_ERROR, "Failed to embbed the documents.");
}

}  // namespace engine
}  // namespace vectordb