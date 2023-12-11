#include "embedding_service.hpp"
#include <iostream>

namespace vectordb {
namespace engine {

EmbeddingService::EmbeddingService(const std::string& baseUrl) : m_httpClient(baseUrl) {}

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

Status EmbeddingService::embedDocuments(
  const std::string& modelName,
  const std::vector<std::string>& documents,
  std::vector<std::vector<double>> &embeddings
) {
  try {
    nlohmann::json requestBody = {
      {"model", modelName},
      {"documents", documents}
    };

    auto response = m_httpClient.Post("/v1/embeddings", requestBody.dump());
    // Parse JSON response and convert to std::vector<std::vector<double>>
    // ...
    auto responseBody = response->readBodyToString();
    std::cout << "responseBody: " << responseBody->c_str() << std::endl;
    return Status::OK();
  } catch (const std::exception& e) {
    std::cerr << "Exception in embedDocuments: " << e.what() << std::endl;
    return Status(INFRA_UNEXPECTED_ERROR, "Failed to embbed the documents.");
  }
}

}  // namespace engine
}  // namespace vectordb