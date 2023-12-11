#pragma once

#include "utils/http_client.hpp"
#include "utils/status.hpp"
#include "utils/json.hpp"
#include <vector>
#include <string>

namespace vectordb {
namespace engine {

struct EmbeddingModel {
  std::string model;
  size_t dim;
  bool dense;
  // std::string description;
  // double size_in_GB;
};

class EmbeddingService {
public:
  EmbeddingService(const std::string& baseUrl);

  Status getSupportedModels(std::vector<EmbeddingModel>& models);
  Status embedDocuments(
    const std::string& modelName,
    const std::vector<std::string>& documents,
    std::vector<std::vector<double>> &embeddings
  );

private:
  HttpClient m_httpClient;
};

}  // namespace engine
}  // namespace vectordb