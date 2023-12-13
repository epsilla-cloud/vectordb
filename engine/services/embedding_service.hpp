#pragma once

#include "utils/http_client.hpp"
#include "utils/status.hpp"
#include "utils/json.hpp"
#include "db/vector.hpp"
#include <vector>
#include <string>

namespace vectordb {
namespace engine {

// TODO: make retry and backoff time configurable.
constexpr const int EmbeddingRetry = 5;
constexpr const int EmbeddingBackoffInitialDelaySec = 1;
constexpr const int EmbeddingBackoffExpBase = 2;
constexpr const int EmbeddingBackoffJitter = 1;  // 0 means no jitter, 1 means with jitter

struct EmbeddingModel {
  std::string model;
  size_t dim;
  bool dense;
  // std::string description;
  // double size_in_GB;
};

class EmbeddingService {
public:
  EmbeddingService(const std::string& base_url);

  Status getSupportedModels(std::vector<EmbeddingModel>& models);
  Status denseEmbedDocuments(
    const std::string& model_name,
    VariableLenAttrColumnContainer& attr_column_container,
    float* vector_table,
    size_t start_record,
    size_t end_record,
    size_t dimension
  );

private:
  HttpClient m_httpClient;
};

}  // namespace engine
}  // namespace vectordb