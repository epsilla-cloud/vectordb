#pragma once

#include "utils/status.hpp"
#include "utils/json.hpp"
#include "db/vector.hpp"
#include <vector>
#include <string>
#include <unordered_map>
#include <utils/constants.hpp>

#include "oatpp-curl/RequestExecutor.hpp"
#include "oatpp/web/client/ApiClient.hpp"
#include "oatpp/core/macro/component.hpp"
#include "oatpp/core/Types.hpp"
#include <oatpp/core/macro/codegen.hpp>
#include "oatpp/codegen/ApiClient_define.hpp"
#include <oatpp/parser/json/mapping/ObjectMapper.hpp>
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

// TODO: make retry and backoff time configurable.
constexpr const int EmbeddingDocsRetry = 3;
constexpr const int EmbeddingQueryRetry = 2;
constexpr const int EmbeddingBackoffInitialDelaySec = 1;
constexpr const int EmbeddingBackoffExpBase = 2;
constexpr const int EmbeddingBackoffJitter = 1;  // 0 means no jitter, 1 means with jitter

#include OATPP_CODEGEN_BEGIN(DTO) ///< Begin DTO codegen section

class EmbeddingRequestBody: public oatpp::DTO {

  DTO_INIT(EmbeddingRequestBody, DTO)

  DTO_FIELD(String, model);
  DTO_FIELD(List<String>, documents);
  DTO_FIELD(Int32, dimensions);
};

class ContextualizedEmbeddingRequestBody: public oatpp::DTO {

  DTO_INIT(ContextualizedEmbeddingRequestBody, DTO)

  DTO_FIELD(String, model);
  DTO_FIELD(List<List<String>>, inputs);
  DTO_FIELD(String, input_type);
  DTO_FIELD(Int32, output_dimension);
  DTO_FIELD(String, output_dtype);
};

class MultimodalEmbeddingRequestBody: public oatpp::DTO {

  DTO_INIT(MultimodalEmbeddingRequestBody, DTO)

  DTO_FIELD(String, model);
  DTO_FIELD(List<List<String>>, inputs);
  DTO_FIELD(String, input_type);
  DTO_FIELD(Boolean, truncation);
};

#include OATPP_CODEGEN_END(DTO) ///< End DTO codegen section

class MyApiClient : public oatpp::web::client::ApiClient {
 #include OATPP_CODEGEN_BEGIN(ApiClient) //<- Begin codegen

  API_CLIENT_INIT(MyApiClient)

  API_CALL("GET", "{path}", getEmbeddings, PATH(String, path))
  API_CALL("POST", "{path}", denseEmbedDocuments, PATH(String, path), HEADER(String, openaiHeader, OPENAI_KEY_HEADER), HEADER(String, jinaaiHeader, JINAAI_KEY_HEADER), HEADER(String, voyageaiHeader, VOYAGEAI_KEY_HEADER), HEADER(String, mixedbreadaiHeader, MIXEDBREADAI_KEY_HEADER), HEADER(String, nomicHeader, NOMIC_KEY_HEADER), HEADER(String, mistralaiHeader, MISTRALAI_KEY_HEADER), BODY_DTO(Object<EmbeddingRequestBody>, body))
  API_CALL("POST", "{path}", contextualizedEmbedDocuments, PATH(String, path), HEADER(String, voyageaiHeader, VOYAGEAI_KEY_HEADER), BODY_DTO(Object<ContextualizedEmbeddingRequestBody>, body))
  API_CALL("POST", "{path}", multimodalEmbedDocuments, PATH(String, path), HEADER(String, voyageaiHeader, VOYAGEAI_KEY_HEADER), BODY_DTO(Object<MultimodalEmbeddingRequestBody>, body))


 #include OATPP_CODEGEN_END(ApiClient) //<- End codegen
};

struct EmbeddingModel {
  std::string model;
  size_t dim;
  bool dense;
  bool dimensionReduction;
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
    size_t dimension,
    std::unordered_map<std::string, std::string> &headers,
    bool isReducingDimension
  );
  Status denseEmbedQuery(
    const std::string& model_name,
    const std::string &query,
    std::vector<engine::DenseVectorElement> &denseQueryVec,
    size_t dimension,
    std::unordered_map<std::string, std::string> &headers,
    bool isReducingDimension
  );

private:
  std::shared_ptr<MyApiClient> m_client;
  vectordb::engine::Logger logger_;
};

}  // namespace engine
}  // namespace vectordb
