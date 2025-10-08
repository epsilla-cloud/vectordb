#include "server/fulltext/fulltext_manager.hpp"
#include "server/fulltext/implementations/quickwit/quickwit_engine.hpp"

namespace vectordb {
namespace server {
namespace fulltext {

// 静态 logger 初始化
vectordb::engine::Logger FullTextManager::logger_;

std::unique_ptr<FullTextEngine> FullTextManager::CreateEngine(
    const FullTextEngineConfig& config) {

  logger_.Info("[FullText] Creating engine [type=" + EngineTypeToString(config.engine_type) + "]");

  switch (config.engine_type) {
    case FullTextEngine::EngineType::QUICKWIT:
      return std::make_unique<quickwit::QuickwitEngine>(config);

    case FullTextEngine::EngineType::ELASTICSEARCH:
      logger_.Error("[FullText] Engine not yet implemented [type=Elasticsearch]");
      return nullptr;

    case FullTextEngine::EngineType::MEILISEARCH:
      logger_.Error("[FullText] Engine not yet implemented [type=Meilisearch]");
      return nullptr;

    case FullTextEngine::EngineType::OPENSEARCH:
      logger_.Error("[FullText] Engine not yet implemented [type=OpenSearch]");
      return nullptr;

    case FullTextEngine::EngineType::CUSTOM:
      logger_.Error("[FullText] Engine not supported [type=Custom]");
      return nullptr;

    default:
      logger_.Error("[FullText] Unknown engine type");
      return nullptr;
  }
}

FullTextEngine::EngineType FullTextManager::ParseEngineType(const std::string& engine_name) {
  std::string name_lower = engine_name;
  // 转换为小写
  std::transform(name_lower.begin(), name_lower.end(), name_lower.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  if (name_lower == "quickwit") {
    return FullTextEngine::EngineType::QUICKWIT;
  } else if (name_lower == "elasticsearch" || name_lower == "es") {
    return FullTextEngine::EngineType::ELASTICSEARCH;
  } else if (name_lower == "meilisearch") {
    return FullTextEngine::EngineType::MEILISEARCH;
  } else if (name_lower == "opensearch") {
    return FullTextEngine::EngineType::OPENSEARCH;
  } else {
    logger_.Warning("[FullText] Unknown engine type, using default [requested=" + engine_name + ", default=Quickwit]");
    return FullTextEngine::EngineType::QUICKWIT;
  }
}

std::string FullTextManager::EngineTypeToString(FullTextEngine::EngineType type) {
  switch (type) {
    case FullTextEngine::EngineType::QUICKWIT:
      return "Quickwit";
    case FullTextEngine::EngineType::ELASTICSEARCH:
      return "Elasticsearch";
    case FullTextEngine::EngineType::MEILISEARCH:
      return "Meilisearch";
    case FullTextEngine::EngineType::OPENSEARCH:
      return "OpenSearch";
    case FullTextEngine::EngineType::CUSTOM:
      return "Custom";
    default:
      return "Unknown";
  }
}

bool FullTextManager::IsEngineSupported(FullTextEngine::EngineType type) {
  // 目前只支持 Quickwit
  return type == FullTextEngine::EngineType::QUICKWIT;
}

std::vector<std::string> FullTextManager::GetSupportedEngines() {
  return {"Quickwit"};
  // 未来添加更多引擎时扩展:
  // return {"Quickwit", "Elasticsearch", "Meilisearch", "OpenSearch"};
}

} // namespace fulltext
} // namespace server
} // namespace vectordb
