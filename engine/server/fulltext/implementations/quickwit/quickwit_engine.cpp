#include "server/fulltext/implementations/quickwit/quickwit_engine.hpp"

namespace vectordb {
namespace server {
namespace fulltext {
namespace quickwit {

QuickwitEngine::QuickwitEngine(const FullTextEngineConfig& config)
  : config_(config) {
  logger_.Info("[Quickwit] Initializing engine");
}

QuickwitEngine::~QuickwitEngine() {
  if (IsRunning()) {
    Stop();
  }
}

Status QuickwitEngine::Start() {
  logger_.Info("[Quickwit] Starting engine");

  // 创建进程管理器
  process_manager_ = std::make_unique<QuickwitProcess>();

  // 启动 Quickwit 子进程
  auto status = process_manager_->Start(
    config_.binary_path,
    config_.config_path,
    config_.data_dir,
    config_.port
  );

  if (!status.ok()) {
    logger_.Error("[Quickwit] Failed to start process [error=" + status.ToString() + "]");
    return status;
  }

  // 创建 HTTP 客户端
  client_ = std::make_unique<QuickwitClient>(process_manager_->GetEndpoint());

  logger_.Info("[Quickwit] Engine started successfully [endpoint=" + process_manager_->GetEndpoint() + "]");
  return Status::OK();
}

Status QuickwitEngine::Stop() {
  logger_.Info("[Quickwit] Stopping engine");

  if (process_manager_) {
    auto status = process_manager_->Stop();
    if (!status.ok()) {
      logger_.Error("[Quickwit] Failed to stop process [error=" + status.ToString() + "]");
      return status;
    }
  }

  client_.reset();
  process_manager_.reset();

  logger_.Info("[Quickwit] Engine stopped");
  return Status::OK();
}

bool QuickwitEngine::IsRunning() const {
  return process_manager_ && process_manager_->IsRunning();
}

bool QuickwitEngine::HealthCheck() {
  if (!IsRunning()) {
    return false;
  }

  if (!client_) {
    return false;
  }

  return client_->HealthCheck();
}

// ===== 索引管理 =====

Status QuickwitEngine::CreateIndex(const std::string& index_name,
                                     const Json& index_config) {
  if (!client_) {
    return Status(DB_UNEXPECTED_ERROR, "Quickwit client not initialized");
  }

  // 将 JSON 配置转换为 YAML 字符串
  // 注意：这里简化处理，实际应该做完整的 JSON→YAML 转换
  Json config_copy = index_config;
  std::string config_str = config_copy.DumpToString();

  return client_->CreateIndex(index_name, config_str);
}

Status QuickwitEngine::DeleteIndex(const std::string& index_name) {
  if (!client_) {
    return Status(DB_UNEXPECTED_ERROR, "Quickwit client not initialized");
  }

  return client_->DeleteIndex(index_name);
}

bool QuickwitEngine::IndexExists(const std::string& index_name) {
  if (!client_) {
    return false;
  }

  return client_->IndexExists(index_name);
}

// ===== 文档操作 =====

Status QuickwitEngine::IndexDocument(const std::string& index_name,
                                       const Json& document) {
  if (!client_) {
    return Status(DB_UNEXPECTED_ERROR, "Quickwit client not initialized");
  }

  return client_->IndexDocument(index_name, document);
}

Status QuickwitEngine::IndexBatch(const std::string& index_name,
                                    const std::vector<Json>& documents) {
  if (!client_) {
    return Status(DB_UNEXPECTED_ERROR, "Quickwit client not initialized");
  }

  return client_->IndexBatch(index_name, documents);
}

Status QuickwitEngine::DeleteDocuments(const std::string& index_name,
                                         const std::vector<std::string>& ids) {
  if (!client_) {
    return Status(DB_UNEXPECTED_ERROR, "Quickwit client not initialized");
  }

  return client_->DeleteByIds(index_name, ids);
}

// ===== 搜索 =====

Json QuickwitEngine::Search(const std::string& index_name,
                              const std::string& query,
                              size_t limit,
                              size_t offset) {
  if (!client_) {
    logger_.Error("[Quickwit] Client not initialized");
    return Json();
  }

  return client_->Search(index_name, query, limit, offset);
}

Json QuickwitEngine::SearchAdvanced(const std::string& index_name,
                                      const Json& search_request) {
  if (!client_) {
    logger_.Error("[Quickwit] Client not initialized");
    return Json();
  }

  return client_->SearchAdvanced(index_name, search_request);
}

// ===== 统计信息 =====

Json QuickwitEngine::GetIndexStats(const std::string& index_name) {
  if (!client_) {
    logger_.Error("[Quickwit] Client not initialized");
    return Json();
  }

  // TODO: 实现获取索引统计信息
  // Quickwit 可能需要调用特定的 API
  logger_.Warning("[Quickwit] GetIndexStats not yet implemented");

  Json stats;
  stats.SetString("index_name", index_name);
  stats.SetString("status", "unknown");
  return stats;
}

// ===== Quickwit 特定方法 =====

std::string QuickwitEngine::GetEndpoint() const {
  if (process_manager_) {
    return process_manager_->GetEndpoint();
  }
  return "";
}

pid_t QuickwitEngine::GetProcessId() const {
  if (process_manager_) {
    return process_manager_->GetPid();
  }
  return -1;
}

} // namespace quickwit
} // namespace fulltext
} // namespace server
} // namespace vectordb
