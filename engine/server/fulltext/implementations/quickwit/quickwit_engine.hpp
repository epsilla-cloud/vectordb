#pragma once

#include <memory>
#include <string>
#include "server/fulltext/fulltext_engine.hpp"
#include "server/fulltext/implementations/quickwit/quickwit_process.hpp"
#include "server/fulltext/implementations/quickwit/quickwit_client.hpp"
#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace server {
namespace fulltext {
namespace quickwit {

/**
 * @brief QuickwitEngine - Quickwit 全文搜索引擎实现
 *
 * 实现 FullTextEngine 接口，提供 Quickwit 特定功能
 */
class QuickwitEngine : public FullTextEngine {
public:
  explicit QuickwitEngine(const FullTextEngineConfig& config);
  ~QuickwitEngine() override;

  // ===== FullTextEngine 接口实现 =====

  EngineType GetEngineType() const override {
    return EngineType::QUICKWIT;
  }

  std::string GetEngineName() const override {
    return "Quickwit";
  }

  Status Start() override;
  Status Stop() override;
  bool IsRunning() const override;
  bool HealthCheck() override;

  // 索引管理
  Status CreateIndex(const std::string& index_name,
                     const Json& index_config) override;
  Status DeleteIndex(const std::string& index_name) override;
  bool IndexExists(const std::string& index_name) override;

  // 文档操作
  Status IndexDocument(const std::string& index_name,
                       const Json& document) override;
  Status IndexBatch(const std::string& index_name,
                    const std::vector<Json>& documents) override;
  Status DeleteDocuments(const std::string& index_name,
                         const std::vector<std::string>& ids) override;

  // 搜索
  Json Search(const std::string& index_name,
              const std::string& query,
              size_t limit = 10,
              size_t offset = 0) override;
  Json SearchAdvanced(const std::string& index_name,
                      const Json& search_request) override;

  // 统计
  Json GetIndexStats(const std::string& index_name) override;

  // ===== Quickwit 特定方法 =====

  /**
   * @brief 获取 Quickwit HTTP 端点
   */
  std::string GetEndpoint() const;

  /**
   * @brief 获取 Quickwit 进程 ID
   */
  pid_t GetProcessId() const;

private:
  FullTextEngineConfig config_;
  std::unique_ptr<QuickwitProcess> process_manager_;
  std::unique_ptr<QuickwitClient> client_;
  vectordb::engine::Logger logger_;
};

} // namespace quickwit
} // namespace fulltext
} // namespace server
} // namespace vectordb
