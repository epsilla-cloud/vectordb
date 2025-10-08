#pragma once

#include <string>
#include <vector>
#include <memory>
#include "utils/status.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace server {
namespace fulltext {

/**
 * @brief FullTextEngine - 全文搜索引擎抽象接口
 *
 * 这是一个通用接口，支持多种全文搜索后端：
 * - Quickwit (默认)
 * - Elasticsearch
 * - Meilisearch
 * - OpenSearch
 * 等等
 */
class FullTextEngine {
public:
  virtual ~FullTextEngine() = default;

  /**
   * @brief 引擎类型枚举
   */
  enum class EngineType {
    QUICKWIT,
    ELASTICSEARCH,
    MEILISEARCH,
    OPENSEARCH,
    CUSTOM
  };

  /**
   * @brief 获取引擎类型
   */
  virtual EngineType GetEngineType() const = 0;

  /**
   * @brief 获取引擎名称
   */
  virtual std::string GetEngineName() const = 0;

  /**
   * @brief 启动全文搜索引擎
   *
   * @return Status
   */
  virtual Status Start() = 0;

  /**
   * @brief 停止全文搜索引擎
   *
   * @return Status
   */
  virtual Status Stop() = 0;

  /**
   * @brief 检查引擎是否正在运行
   *
   * @return bool
   */
  virtual bool IsRunning() const = 0;

  /**
   * @brief 健康检查
   *
   * @return bool
   */
  virtual bool HealthCheck() = 0;

  // ===== 索引管理 =====

  /**
   * @brief 创建索引
   *
   * @param index_name 索引名称
   * @param index_config 索引配置（JSON）
   * @return Status
   */
  virtual Status CreateIndex(const std::string& index_name,
                              const Json& index_config) = 0;

  /**
   * @brief 删除索引
   *
   * @param index_name 索引名称
   * @return Status
   */
  virtual Status DeleteIndex(const std::string& index_name) = 0;

  /**
   * @brief 检查索引是否存在
   *
   * @param index_name 索引名称
   * @return bool
   */
  virtual bool IndexExists(const std::string& index_name) = 0;

  // ===== 文档操作 =====

  /**
   * @brief 索引单个文档
   *
   * @param index_name 索引名称
   * @param document 文档（JSON）
   * @return Status
   */
  virtual Status IndexDocument(const std::string& index_name,
                                const Json& document) = 0;

  /**
   * @brief 批量索引文档
   *
   * @param index_name 索引名称
   * @param documents 文档列表
   * @return Status
   */
  virtual Status IndexBatch(const std::string& index_name,
                             const std::vector<Json>& documents) = 0;

  /**
   * @brief 删除文档（通过 ID 列表）
   *
   * @param index_name 索引名称
   * @param ids 文档 ID 列表
   * @return Status
   */
  virtual Status DeleteDocuments(const std::string& index_name,
                                  const std::vector<std::string>& ids) = 0;

  // ===== 搜索 =====

  /**
   * @brief 全文搜索
   *
   * @param index_name 索引名称
   * @param query 查询字符串
   * @param limit 返回结果数量
   * @param offset 起始偏移
   * @return Json 搜索结果
   */
  virtual Json Search(const std::string& index_name,
                      const std::string& query,
                      size_t limit = 10,
                      size_t offset = 0) = 0;

  /**
   * @brief 高级搜索（支持完整查询参数）
   *
   * @param index_name 索引名称
   * @param search_request 完整搜索请求（JSON）
   * @return Json 搜索结果
   */
  virtual Json SearchAdvanced(const std::string& index_name,
                               const Json& search_request) = 0;

  // ===== 统计信息 =====

  /**
   * @brief 获取索引统计信息
   *
   * @param index_name 索引名称
   * @return Json 统计信息
   */
  virtual Json GetIndexStats(const std::string& index_name) = 0;
};

/**
 * @brief 全文搜索引擎配置
 */
struct FullTextEngineConfig {
  // 通用配置
  FullTextEngine::EngineType engine_type = FullTextEngine::EngineType::QUICKWIT;
  std::string data_dir = "./fulltext_data";
  int port = 7280;

  // 引擎特定配置（可选）
  std::string binary_path;        // 二进制路径（适用于 Quickwit）
  std::string config_path;        // 配置文件路径
  std::string endpoint;           // HTTP 端点（适用于 ES/Meilisearch）
  std::string api_key;            // API 密钥（可选）
  std::string username;           // 用户名（可选）
  std::string password;           // 密码（可选）

  // 性能配置
  int max_memory_mb = 2048;
  int num_threads = 4;
  bool enable_cache = true;
};

} // namespace fulltext
} // namespace server
} // namespace vectordb
