#pragma once

#include <memory>
#include <string>
#include <vector>
#include "utils/status.hpp"
#include "utils/json.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace server {
namespace fulltext {
namespace quickwit {

using vectordb::engine::Logger;

/**
 * @brief QuickwitClient - Quickwit HTTP REST API 客户端
 *
 * 功能：
 * - 索引管理（创建、删除）
 * - 文档操作（索引、批量索引、删除）
 * - 全文搜索查询
 *
 * 注意：这是 Quickwit 特定的实现，不对外暴露
 */
class QuickwitClient {
public:
  /**
   * @brief 构造函数
   * @param base_url Quickwit base URL (e.g., "http://localhost:7280")
   */
  explicit QuickwitClient(const std::string& base_url);

  ~QuickwitClient() = default;

  // ===== 索引管理 =====

  /**
   * @brief 创建索引
   * @param index_id 索引 ID
   * @param index_config 索引配置（YAML 格式字符串）
   * @return Status
   */
  Status CreateIndex(const std::string& index_id,
                     const std::string& index_config);

  /**
   * @brief 删除索引
   * @param index_id 索引 ID
   * @return Status
   */
  Status DeleteIndex(const std::string& index_id);

  /**
   * @brief 检查索引是否存在
   * @param index_id 索引 ID
   * @return bool
   */
  bool IndexExists(const std::string& index_id);

  // ===== 文档操作 =====

  /**
   * @brief 索引单个文档
   * @param index_id 索引 ID
   * @param document 文档（JSON）
   * @return Status
   */
  Status IndexDocument(const std::string& index_id,
                       const Json& document);

  /**
   * @brief 批量索引文档（推荐用于提高性能）
   * @param index_id 索引 ID
   * @param documents 文档列表
   * @return Status
   */
  Status IndexBatch(const std::string& index_id,
                    const std::vector<Json>& documents);

  /**
   * @brief 删除文档（通过查询）
   * @param index_id 索引 ID
   * @param query 删除查询（例如 "id:123"）
   * @return Status
   */
  Status DeleteByQuery(const std::string& index_id,
                       const std::string& query);

  /**
   * @brief 删除文档（通过 ID 列表）
   * @param index_id 索引 ID
   * @param ids 文档 ID 列表
   * @return Status
   */
  Status DeleteByIds(const std::string& index_id,
                     const std::vector<std::string>& ids);

  // ===== 搜索 =====

  /**
   * @brief 全文搜索
   * @param index_id 索引 ID
   * @param query 查询字符串
   * @param max_hits 返回结果数量
   * @param start_offset 起始偏移
   * @param search_fields 搜索字段列表（可选）
   * @return Json 搜索结果
   */
  Json Search(const std::string& index_id,
              const std::string& query,
              size_t max_hits = 10,
              size_t start_offset = 0,
              const std::vector<std::string>& search_fields = {});

  /**
   * @brief 高级搜索（支持完整查询参数）
   * @param index_id 索引 ID
   * @param search_request 完整搜索请求（JSON）
   * @return Json 搜索结果
   */
  Json SearchAdvanced(const std::string& index_id,
                      const Json& search_request);

  // ===== 工具方法 =====

  /**
   * @brief 健康检查
   * @return bool
   */
  bool HealthCheck();

  /**
   * @brief 获取 base URL
   * @return std::string
   */
  std::string GetBaseUrl() const {
    return base_url_;
  }

private:
  /**
   * @brief 执行 HTTP GET 请求
   * @param path API 路径
   * @return pair<int, string> HTTP 状态码和响应体
   */
  std::pair<int, std::string> HttpGet(const std::string& path);

  /**
   * @brief 执行 HTTP POST 请求
   * @param path API 路径
   * @param body 请求体
   * @param content_type Content-Type header
   * @return pair<int, string> HTTP 状态码和响应体
   */
  std::pair<int, std::string> HttpPost(const std::string& path,
                                        const std::string& body,
                                        const std::string& content_type = "application/json");

  /**
   * @brief 执行 HTTP DELETE 请求
   * @param path API 路径
   * @return pair<int, string> HTTP 状态码和响应体
   */
  std::pair<int, std::string> HttpDelete(const std::string& path);

  /**
   * @brief 将文档转换为 NDJSON 格式
   * @param documents 文档列表
   * @return string NDJSON 字符串
   */
  std::string ToNDJSON(const std::vector<Json>& documents);

  std::string base_url_;
  Logger logger_;
};

} // namespace quickwit
} // namespace fulltext
} // namespace server
} // namespace vectordb
