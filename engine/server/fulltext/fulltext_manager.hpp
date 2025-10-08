#pragma once

#include <memory>
#include <string>
#include "server/fulltext/fulltext_engine.hpp"
#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace server {
namespace fulltext {

/**
 * @brief FullTextManager - 全文搜索引擎管理器（工厂模式）
 *
 * 负责：
 * - 根据配置创建对应的全文搜索引擎实例
 * - 提供统一的访问接口
 * - 管理引擎的生命周期
 */
class FullTextManager {
public:
  /**
   * @brief 创建全文搜索引擎
   *
   * @param config 引擎配置
   * @return unique_ptr<FullTextEngine> 引擎实例
   */
  static std::unique_ptr<FullTextEngine> CreateEngine(
      const FullTextEngineConfig& config);

  /**
   * @brief 从字符串创建引擎类型
   *
   * @param engine_name 引擎名称（"quickwit", "elasticsearch", 等）
   * @return EngineType
   */
  static FullTextEngine::EngineType ParseEngineType(const std::string& engine_name);

  /**
   * @brief 获取引擎类型的字符串表示
   *
   * @param type 引擎类型
   * @return string 引擎名称
   */
  static std::string EngineTypeToString(FullTextEngine::EngineType type);

  /**
   * @brief 检查引擎是否被支持
   *
   * @param type 引擎类型
   * @return bool
   */
  static bool IsEngineSupported(FullTextEngine::EngineType type);

  /**
   * @brief 获取所有支持的引擎列表
   *
   * @return vector<string> 引擎名称列表
   */
  static std::vector<std::string> GetSupportedEngines();

private:
  static vectordb::engine::Logger logger_;
};

} // namespace fulltext
} // namespace server
} // namespace vectordb
