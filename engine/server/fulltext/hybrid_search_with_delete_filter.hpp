#pragma once

#include <vector>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include "server/fulltext/hybrid_search_engine.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace server {
namespace fulltext {

/**
 * @brief 支持删除过滤的混合搜索引擎
 *
 * 基于主键（Primary Key）实现向量搜索和全文搜索的结果合并，
 * 并在应用层过滤已删除的记录。
 */
class HybridSearchWithDeleteFilter {
public:
  using SearchResult = HybridSearchEngine::SearchResult;

  /**
   * @brief 合并搜索结果并过滤已删除记录
   *
   * @param vector_results 向量搜索结果（使用主键作为 ID）
   * @param text_results 全文搜索结果（使用主键作为 ID）
   * @param deleted_primary_keys 已删除记录的主键集合
   * @param k RRF 常数（默认 60）
   * @param vector_weight 向量搜索权重
   * @param text_weight 全文搜索权重
   * @return 过滤后的合并结果
   */
  static std::vector<SearchResult> MergeAndFilter(
      const std::vector<SearchResult>& vector_results,
      const std::vector<SearchResult>& text_results,
      const std::unordered_set<std::string>& deleted_primary_keys,
      int k = 60,
      double vector_weight = 0.5,
      double text_weight = 0.5) {

    engine::Logger logger;
    logger.Info("[Hybrid] Starting merge and filter [vector_count=" +
                std::to_string(vector_results.size()) +
                ", text_count=" + std::to_string(text_results.size()) +
                ", deleted_count=" + std::to_string(deleted_primary_keys.size()) + "]");

    // 1. 使用标准 RRF 合并
    auto merged = HybridSearchEngine::MergeResults(
        vector_results, text_results, k, vector_weight, text_weight);

    logger.Debug("[Hybrid] RRF merge completed [merged_count=" +
                 std::to_string(merged.size()) + "]");

    // 2. 过滤已删除的记录
    std::vector<SearchResult> filtered;
    filtered.reserve(merged.size());

    size_t filtered_count = 0;
    for (const auto& result : merged) {
      if (deleted_primary_keys.find(result.id) == deleted_primary_keys.end()) {
        // 主键不在删除集合中，保留
        filtered.push_back(result);
      } else {
        // 主键在删除集合中，过滤掉
        filtered_count++;
        logger.Debug("[Hybrid] Filtered deleted record [pk=" + result.id + "]");
      }
    }

    logger.Info("[Hybrid] Filter completed [filtered_out=" +
                std::to_string(filtered_count) +
                ", final_count=" + std::to_string(filtered.size()) + "]");

    return filtered;
  }

  /**
   * @brief 从向量搜索结果中过滤已删除记录
   */
  static std::vector<SearchResult> FilterVectorResults(
      const std::vector<SearchResult>& results,
      const std::unordered_set<std::string>& deleted_primary_keys) {

    std::vector<SearchResult> filtered;
    filtered.reserve(results.size());

    for (const auto& result : results) {
      if (deleted_primary_keys.find(result.id) == deleted_primary_keys.end()) {
        filtered.push_back(result);
      }
    }

    return filtered;
  }

  /**
   * @brief 构建删除集合的统计信息
   */
  struct DeletedStats {
    size_t total_deleted;
    size_t in_vector_results;
    size_t in_text_results;
    size_t in_both;
  };

  static DeletedStats AnalyzeDeletedImpact(
      const std::vector<SearchResult>& vector_results,
      const std::vector<SearchResult>& text_results,
      const std::unordered_set<std::string>& deleted_primary_keys) {

    DeletedStats stats{};
    stats.total_deleted = deleted_primary_keys.size();

    std::unordered_set<std::string> deleted_in_vector;
    std::unordered_set<std::string> deleted_in_text;

    for (const auto& result : vector_results) {
      if (deleted_primary_keys.find(result.id) != deleted_primary_keys.end()) {
        deleted_in_vector.insert(result.id);
      }
    }

    for (const auto& result : text_results) {
      if (deleted_primary_keys.find(result.id) != deleted_primary_keys.end()) {
        deleted_in_text.insert(result.id);
      }
    }

    stats.in_vector_results = deleted_in_vector.size();
    stats.in_text_results = deleted_in_text.size();

    // 计算同时出现在两个结果中的删除记录
    for (const auto& pk : deleted_in_vector) {
      if (deleted_in_text.find(pk) != deleted_in_text.end()) {
        stats.in_both++;
      }
    }

    return stats;
  }

  /**
   * @brief 验证结果中是否包含已删除记录
   *
   * @return true 如果结果干净（无已删除记录），false 如果发现已删除记录
   */
  static bool VerifyNoDeletedRecords(
      const std::vector<SearchResult>& results,
      const std::unordered_set<std::string>& deleted_primary_keys) {

    for (const auto& result : results) {
      if (deleted_primary_keys.find(result.id) != deleted_primary_keys.end()) {
        engine::Logger logger;
        logger.Error("[Hybrid] VERIFICATION FAILED: Found deleted record in results [pk=" +
                    result.id + "]");
        return false;
      }
    }

    return true;
  }
};

} // namespace fulltext
} // namespace server
} // namespace vectordb
