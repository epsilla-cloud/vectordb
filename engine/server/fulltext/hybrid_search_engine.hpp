#pragma once

#include <vector>
#include <string>
#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include "utils/json.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace server {
namespace fulltext {

/**
 * @brief HybridSearchEngine - 混合搜索引擎
 *
 * 结合向量搜索和全文搜索结果，使用 RRF (Reciprocal Rank Fusion) 算法
 */
class HybridSearchEngine {
public:
  /**
   * @brief 搜索结果项
   */
  struct SearchResult {
    std::string id;          // 文档 ID
    double score;            // 相关性分数
    double vector_score;     // 向量搜索分数（可选）
    double text_score;       // 全文搜索分数（可选）
    int vector_rank;         // 向量搜索排名（0 表示未匹配）
    int text_rank;           // 全文搜索排名（0 表示未匹配）
    Json metadata;           // 文档元数据

    SearchResult() : score(0.0), vector_score(0.0), text_score(0.0),
                     vector_rank(0), text_rank(0) {}
  };

  /**
   * @brief 使用 RRF 算法合并搜索结果
   *
   * RRF(d) = sum(1 / (k + rank_i(d)))
   *
   * @param vector_results 向量搜索结果（按相关性降序排列）
   * @param text_results 全文搜索结果（按相关性降序排列）
   * @param k RRF 常数（默认 60，论文推荐值）
   * @param vector_weight 向量搜索权重（0.0-1.0）
   * @param text_weight 全文搜索权重（0.0-1.0）
   * @return 合并后的搜索结果（按最终分数降序）
   */
  static std::vector<SearchResult> MergeResults(
      const std::vector<SearchResult>& vector_results,
      const std::vector<SearchResult>& text_results,
      int k = 60,
      double vector_weight = 0.5,
      double text_weight = 0.5) {

    // 归一化权重
    double total_weight = vector_weight + text_weight;
    if (total_weight > 0) {
      vector_weight /= total_weight;
      text_weight /= total_weight;
    }

    // 构建文档 ID 到排名的映射
    std::unordered_map<std::string, SearchResult> merged;

    // 处理向量搜索结果
    for (size_t i = 0; i < vector_results.size(); ++i) {
      const auto& result = vector_results[i];
      auto& merged_result = merged[result.id];
      merged_result.id = result.id;
      merged_result.vector_rank = i + 1;
      merged_result.vector_score = result.score;
      merged_result.metadata = result.metadata;

      // RRF 分数贡献
      double rrf_contribution = 1.0 / (k + (i + 1));
      merged_result.score += vector_weight * rrf_contribution;
    }

    // 处理全文搜索结果
    for (size_t i = 0; i < text_results.size(); ++i) {
      const auto& result = text_results[i];
      auto& merged_result = merged[result.id];
      merged_result.id = result.id;
      merged_result.text_rank = i + 1;
      merged_result.text_score = result.score;

      // 如果向量搜索没有匹配，补充元数据
      if (merged_result.vector_rank == 0) {
        merged_result.metadata = result.metadata;
      }

      // RRF 分数贡献
      double rrf_contribution = 1.0 / (k + (i + 1));
      merged_result.score += text_weight * rrf_contribution;
    }

    // 转换为向量并排序
    std::vector<SearchResult> final_results;
    final_results.reserve(merged.size());
    for (auto& [id, result] : merged) {
      final_results.push_back(std::move(result));
    }

    // 按最终分数降序排序
    std::sort(final_results.begin(), final_results.end(),
              [](const SearchResult& a, const SearchResult& b) {
                return a.score > b.score;
              });

    return final_results;
  }

  /**
   * @brief 从 JSON 数组解析搜索结果
   */
  static std::vector<SearchResult> ParseResults(const Json& json_results) {
    std::vector<SearchResult> results;

    if (!json_results.IsArray()) {
      return results;
    }

    size_t count = json_results.GetSize();
    results.reserve(count);

    for (size_t i = 0; i < count; ++i) {
      Json item = json_results.GetArrayElement(i);
      SearchResult result;

      if (item.HasMember("id")) {
        result.id = item.GetString("id");
      } else if (item.HasMember("_id")) {
        result.id = item.GetString("_id");
      }

      if (item.HasMember("score")) {
        result.score = item.GetDouble("score");
      } else if (item.HasMember("_score")) {
        result.score = item.GetDouble("_score");
      } else if (item.HasMember("distance")) {
        // 向量搜索通常返回距离，需要转换为相似度分数
        // score = 1 / (1 + distance)
        double distance = item.GetDouble("distance");
        result.score = 1.0 / (1.0 + distance);
      }

      result.metadata = item;
      results.push_back(result);
    }

    return results;
  }

  /**
   * @brief 将结果转换为 JSON
   */
  static Json ResultsToJson(const std::vector<SearchResult>& results, size_t limit = 10) {
    Json json_results;

    size_t count = std::min(results.size(), limit);
    for (size_t i = 0; i < count; ++i) {
      const auto& result = results[i];

      Json item;
      item.SetString("id", result.id);
      item.SetDouble("score", result.score);

      if (result.vector_rank > 0) {
        item.SetInt("vector_rank", result.vector_rank);
        item.SetDouble("vector_score", result.vector_score);
      }

      if (result.text_rank > 0) {
        item.SetInt("text_rank", result.text_rank);
        item.SetDouble("text_score", result.text_score);
      }

      // 添加元数据
      if (result.metadata.IsObject()) {
        item.SetObject("metadata", result.metadata);
      }

      json_results.AddObjectToArray(item);
    }

    return json_results;
  }

  /**
   * @brief 计算结果集的多样性（去重）
   *
   * 移除完全重复的结果
   */
  static std::vector<SearchResult> Deduplicate(const std::vector<SearchResult>& results) {
    std::vector<SearchResult> deduped;
    std::unordered_set<std::string> seen_ids;

    for (const auto& result : results) {
      if (seen_ids.find(result.id) == seen_ids.end()) {
        deduped.push_back(result);
        seen_ids.insert(result.id);
      }
    }

    return deduped;
  }
};

} // namespace fulltext
} // namespace server
} // namespace vectordb
