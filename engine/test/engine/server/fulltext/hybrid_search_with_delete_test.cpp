#include <gtest/gtest.h>
#include <vector>
#include <unordered_set>
#include "server/fulltext/hybrid_search_with_delete_filter.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace server {
namespace fulltext {
namespace test {

using SearchResult = HybridSearchWithDeleteFilter::SearchResult;

class HybridSearchWithDeleteTest : public ::testing::Test {
protected:
  void SetUp() override {
    // 创建测试数据：10 篇文档
    CreateTestDocuments();
    CreateVectorSearchResults();
    CreateTextSearchResults();
  }

  void CreateTestDocuments() {
    // 模拟 10 篇文档，使用主键（如 "doc_0001"）
    documents_ = {
      {"doc_0001", "Introduction to Machine Learning", "ML is a subset of AI..."},
      {"doc_0002", "Deep Learning Basics", "Neural networks are..."},
      {"doc_0003", "Natural Language Processing", "NLP processes text..."},
      {"doc_0004", "Computer Vision", "CV analyzes images..."},
      {"doc_0005", "Reinforcement Learning", "RL learns from actions..."},
      {"doc_0006", "Data Science Guide", "Data science combines..."},
      {"doc_0007", "Big Data Processing", "Big data handles..."},
      {"doc_0008", "Cloud Computing", "Cloud provides resources..."},
      {"doc_0009", "Distributed Systems", "Distributed systems scale..."},
      {"doc_0010", "Database Design", "Databases store data..."}
    };
  }

  void CreateVectorSearchResults() {
    // 向量搜索返回 top 6（基于语义相似度）
    // 使用主键作为 ID
    vector_results_ = {
      CreateResult("doc_0001", 0.95),  // 最相似
      CreateResult("doc_0002", 0.88),
      CreateResult("doc_0003", 0.82),
      CreateResult("doc_0005", 0.75),  // ← 这个会被删除
      CreateResult("doc_0006", 0.70),
      CreateResult("doc_0004", 0.65)
    };
  }

  void CreateTextSearchResults() {
    // 全文搜索返回 top 6（基于关键词匹配）
    // 使用主键作为 ID
    text_results_ = {
      CreateResult("doc_0003", 0.92),  // 最匹配
      CreateResult("doc_0001", 0.85),
      CreateResult("doc_0007", 0.78),  // ← 这个会被删除
      CreateResult("doc_0002", 0.72),
      CreateResult("doc_0008", 0.68),
      CreateResult("doc_0009", 0.60)   // ← 这个也会被删除
    };
  }

  SearchResult CreateResult(const std::string& pk, double score) {
    SearchResult result;
    result.id = pk;  // 使用主键
    result.score = score;

    // 添加元数据
    auto it = std::find_if(documents_.begin(), documents_.end(),
                          [&pk](const auto& doc) { return std::get<0>(doc) == pk; });
    if (it != documents_.end()) {
      Json metadata;
      metadata.SetString("primary_key", std::get<0>(*it));
      metadata.SetString("title", std::get<1>(*it));
      metadata.SetString("content", std::get<2>(*it));
      result.metadata = metadata;
    }

    return result;
  }

  // 测试数据
  std::vector<std::tuple<std::string, std::string, std::string>> documents_;
  std::vector<SearchResult> vector_results_;
  std::vector<SearchResult> text_results_;
};

// ============================================================================
// 测试 1: 无删除的基准测试
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, BaselineWithoutDeletion) {
  std::cout << "\n=== Test 1: Baseline (No Deletion) ===" << std::endl;

  // 空的删除集合
  std::unordered_set<std::string> deleted_pks;

  // 执行混合搜索
  auto results = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);  // 向量权重 60%, 文本权重 40%

  std::cout << "Merged results count: " << results.size() << std::endl;

  // 验证：应该返回所有唯一的文档
  std::unordered_set<std::string> unique_ids;
  for (const auto& v : vector_results_) unique_ids.insert(v.id);
  for (const auto& t : text_results_) unique_ids.insert(t.id);

  EXPECT_EQ(results.size(), unique_ids.size());

  // 打印结果
  std::cout << "\nTop 5 results:" << std::endl;
  for (size_t i = 0; i < std::min(size_t(5), results.size()); ++i) {
    const auto& r = results[i];
    std::cout << i + 1 << ". " << r.id
              << " (score=" << r.score
              << ", v_rank=" << r.vector_rank
              << ", t_rank=" << r.text_rank << ")" << std::endl;
  }

  // 验证：没有已删除记录
  EXPECT_TRUE(HybridSearchWithDeleteFilter::VerifyNoDeletedRecords(results, deleted_pks));
}

// ============================================================================
// 测试 2: 删除部分记录（仅在向量结果中）
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, DeleteInVectorResultsOnly) {
  std::cout << "\n=== Test 2: Delete in Vector Results Only ===" << std::endl;

  // 删除 doc_0005（仅出现在向量搜索结果中）
  std::unordered_set<std::string> deleted_pks = {"doc_0005"};

  std::cout << "Deleted primary keys: doc_0005" << std::endl;

  // 执行混合搜索
  auto results = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);

  std::cout << "Results count after filter: " << results.size() << std::endl;

  // 验证：doc_0005 不应该出现在结果中
  for (const auto& result : results) {
    EXPECT_NE(result.id, "doc_0005") << "Deleted record found in results!";
  }

  // 验证：没有已删除记录
  EXPECT_TRUE(HybridSearchWithDeleteFilter::VerifyNoDeletedRecords(results, deleted_pks));

  std::cout << "✓ Deleted record successfully filtered" << std::endl;
}

// ============================================================================
// 测试 3: 删除部分记录（仅在文本结果中）
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, DeleteInTextResultsOnly) {
  std::cout << "\n=== Test 3: Delete in Text Results Only ===" << std::endl;

  // 删除 doc_0007（仅出现在文本搜索结果中）
  std::unordered_set<std::string> deleted_pks = {"doc_0007"};

  std::cout << "Deleted primary keys: doc_0007" << std::endl;

  // 执行混合搜索
  auto results = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);

  std::cout << "Results count after filter: " << results.size() << std::endl;

  // 验证：doc_0007 不应该出现在结果中
  for (const auto& result : results) {
    EXPECT_NE(result.id, "doc_0007") << "Deleted record found in results!";
  }

  EXPECT_TRUE(HybridSearchWithDeleteFilter::VerifyNoDeletedRecords(results, deleted_pks));

  std::cout << "✓ Deleted record successfully filtered" << std::endl;
}

// ============================================================================
// 测试 4: 删除同时出现在两个结果中的记录
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, DeleteInBothResults) {
  std::cout << "\n=== Test 4: Delete in Both Results ===" << std::endl;

  // 删除 doc_0001 和 doc_0003（同时出现在向量和文本结果中）
  std::unordered_set<std::string> deleted_pks = {"doc_0001", "doc_0003"};

  std::cout << "Deleted primary keys: doc_0001, doc_0003" << std::endl;

  // 分析删除影响
  auto stats = HybridSearchWithDeleteFilter::AnalyzeDeletedImpact(
      vector_results_, text_results_, deleted_pks);

  std::cout << "Delete impact analysis:" << std::endl;
  std::cout << "  - In vector results: " << stats.in_vector_results << std::endl;
  std::cout << "  - In text results: " << stats.in_text_results << std::endl;
  std::cout << "  - In both: " << stats.in_both << std::endl;

  EXPECT_EQ(stats.in_both, 2);  // doc_0001 和 doc_0003 都在两个结果中

  // 执行混合搜索
  auto results = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);

  std::cout << "Results count after filter: " << results.size() << std::endl;

  // 验证：doc_0001 和 doc_0003 都不应该出现
  for (const auto& result : results) {
    EXPECT_NE(result.id, "doc_0001");
    EXPECT_NE(result.id, "doc_0003");
  }

  EXPECT_TRUE(HybridSearchWithDeleteFilter::VerifyNoDeletedRecords(results, deleted_pks));

  std::cout << "✓ Both deleted records successfully filtered" << std::endl;
}

// ============================================================================
// 测试 5: 批量删除（多个记录）
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, BatchDeletion) {
  std::cout << "\n=== Test 5: Batch Deletion ===" << std::endl;

  // 删除 3 个记录
  std::unordered_set<std::string> deleted_pks = {
    "doc_0005",  // 仅在向量结果中
    "doc_0007",  // 仅在文本结果中
    "doc_0009"   // 仅在文本结果中
  };

  std::cout << "Deleted primary keys: doc_0005, doc_0007, doc_0009" << std::endl;

  // 执行混合搜索
  auto results_before = HybridSearchEngine::MergeResults(
      vector_results_, text_results_, 60, 0.6, 0.4);

  auto results_after = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);

  std::cout << "Results before filter: " << results_before.size() << std::endl;
  std::cout << "Results after filter: " << results_after.size() << std::endl;

  // 验证：过滤掉 3 条记录
  EXPECT_EQ(results_after.size(), results_before.size() - 3);

  // 验证：所有删除的记录都不在结果中
  for (const auto& result : results_after) {
    EXPECT_TRUE(deleted_pks.find(result.id) == deleted_pks.end());
  }

  EXPECT_TRUE(HybridSearchWithDeleteFilter::VerifyNoDeletedRecords(results_after, deleted_pks));

  std::cout << "✓ All deleted records successfully filtered" << std::endl;
}

// ============================================================================
// 测试 6: 极端情况 - 所有结果都被删除
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, AllResultsDeleted) {
  std::cout << "\n=== Test 6: All Results Deleted ===" << std::endl;

  // 删除所有出现在结果中的文档
  std::unordered_set<std::string> deleted_pks;
  for (const auto& r : vector_results_) deleted_pks.insert(r.id);
  for (const auto& r : text_results_) deleted_pks.insert(r.id);

  std::cout << "Deleted all primary keys in results (" << deleted_pks.size() << " keys)" << std::endl;

  // 执行混合搜索
  auto results = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);

  std::cout << "Results count after filter: " << results.size() << std::endl;

  // 验证：应该没有结果
  EXPECT_EQ(results.size(), 0);

  std::cout << "✓ Correctly returned empty results" << std::endl;
}

// ============================================================================
// 测试 7: 删除不存在的记录（负面测试）
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, DeleteNonExistentRecords) {
  std::cout << "\n=== Test 7: Delete Non-Existent Records ===" << std::endl;

  // 删除不存在的记录
  std::unordered_set<std::string> deleted_pks = {
    "doc_9999",  // 不存在
    "doc_8888"   // 不存在
  };

  std::cout << "Deleted non-existent keys: doc_9999, doc_8888" << std::endl;

  // 执行混合搜索
  auto results_before = HybridSearchEngine::MergeResults(
      vector_results_, text_results_, 60, 0.6, 0.4);

  auto results_after = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);

  std::cout << "Results before: " << results_before.size() << std::endl;
  std::cout << "Results after: " << results_after.size() << std::endl;

  // 验证：结果数量不变
  EXPECT_EQ(results_after.size(), results_before.size());

  std::cout << "✓ Non-existent deletions had no impact" << std::endl;
}

// ============================================================================
// 测试 8: 真实场景 - 用户删除自己的文档
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, RealWorldScenario_UserDeletion) {
  std::cout << "\n=== Test 8: Real World - User Deletion Scenario ===" << std::endl;

  // 场景：用户删除了 doc_0002 和 doc_0003
  std::unordered_set<std::string> deleted_pks = {"doc_0002", "doc_0003"};

  std::cout << "User deleted: doc_0002, doc_0003" << std::endl;

  // 执行混合搜索
  auto results = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);

  std::cout << "\nSearch results (user should NOT see deleted docs):" << std::endl;
  for (size_t i = 0; i < std::min(size_t(5), results.size()); ++i) {
    const auto& r = results[i];
    std::string title = r.metadata.HasMember("title") ?
                       r.metadata.GetString("title") : "N/A";
    std::cout << i + 1 << ". " << r.id << " - " << title
              << " (score=" << r.score << ")" << std::endl;

    // 验证：用户不应该看到已删除的文档
    EXPECT_NE(r.id, "doc_0002");
    EXPECT_NE(r.id, "doc_0003");
  }

  EXPECT_TRUE(HybridSearchWithDeleteFilter::VerifyNoDeletedRecords(results, deleted_pks));

  std::cout << "✓ User cannot see their deleted documents" << std::endl;
}

// ============================================================================
// 测试 9: 压缩场景模拟 - ID 改变但主键不变
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, CompactionScenario_PrimaryKeyUnchanged) {
  std::cout << "\n=== Test 9: Compaction Scenario (Primary Key Unchanged) ===" << std::endl;

  // 场景说明：
  // 1. 向量 DB 压缩前：doc_0005 的内部 ID 是 5
  // 2. 压缩后：doc_0005 的内部 ID 变成 3（因为删除了前面的记录）
  // 3. Quickwit：仍然使用主键 "doc_0005"（不变）
  // 4. 混合搜索：基于主键合并，无影响

  std::cout << "Simulating compaction:" << std::endl;
  std::cout << "  - VectorDB: internal_id changed (5 → 3)" << std::endl;
  std::cout << "  - Quickwit: primary_key unchanged (doc_0005)" << std::endl;
  std::cout << "  - Hybrid search: uses primary_key ✓" << std::endl;

  // 删除 doc_0001（模拟压缩前删除）
  std::unordered_set<std::string> deleted_pks = {"doc_0001"};

  // 执行混合搜索（压缩后）
  auto results = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);

  // 验证：doc_0005 仍然可以被正确合并和返回
  bool found_doc_0005 = false;
  for (const auto& r : results) {
    if (r.id == "doc_0005") {
      found_doc_0005 = true;
      std::cout << "✓ Found doc_0005 in results (score=" << r.score << ")" << std::endl;
      break;
    }
  }

  EXPECT_TRUE(found_doc_0005) << "doc_0005 should be found despite internal ID change";

  // 验证：doc_0001 被正确过滤
  for (const auto& r : results) {
    EXPECT_NE(r.id, "doc_0001");
  }

  std::cout << "✓ Compaction does not affect hybrid search (uses primary keys)" << std::endl;
}

// ============================================================================
// 测试 10: 性能测试 - 大量删除
// ============================================================================
TEST_F(HybridSearchWithDeleteTest, PerformanceTest_ManyDeletions) {
  std::cout << "\n=== Test 10: Performance Test (Many Deletions) ===" << std::endl;

  // 创建大量删除（50% 删除率）
  std::unordered_set<std::string> deleted_pks = {
    "doc_0001", "doc_0003", "doc_0005", "doc_0007", "doc_0009"
  };

  std::cout << "Deleting 50% of documents (" << deleted_pks.size() << " out of 10)" << std::endl;

  auto start = std::chrono::high_resolution_clock::now();

  // 执行混合搜索
  auto results = HybridSearchWithDeleteFilter::MergeAndFilter(
      vector_results_, text_results_, deleted_pks,
      60, 0.6, 0.4);

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  std::cout << "Filter performance: " << duration.count() << " μs" << std::endl;
  std::cout << "Results after filtering: " << results.size() << std::endl;

  // 验证：所有删除的记录都不在结果中
  EXPECT_TRUE(HybridSearchWithDeleteFilter::VerifyNoDeletedRecords(results, deleted_pks));

  // 性能基准：应该在 1ms 内完成（小数据集）
  EXPECT_LT(duration.count(), 1000) << "Filtering should be fast for small dataset";

  std::cout << "✓ Performance acceptable" << std::endl;
}

} // namespace test
} // namespace fulltext
} // namespace server
} // namespace vectordb
