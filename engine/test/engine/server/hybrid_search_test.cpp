#include <gtest/gtest.h>
#include <vector>
#include "server/fulltext/hybrid_search_engine.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace server {
namespace test {

using HybridSearchEngine = vectordb::server::fulltext::HybridSearchEngine;
using SearchResult = HybridSearchEngine::SearchResult;

class HybridSearchTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create sample vector search results
    CreateSampleVectorResults();
    CreateSampleTextResults();
  }

  void CreateSampleVectorResults() {
    // Simulate vector search results: documents about "machine learning"
    SearchResult r1;
    r1.id = "doc1";
    r1.score = 0.95;  // High similarity
    vectordb::Json meta1;
    meta1.SetString("title", "Introduction to Machine Learning");
    meta1.SetString("content", "Machine learning is a subset of artificial intelligence...");
    r1.metadata = meta1;
    vector_results_.push_back(r1);

    SearchResult r2;
    r2.id = "doc3";
    r2.score = 0.85;
    vectordb::Json meta2;
    meta2.SetString("title", "Deep Learning Fundamentals");
    meta2.SetString("content", "Deep learning uses neural networks...");
    r2.metadata = meta2;
    vector_results_.push_back(r2);

    SearchResult r3;
    r3.id = "doc5";
    r3.score = 0.75;
    vectordb::Json meta3;
    meta3.SetString("title", "Natural Language Processing");
    meta3.SetString("content", "NLP is a branch of AI...");
    r3.metadata = meta3;
    vector_results_.push_back(r3);
  }

  void CreateSampleTextResults() {
    // Simulate text search results: keyword matching
    SearchResult r1;
    r1.id = "doc2";
    r1.score = 0.90;  // High relevance
    vectordb::Json meta1;
    meta1.SetString("title", "Machine Learning Tutorial");
    meta1.SetString("content", "This tutorial covers machine learning basics...");
    r1.metadata = meta1;
    text_results_.push_back(r1);

    SearchResult r2;
    r2.id = "doc1";  // Also in vector results
    r2.score = 0.80;
    vectordb::Json meta2;
    meta2.SetString("title", "Introduction to Machine Learning");
    meta2.SetString("content", "Machine learning is a subset of artificial intelligence...");
    r2.metadata = meta2;
    text_results_.push_back(r2);

    SearchResult r3;
    r3.id = "doc4";
    r3.score = 0.70;
    vectordb::Json meta3;
    meta3.SetString("title", "Supervised Learning Methods");
    meta3.SetString("content", "Supervised learning requires labeled data...");
    r3.metadata = meta3;
    text_results_.push_back(r3);
  }

  std::vector<SearchResult> vector_results_;
  std::vector<SearchResult> text_results_;
};

// Test 1: RRF Merge with Equal Weights
TEST_F(HybridSearchTest, RRFMergeEqualWeights) {
  auto merged = HybridSearchEngine::MergeResults(
      vector_results_, text_results_, 60, 0.5, 0.5);

  // Should have 5 unique documents (3 vector + 3 text - 1 overlap)
  EXPECT_EQ(merged.size(), 5);

  // doc1 appears in both results, should have highest score
  EXPECT_EQ(merged[0].id, "doc1");
  EXPECT_GT(merged[0].score, 0.0);
  EXPECT_GT(merged[0].vector_rank, 0);
  EXPECT_GT(merged[0].text_rank, 0);

  // Results should be sorted by score descending
  for (size_t i = 1; i < merged.size(); ++i) {
    EXPECT_GE(merged[i - 1].score, merged[i].score);
  }
}

// Test 2: RRF Merge with Vector Bias
TEST_F(HybridSearchTest, RRFMergeVectorBias) {
  // Heavy weight on vector search
  auto merged = HybridSearchEngine::MergeResults(
      vector_results_, text_results_, 60, 0.9, 0.1);

  EXPECT_EQ(merged.size(), 5);

  // Top result should be from vector search
  // doc1 has rank 1 in vector, rank 2 in text
  // doc2 has rank 1 in text only
  // With 90% vector weight, doc1 should still be on top
  bool doc1_in_top3 = false;
  for (size_t i = 0; i < 3 && i < merged.size(); ++i) {
    if (merged[i].id == "doc1") {
      doc1_in_top3 = true;
      break;
    }
  }
  EXPECT_TRUE(doc1_in_top3);
}

// Test 3: RRF Merge with Text Bias
TEST_F(HybridSearchTest, RRFMergeTextBias) {
  // Heavy weight on text search
  auto merged = HybridSearchEngine::MergeResults(
      vector_results_, text_results_, 60, 0.1, 0.9);

  EXPECT_EQ(merged.size(), 5);

  // doc2 has rank 1 in text only
  // With 90% text weight, it should be in top positions
  bool doc2_in_top3 = false;
  for (size_t i = 0; i < 3 && i < merged.size(); ++i) {
    if (merged[i].id == "doc2") {
      doc2_in_top3 = true;
      break;
    }
  }
  EXPECT_TRUE(doc2_in_top3);
}

// Test 4: Empty Vector Results
TEST_F(HybridSearchTest, EmptyVectorResults) {
  std::vector<SearchResult> empty_vector;

  auto merged = HybridSearchEngine::MergeResults(
      empty_vector, text_results_, 60, 0.5, 0.5);

  // Should have all text results
  EXPECT_EQ(merged.size(), text_results_.size());
  EXPECT_EQ(merged[0].id, text_results_[0].id);
}

// Test 5: Empty Text Results
TEST_F(HybridSearchTest, EmptyTextResults) {
  std::vector<SearchResult> empty_text;

  auto merged = HybridSearchEngine::MergeResults(
      vector_results_, empty_text, 60, 0.5, 0.5);

  // Should have all vector results
  EXPECT_EQ(merged.size(), vector_results_.size());
  EXPECT_EQ(merged[0].id, vector_results_[0].id);
}

// Test 6: Both Empty
TEST_F(HybridSearchTest, BothEmpty) {
  std::vector<SearchResult> empty_vector;
  std::vector<SearchResult> empty_text;

  auto merged = HybridSearchEngine::MergeResults(
      empty_vector, empty_text, 60, 0.5, 0.5);

  EXPECT_EQ(merged.size(), 0);
}

// Test 7: Different K Values
TEST_F(HybridSearchTest, DifferentKValues) {
  auto merged_k10 = HybridSearchEngine::MergeResults(
      vector_results_, text_results_, 10, 0.5, 0.5);

  auto merged_k100 = HybridSearchEngine::MergeResults(
      vector_results_, text_results_, 100, 0.5, 0.5);

  // Both should have same number of results
  EXPECT_EQ(merged_k10.size(), merged_k100.size());

  // But scores should be different (k affects RRF calculation)
  EXPECT_NE(merged_k10[0].score, merged_k100[0].score);
}

// Test 8: JSON Conversion
TEST_F(HybridSearchTest, JSONConversion) {
  auto merged = HybridSearchEngine::MergeResults(
      vector_results_, text_results_, 60, 0.5, 0.5);

  auto json_result = HybridSearchEngine::ResultsToJson(merged, 10);

  EXPECT_TRUE(json_result.IsArray());
  EXPECT_LE(json_result.GetSize(), 10);

  // Check first result has required fields
  if (json_result.GetSize() > 0) {
    auto first = json_result.GetArrayElement(0);
    EXPECT_TRUE(first.HasMember("id"));
    EXPECT_TRUE(first.HasMember("score"));
  }
}

// Test 9: Deduplication
TEST_F(HybridSearchTest, Deduplication) {
  std::vector<SearchResult> results_with_duplicates;

  SearchResult r1;
  r1.id = "doc1";
  r1.score = 0.9;
  results_with_duplicates.push_back(r1);

  SearchResult r2;
  r2.id = "doc2";
  r2.score = 0.8;
  results_with_duplicates.push_back(r2);

  SearchResult r3;
  r3.id = "doc1";  // Duplicate
  r3.score = 0.7;
  results_with_duplicates.push_back(r3);

  auto deduped = HybridSearchEngine::Deduplicate(results_with_duplicates);

  EXPECT_EQ(deduped.size(), 2);
  EXPECT_EQ(deduped[0].id, "doc1");
  EXPECT_EQ(deduped[1].id, "doc2");
  EXPECT_DOUBLE_EQ(deduped[0].score, 0.9);  // Should keep first occurrence
}

// Test 10: Parse JSON Results
TEST_F(HybridSearchTest, ParseJSONResults) {
  vectordb::Json json_array;

  vectordb::Json item1;
  item1.SetString("id", "doc1");
  item1.SetDouble("score", 0.95);
  json_array.AddObjectToArray(item1);

  vectordb::Json item2;
  item2.SetString("_id", "doc2");  // Alternative ID field
  item2.SetDouble("_score", 0.85);  // Alternative score field
  json_array.AddObjectToArray(item2);

  vectordb::Json item3;
  item3.SetString("id", "doc3");
  item3.SetDouble("distance", 0.5);  // Distance instead of score
  json_array.AddObjectToArray(item3);

  auto parsed = HybridSearchEngine::ParseResults(json_array);

  EXPECT_EQ(parsed.size(), 3);
  EXPECT_EQ(parsed[0].id, "doc1");
  EXPECT_DOUBLE_EQ(parsed[0].score, 0.95);
  EXPECT_EQ(parsed[1].id, "doc2");
  EXPECT_DOUBLE_EQ(parsed[1].score, 0.85);
  EXPECT_EQ(parsed[2].id, "doc3");
  // score = 1 / (1 + distance) = 1 / 1.5 = 0.666...
  EXPECT_NEAR(parsed[2].score, 0.6667, 0.001);
}

// Test 11: RRF Score Calculation Verification
TEST_F(HybridSearchTest, RRFScoreCalculation) {
  // Single result in each list to verify exact RRF calculation
  std::vector<SearchResult> vec_only;
  SearchResult v1;
  v1.id = "doc1";
  v1.score = 1.0;
  vec_only.push_back(v1);

  std::vector<SearchResult> txt_only;
  SearchResult t1;
  t1.id = "doc1";
  t1.score = 1.0;
  txt_only.push_back(t1);

  int k = 60;
  double vec_weight = 0.5;
  double txt_weight = 0.5;

  auto merged = HybridSearchEngine::MergeResults(
      vec_only, txt_only, k, vec_weight, txt_weight);

  EXPECT_EQ(merged.size(), 1);

  // RRF score = vec_weight * (1/(k+1)) + txt_weight * (1/(k+1))
  // = 0.5 * (1/61) + 0.5 * (1/61) = 1/61
  double expected_score = (vec_weight + txt_weight) / (k + 1);
  EXPECT_NEAR(merged[0].score, expected_score, 0.0001);
}

// Test 12: Limit Results in JSON Conversion
TEST_F(HybridSearchTest, LimitJSONResults) {
  auto merged = HybridSearchEngine::MergeResults(
      vector_results_, text_results_, 60, 0.5, 0.5);

  // Request only top 3 results
  auto json_result = HybridSearchEngine::ResultsToJson(merged, 3);

  EXPECT_TRUE(json_result.IsArray());
  EXPECT_EQ(json_result.GetSize(), 3);
}

} // namespace test
} // namespace server
} // namespace vectordb
