#include "db/index/index_selector.hpp"
#include <algorithm>
#include <cmath>

namespace vectordb {
namespace engine {
namespace index {

// Strategy weight configurations
const std::unordered_map<IndexSelectionStrategy, IndexSelector::ScoringWeights> 
IndexSelector::strategy_weights_ = {
  {IndexSelectionStrategy::PERFORMANCE_FIRST, {
    .performance_weight = 0.5,
    .memory_weight = 0.1,
    .accuracy_weight = 0.3,
    .build_time_weight = 0.1
  }},
  {IndexSelectionStrategy::MEMORY_FIRST, {
    .performance_weight = 0.1,
    .memory_weight = 0.5,
    .accuracy_weight = 0.2,
    .build_time_weight = 0.2
  }},
  {IndexSelectionStrategy::BALANCED, {
    .performance_weight = 0.25,
    .memory_weight = 0.25,
    .accuracy_weight = 0.25,
    .build_time_weight = 0.25
  }},
  {IndexSelectionStrategy::CUSTOM, {
    .performance_weight = 0.25,
    .memory_weight = 0.25,
    .accuracy_weight = 0.25,
    .build_time_weight = 0.25
  }}
};

IndexType IndexSelector::SelectBestIndex(const SelectionCriteria& criteria) {
  // Apply heuristic rules first
  IndexType heuristic_result = ApplyHeuristicRules(criteria);
  if (heuristic_result != IndexType::AUTO) {
    return heuristic_result;
  }

  // Score all index types
  std::vector<SelectionScore> scores = AnalyzeAllIndexTypes(criteria);
  
  // Filter out indexes that don't meet hard constraints
  scores.erase(std::remove_if(scores.begin(), scores.end(),
    [&criteria](const SelectionScore& score) {
      return !MeetsConstraints(score.index_type, criteria);
    }), scores.end());

  if (scores.empty()) {
    // Fallback to FLAT if no index meets constraints
    return IndexType::FLAT;
  }

  // Sort by total score (highest first)
  std::sort(scores.begin(), scores.end(),
    [](const SelectionScore& a, const SelectionScore& b) {
      return a.total_score > b.total_score;
    });

  return scores[0].index_type;
}

std::vector<SelectionScore> IndexSelector::AnalyzeAllIndexTypes(const SelectionCriteria& criteria) {
  std::vector<SelectionScore> scores;
  
  // Score each available index type
  std::vector<IndexType> index_types = {
    IndexType::NSG,
    IndexType::IVF,
    IndexType::PQ,
    IndexType::IVF_PQ,
    IndexType::FLAT
  };

  for (IndexType type : index_types) {
    SelectionScore score = ScoreIndexType(type, criteria);
    scores.push_back(score);
  }

  return scores;
}

std::unique_ptr<IndexBuildParams> IndexSelector::GetRecommendedParams(
    IndexType index_type, 
    const SelectionCriteria& criteria) {
  
  switch (index_type) {
    case IndexType::NSG: {
      auto params = std::make_unique<NSGBuildParams>();
      params->metric_type = criteria.metric_type;
      params->selection_strategy = criteria.strategy;
      params->target_accuracy = criteria.target_accuracy;
      
      // Adjust NSG parameters based on data size
      if (criteria.vector_count < 10000) {
        params->search_length = 20;
        params->out_degree = 30;
        params->candidate_pool_size = 100;
      } else if (criteria.vector_count < 100000) {
        params->search_length = 45;  // Default
        params->out_degree = 50;
        params->candidate_pool_size = 300;
      } else {
        params->search_length = 60;
        params->out_degree = 70;
        params->candidate_pool_size = 500;
      }
      
      return std::move(params);
    }
    
    case IndexType::IVF: {
      auto params = std::make_unique<IVFBuildParams>();
      params->metric_type = criteria.metric_type;
      params->selection_strategy = criteria.strategy;
      params->target_accuracy = criteria.target_accuracy;
      
      // Calculate optimal nlist based on data size (rule of thumb: sqrt(n))
      params->nlist = static_cast<size_t>(std::sqrt(criteria.vector_count));
      params->nlist = std::max(params->nlist, size_t(256));  // Minimum 256 clusters
      params->nlist = std::min(params->nlist, size_t(65536)); // Maximum 64k clusters
      
      // Set nprobe based on accuracy requirements
      if (criteria.target_accuracy > 0.98) {
        params->nprobe = params->nlist / 4;  // Search 25% of clusters
      } else if (criteria.target_accuracy > 0.95) {
        params->nprobe = params->nlist / 8;  // Search 12.5% of clusters
      } else {
        params->nprobe = params->nlist / 16; // Search 6.25% of clusters
      }
      params->nprobe = std::max(params->nprobe, size_t(32)); // Minimum 32
      
      return std::move(params);
    }
    
    case IndexType::PQ: {
      auto params = std::make_unique<PQBuildParams>();
      params->metric_type = criteria.metric_type;
      params->selection_strategy = criteria.strategy;
      params->target_accuracy = criteria.target_accuracy;
      
      // Choose m based on dimension (typically 8-16 subquantizers)
      params->m = std::min(size_t(16), std::max(size_t(4), criteria.dimension / 16));
      
      // Ensure dimension is divisible by m
      while (criteria.dimension % params->m != 0 && params->m > 4) {
        params->m--;
      }
      
      // Use 8 bits per subquantizer for good balance
      params->nbits = 8;
      
      return std::move(params);
    }
    
    default: {
      // Return basic params for other types
      auto params = std::make_unique<IndexBuildParams>();
      params->index_type = index_type;
      params->metric_type = criteria.metric_type;
      params->selection_strategy = criteria.strategy;
      params->target_accuracy = criteria.target_accuracy;
      return std::move(params);
    }
  }
}

SelectionScore IndexSelector::ScoreIndexType(IndexType index_type, 
                                           const SelectionCriteria& criteria) {
  SelectionScore score;
  score.index_type = index_type;
  
  // Calculate individual scores
  score.performance_score = CalculatePerformanceScore(index_type, criteria);
  score.memory_score = CalculateMemoryScore(index_type, criteria);
  score.accuracy_score = CalculateAccuracyScore(index_type, criteria);
  score.build_time_score = CalculateBuildTimeScore(index_type, criteria);
  
  // Get weights for the strategy
  auto weights_it = strategy_weights_.find(criteria.strategy);
  const ScoringWeights& weights = (weights_it != strategy_weights_.end()) ? 
    weights_it->second : strategy_weights_.at(IndexSelectionStrategy::BALANCED);
  
  // Calculate weighted total score
  score.total_score = 
    score.performance_score * weights.performance_weight +
    score.memory_score * weights.memory_weight +
    score.accuracy_score * weights.accuracy_weight +
    score.build_time_score * weights.build_time_weight;
  
  // Generate rationale
  score.rationale = GenerateRationale(index_type, criteria, score);
  
  return score;
}

double IndexSelector::CalculatePerformanceScore(IndexType index_type,
                                               const SelectionCriteria& criteria) {
  IndexCharacteristics chars = GetIndexCharacteristics(index_type);
  
  // Base performance score (inverted - lower complexity factor = higher score)
  double base_score = 1.0 - (chars.search_time_complexity_factor - 1.0) / 9.0;
  
  // Adjust based on data size fit
  if (criteria.vector_count < chars.min_recommended_vectors) {
    base_score *= 0.7; // Penalty for using on too small dataset
  } else if (criteria.vector_count > chars.max_recommended_vectors) {
    base_score *= 0.5; // Larger penalty for too large dataset
  }
  
  return std::max(0.0, std::min(1.0, base_score));
}

double IndexSelector::CalculateMemoryScore(IndexType index_type,
                                          const SelectionCriteria& criteria) {
  size_t estimated_memory = EstimateMemoryUsage(index_type, 
                                               criteria.vector_count,
                                               criteria.dimension);
  
  if (criteria.available_memory_mb == 0) {
    // No memory constraint - score based on efficiency
    IndexCharacteristics chars = GetIndexCharacteristics(index_type);
    return 1.0 - (chars.memory_usage_factor - 1.0) / 9.0;
  }
  
  size_t available_bytes = criteria.available_memory_mb * 1024 * 1024;
  
  if (estimated_memory > available_bytes) {
    return 0.0; // Doesn't fit in available memory
  }
  
  // Score based on memory utilization (higher is better up to ~80% usage)
  double utilization = static_cast<double>(estimated_memory) / available_bytes;
  
  if (utilization <= 0.8) {
    return utilization / 0.8; // Linear increase up to 80%
  } else {
    return 1.0 - (utilization - 0.8) / 0.2; // Decrease after 80%
  }
}

double IndexSelector::CalculateAccuracyScore(IndexType index_type,
                                            const SelectionCriteria& criteria) {
  IndexCharacteristics chars = GetIndexCharacteristics(index_type);
  
  if (chars.accuracy_factor >= criteria.target_accuracy) {
    return 1.0; // Meets or exceeds target accuracy
  }
  
  // Penalty based on accuracy gap
  double accuracy_gap = criteria.target_accuracy - chars.accuracy_factor;
  return std::max(0.0, 1.0 - accuracy_gap * 2.0); // Double penalty for accuracy loss
}

double IndexSelector::CalculateBuildTimeScore(IndexType index_type,
                                             const SelectionCriteria& criteria) {
  auto estimated_time = EstimateBuildTime(index_type, 
                                         criteria.vector_count,
                                         criteria.dimension);
  
  if (estimated_time > std::chrono::minutes(criteria.max_build_time_minutes)) {
    return 0.0; // Exceeds maximum allowed build time
  }
  
  // Score inversely proportional to build time (faster = higher score)
  double normalized_time = static_cast<double>(estimated_time.count()) / 
                          criteria.max_build_time_minutes;
  
  return std::max(0.0, 1.0 - normalized_time);
}

bool IndexSelector::MeetsConstraints(IndexType index_type,
                                   const SelectionCriteria& criteria) {
  IndexCharacteristics chars = GetIndexCharacteristics(index_type);
  
  // Check accuracy constraint
  if (chars.accuracy_factor < criteria.min_accuracy) {
    return false;
  }
  
  // Check memory constraint
  if (criteria.max_memory_usage_mb > 0) {
    size_t estimated_memory = EstimateMemoryUsage(index_type,
                                                 criteria.vector_count,
                                                 criteria.dimension);
    if (estimated_memory > criteria.max_memory_usage_mb * 1024 * 1024) {
      return false;
    }
  }
  
  // Check build time constraint
  auto estimated_time = EstimateBuildTime(index_type,
                                         criteria.vector_count,
                                         criteria.dimension);
  if (estimated_time > std::chrono::minutes(criteria.max_build_time_minutes)) {
    return false;
  }
  
  return true;
}

IndexType IndexSelector::ApplyHeuristicRules(const SelectionCriteria& criteria) {
  // Very small datasets - use FLAT
  if (criteria.vector_count < 1000) {
    return IndexType::FLAT;
  }
  
  // Very high accuracy requirements with reasonable dataset size
  if (criteria.target_accuracy > 0.99 && criteria.vector_count < 10000000) {
    return IndexType::NSG;
  }
  
  // Very large datasets with memory constraints
  if (criteria.vector_count > 100000000 && criteria.available_memory_mb < 8192) {
    return IndexType::PQ;
  }
  
  // High dimensional data with memory constraints
  if (criteria.dimension > 512 && criteria.available_memory_mb < 4096) {
    return IndexType::PQ;
  }
  
  return IndexType::AUTO; // Continue with scoring
}

std::string IndexSelector::GenerateRationale(IndexType index_type,
                                            const SelectionCriteria& criteria,
                                            const SelectionScore& score) {
  std::string rationale = "Selected " + GetIndexTypeName(index_type) + " because: ";
  
  if (score.performance_score > 0.8) {
    rationale += "excellent search performance, ";
  } else if (score.performance_score > 0.6) {
    rationale += "good search performance, ";
  }
  
  if (score.memory_score > 0.8) {
    rationale += "efficient memory usage, ";
  } else if (score.memory_score > 0.6) {
    rationale += "reasonable memory usage, ";
  }
  
  if (score.accuracy_score > 0.9) {
    rationale += "meets accuracy requirements, ";
  }
  
  if (score.build_time_score > 0.8) {
    rationale += "fast build time";
  } else {
    rationale += "acceptable build time";
  }
  
  return rationale;
}

// Simple estimation functions (can be improved with benchmarking data)
size_t IndexSelector::EstimateMemoryUsage(IndexType index_type,
                                         size_t vector_count,
                                         size_t dimension) {
  IndexCharacteristics chars = GetIndexCharacteristics(index_type);
  
  // Base memory usage per vector (in bytes)
  size_t base_per_vector = dimension * sizeof(float);
  
  // Index overhead factor
  double overhead_factor = chars.memory_usage_factor / 10.0;
  
  return static_cast<size_t>(vector_count * base_per_vector * (1.0 + overhead_factor));
}

std::chrono::minutes IndexSelector::EstimateBuildTime(IndexType index_type,
                                                     size_t vector_count,
                                                     size_t dimension) {
  IndexCharacteristics chars = GetIndexCharacteristics(index_type);
  
  // Very rough estimation based on complexity factor and data size
  double base_seconds = vector_count * dimension * chars.build_time_complexity_factor / 1000000.0;
  
  return std::chrono::minutes(static_cast<int>(base_seconds / 60.0) + 1);
}

size_t IndexSelector::EstimateSearchQPS(IndexType index_type,
                                       size_t vector_count,
                                       size_t dimension,
                                       size_t k) {
  IndexCharacteristics chars = GetIndexCharacteristics(index_type);
  
  // Base QPS (very rough estimation)
  double base_qps = 10000.0 / chars.search_time_complexity_factor;
  
  // Adjust for data size and dimension
  double size_factor = std::log10(vector_count) / 6.0; // Normalize to log scale
  double dim_factor = std::log2(dimension) / 10.0;
  
  return static_cast<size_t>(base_qps / (1.0 + size_factor + dim_factor));
}

} // namespace index
} // namespace engine
} // namespace vectordb