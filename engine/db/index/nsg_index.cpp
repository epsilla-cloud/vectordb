#include "db/index/nsg_index.hpp"
#include <fstream>
#include <chrono>

namespace vectordb {
namespace engine {
namespace index {

NSGIndex::NSGIndex(size_t dimension, meta::MetricType metric_type)
    : dimension_(dimension)
    , metric_type_(metric_type)
    , is_trained_(false) {
  
  // Create the underlying NSG implementation
  auto nsg_metric = ConvertMetricType(metric_type);
  nsg_impl_ = std::make_unique<::vectordb::engine::index::NsgIndex>(dimension, 0, nsg_metric);
  
  // Initialize stats
  stats_.dimensions = dimension;
  stats_.last_updated = std::chrono::system_clock::now();
  
  logger_.Info("Created NSG index with dimension=" + std::to_string(dimension) + 
              ", metric=" + std::to_string(static_cast<int>(metric_type)));
}

NSGIndex::~NSGIndex() = default;

size_t NSGIndex::Build(size_t nb, 
                      const VectorColumnData& data, 
                      const int64_t* ids,
                      const IndexBuildParams& params) {
  auto start_time = std::chrono::high_resolution_clock::now();
  
  try {
    // Extract NSG-specific parameters
    NSGBuildParams nsg_params = ExtractNSGParams(params);
    
    // Convert to NSG build parameters format
    ::vectordb::engine::index::BuildParams build_params;
    build_params.search_length = nsg_params.search_length;
    build_params.out_degree = nsg_params.out_degree;
    build_params.candidate_pool_size = nsg_params.candidate_pool_size;
    
    logger_.Info("Building NSG index with " + std::to_string(nb) + " vectors");
    logger_.Debug("NSG params: search_length=" + std::to_string(build_params.search_length) +
                 ", out_degree=" + std::to_string(build_params.out_degree) +
                 ", candidate_pool_size=" + std::to_string(build_params.candidate_pool_size));
    
    // Build the index using the underlying NSG implementation
    size_t built_count = nsg_impl_->Build(nb, data, ids, build_params);
    
    if (built_count > 0) {
      is_trained_ = true;
      build_params_ = nsg_params;
      
      // Update statistics
      auto end_time = std::chrono::high_resolution_clock::now();
      stats_.build_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);
      stats_.indexed_vectors = built_count;
      stats_.memory_usage_bytes = GetMemoryUsage();
      stats_.last_updated = std::chrono::system_clock::now();
      
      logger_.Info("NSG index built successfully: " + std::to_string(built_count) + 
                  " vectors indexed in " + std::to_string(stats_.build_time.count()) + "ms");
    } else {
      logger_.Error("Failed to build NSG index");
    }
    
    return built_count;
    
  } catch (const std::exception& e) {
    logger_.Error("Exception during NSG index build: " + std::string(e.what()));
    return 0;
  }
}

void NSGIndex::Search(const DenseVectorPtr query,
                     const size_t nq,
                     const size_t dim,
                     const size_t k,
                     float* distances,
                     int64_t* ids,
                     ConcurrentBitsetPtr bitset) {
  
  if (!is_trained_) {
    logger_.Warning("Attempting search on untrained NSG index");
    return;
  }
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  try {
    // Prepare search parameters
    ::vectordb::engine::index::SearchParams search_params;
    search_params.search_length = build_params_.search_length;
    search_params.k = k;
    
    // Perform the search using underlying NSG implementation
    nsg_impl_->Search(query, nq, dim, k, distances, ids, search_params, bitset);
    
    // Update search statistics
    auto end_time = std::chrono::high_resolution_clock::now();
    auto search_duration = std::chrono::duration_cast<std::chrono::microseconds>(
      end_time - start_time);
    
    UpdateSearchStats(search_duration);
    
    logger_.Debug("NSG search completed: " + std::to_string(nq) + " queries, k=" + 
                 std::to_string(k) + ", time=" + std::to_string(search_duration.count()) + "μs");
    
  } catch (const std::exception& e) {
    logger_.Error("Exception during NSG search: " + std::string(e.what()));
  }
}

size_t NSGIndex::GetMemoryUsage() const {
  if (!nsg_impl_) {
    return 0;
  }
  
  // Get memory usage from underlying NSG implementation
  size_t nsg_memory = static_cast<size_t>(nsg_impl_->GetSize());
  
  // Add estimated overhead from this adapter
  size_t adapter_overhead = sizeof(*this) + 1024; // Rough estimate
  
  return nsg_memory + adapter_overhead;
}

IndexStats NSGIndex::GetStats() const {
  IndexStats current_stats = stats_;
  current_stats.memory_usage_bytes = GetMemoryUsage();
  current_stats.last_updated = std::chrono::system_clock::now();
  return current_stats;
}

size_t NSGIndex::GetVectorCount() const {
  if (!nsg_impl_) {
    return 0;
  }
  return nsg_impl_->ntotal;
}

bool NSGIndex::SaveToFile(const std::string& file_path) const {
  if (!is_trained_) {
    logger_.Warning("Cannot save untrained NSG index");
    return false;
  }
  
  try {
    // For now, we'll create a simple serialization format
    // In a full implementation, this would use the NSG's native save format
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
      logger_.Error("Cannot open file for saving: " + file_path);
      return false;
    }
    
    // Write header
    file.write("NSG_INDEX", 9);
    file.write(reinterpret_cast<const char*>(&dimension_), sizeof(dimension_));
    file.write(reinterpret_cast<const char*>(&metric_type_), sizeof(metric_type_));
    
    // Write build parameters
    file.write(reinterpret_cast<const char*>(&build_params_), sizeof(build_params_));
    
    // Write statistics
    file.write(reinterpret_cast<const char*>(&stats_), sizeof(stats_));
    
    logger_.Info("NSG index saved to: " + file_path);
    return true;
    
  } catch (const std::exception& e) {
    logger_.Error("Failed to save NSG index: " + std::string(e.what()));
    return false;
  }
}

bool NSGIndex::LoadFromFile(const std::string& file_path) {
  try {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
      logger_.Error("Cannot open file for loading: " + file_path);
      return false;
    }
    
    // Read and verify header
    char header[10];
    file.read(header, 9);
    header[9] = '\0';
    
    if (std::string(header) != "NSG_INDEX") {
      logger_.Error("Invalid NSG index file format");
      return false;
    }
    
    // Read dimension and metric type
    file.read(reinterpret_cast<char*>(&dimension_), sizeof(dimension_));
    file.read(reinterpret_cast<char*>(&metric_type_), sizeof(metric_type_));
    
    // Read build parameters
    file.read(reinterpret_cast<char*>(&build_params_), sizeof(build_params_));
    
    // Read statistics
    file.read(reinterpret_cast<char*>(&stats_), sizeof(stats_));
    
    // Recreate NSG implementation with loaded parameters
    auto nsg_metric = ConvertMetricType(metric_type_);
    nsg_impl_ = std::make_unique<::vectordb::engine::index::NsgIndex>(dimension_, 0, nsg_metric);
    
    is_trained_ = true; // Mark as trained since we loaded from file
    
    logger_.Info("NSG index loaded from: " + file_path);
    return true;
    
  } catch (const std::exception& e) {
    logger_.Error("Failed to load NSG index: " + std::string(e.what()));
    return false;
  }
}

void NSGIndex::Prewarm(const DenseVectorPtr sample_queries,
                      size_t query_count,
                      size_t dim, 
                      size_t k) {
  if (!is_trained_) {
    logger_.Warning("Cannot prewarm untrained NSG index");
    return;
  }
  
  logger_.Info("Prewarming NSG index with " + std::to_string(query_count) + " sample queries");
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  try {
    // Prepare temporary buffers for warmup results
    std::vector<float> temp_distances(k * query_count);
    std::vector<int64_t> temp_ids(k * query_count);
    
    // Execute warmup queries
    for (size_t i = 0; i < query_count; ++i) {
      const float* query = sample_queries + i * dim;
      float* query_distances = temp_distances.data() + i * k;
      int64_t* query_ids = temp_ids.data() + i * k;
      
      Search(const_cast<float*>(query), 1, dim, k, query_distances, query_ids);
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto warmup_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);
    
    logger_.Info("NSG index prewarming completed in " + std::to_string(warmup_time.count()) + "ms");
    
  } catch (const std::exception& e) {
    logger_.Error("Exception during NSG prewarming: " + std::string(e.what()));
  }
}

::vectordb::engine::index::NsgIndex::Metric_Type NSGIndex::ConvertMetricType(meta::MetricType type) const {
  switch (type) {
    case meta::MetricType::EUCLIDEAN:
      return ::vectordb::engine::index::NsgIndex::Metric_Type_L2;
    case meta::MetricType::COSINE:
      return ::vectordb::engine::index::NsgIndex::Metric_Type_COSINE;
    case meta::MetricType::DOT_PRODUCT:
      return ::vectordb::engine::index::NsgIndex::Metric_Type_IP;
    default:
      logger_.Warning("Unknown metric type, defaulting to L2");
      return ::vectordb::engine::index::NsgIndex::Metric_Type_L2;
  }
}

NSGBuildParams NSGIndex::ExtractNSGParams(const IndexBuildParams& params) const {
  NSGBuildParams nsg_params;
  
  // Copy base parameters
  nsg_params.metric_type = params.metric_type;
  nsg_params.selection_strategy = params.selection_strategy;
  nsg_params.target_accuracy = params.target_accuracy;
  
  // If the params are already NSGBuildParams, use them directly
  if (const NSGBuildParams* nsg_specific = dynamic_cast<const NSGBuildParams*>(&params)) {
    nsg_params = *nsg_specific;
  } else {
    // Use default NSG parameters
    logger_.Debug("Using default NSG parameters");
  }
  
  return nsg_params;
}

} // namespace index
} // namespace engine
} // namespace vectordb