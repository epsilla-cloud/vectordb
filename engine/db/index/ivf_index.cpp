#include "db/index/ivf_index.hpp"
#include "db/index/index.hpp"
#include <algorithm>
#include <numeric>
#include <fstream>
#include <cstring>

namespace vectordb {
namespace engine {
namespace index {

IVFIndex::IVFIndex(size_t dimension, meta::MetricType metric_type)
    : dimension_(dimension)
    , nlist_(1024)
    , nprobe_(128)
    , metric_type_(metric_type)
    , is_trained_(false) {
  
  InitializeDistanceFunction();
  
  stats_.dimensions = dimension;
  stats_.last_updated = std::chrono::system_clock::now();
  
  logger_.Info("Created IVF index with dimension=" + std::to_string(dimension) + 
              ", metric=" + std::to_string(static_cast<int>(metric_type)));
}

size_t IVFIndex::Build(size_t nb, 
                      const VectorColumnData& data, 
                      const int64_t* ids,
                      const IndexBuildParams& params) {
  auto start_time = std::chrono::high_resolution_clock::now();
  
  try {
    IVFBuildParams ivf_params = ExtractIVFParams(params);
    nlist_ = ivf_params.nlist;
    nprobe_ = ivf_params.nprobe;
    
    logger_.Info("Building IVF index with " + std::to_string(nb) + " vectors, " +
                std::to_string(nlist_) + " clusters");
    
    // Step 1: Train k-means clustering
    TrainKMeans(data, nb, ivf_params);
    
    // Step 2: Assign vectors to clusters
    AssignVectorsToClusters(data, ids, nb);
    
    is_trained_ = true;
    
    // Update statistics
    auto end_time = std::chrono::high_resolution_clock::now();
    stats_.build_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);
    stats_.indexed_vectors = nb;
    stats_.memory_usage_bytes = GetMemoryUsage();
    stats_.last_updated = std::chrono::system_clock::now();
    
    logger_.Info("IVF index built successfully: " + std::to_string(nb) + 
                " vectors indexed in " + std::to_string(stats_.build_time.count()) + "ms");
    
    return nb;
    
  } catch (const std::exception& e) {
    logger_.Error("Exception during IVF index build: " + std::string(e.what()));
    return 0;
  }
}

void IVFIndex::Search(const DenseVectorPtr query,
                     const size_t nq,
                     const size_t dim,
                     const size_t k,
                     float* distances,
                     int64_t* ids,
                     ConcurrentBitsetPtr bitset) {
  
  if (!is_trained_) {
    logger_.Warning("Attempting search on untrained IVF index");
    return;
  }
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  try {
    for (size_t q = 0; q < nq; ++q) {
      const float* single_query = query + q * dim;
      
      // Find nearest clusters
      auto nearest_clusters = FindNearestClusters(single_query, nprobe_);
      
      // Collect candidates from selected clusters
      std::vector<std::pair<float, int64_t>> candidates;
      
      for (const auto& [cluster_id, cluster_dist] : nearest_clusters) {
        SearchInCluster(cluster_id, single_query, k, candidates, bitset);
      }
      
      // Sort candidates and select top-k
      std::partial_sort(candidates.begin(), 
                       candidates.begin() + std::min(k, candidates.size()),
                       candidates.end());
      
      // Copy results
      size_t result_count = std::min(k, candidates.size());
      for (size_t i = 0; i < result_count; ++i) {
        distances[q * k + i] = candidates[i].first;
        ids[q * k + i] = candidates[i].second;
      }
      
      // Fill remaining slots if needed
      for (size_t i = result_count; i < k; ++i) {
        distances[q * k + i] = std::numeric_limits<float>::max();
        ids[q * k + i] = -1;
      }
    }
    
    // Update search statistics
    auto end_time = std::chrono::high_resolution_clock::now();
    auto search_duration = std::chrono::duration_cast<std::chrono::microseconds>(
      end_time - start_time);
    
    UpdateSearchStats(search_duration);
    
    logger_.Debug("IVF search completed: " + std::to_string(nq) + " queries, k=" + 
                 std::to_string(k) + ", time=" + std::to_string(search_duration.count()) + "μs");
    
  } catch (const std::exception& e) {
    logger_.Error("Exception during IVF search: " + std::string(e.what()));
  }
}

size_t IVFIndex::GetMemoryUsage() const {
  size_t total = 0;
  
  // Centroids
  total += centroids_.size() * sizeof(float);
  
  // Inverted lists
  for (const auto& list : inverted_lists_) {
    total += list.size() * sizeof(int64_t);
  }
  
  // Cluster vectors
  for (const auto& cluster : cluster_vectors_) {
    for (const auto& vector : cluster) {
      total += vector.size() * sizeof(float);
    }
  }
  
  // Vector to cluster mapping
  total += vector_to_cluster_.size() * (sizeof(int64_t) + sizeof(size_t));
  
  // Object overhead
  total += sizeof(*this);
  
  return total;
}

IndexStats IVFIndex::GetStats() const {
  IndexStats current_stats = stats_;
  current_stats.memory_usage_bytes = GetMemoryUsage();
  current_stats.last_updated = std::chrono::system_clock::now();
  return current_stats;
}

size_t IVFIndex::GetVectorCount() const {
  size_t total = 0;
  for (const auto& list : inverted_lists_) {
    total += list.size();
  }
  return total;
}

bool IVFIndex::SaveToFile(const std::string& file_path) const {
  if (!is_trained_) {
    logger_.Warning("Cannot save untrained IVF index");
    return false;
  }
  
  try {
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
      logger_.Error("Cannot open file for saving: " + file_path);
      return false;
    }
    
    // Write header
    file.write("IVF_INDEX", 9);
    file.write(reinterpret_cast<const char*>(&dimension_), sizeof(dimension_));
    file.write(reinterpret_cast<const char*>(&nlist_), sizeof(nlist_));
    file.write(reinterpret_cast<const char*>(&nprobe_), sizeof(nprobe_));
    file.write(reinterpret_cast<const char*>(&metric_type_), sizeof(metric_type_));
    
    // Write centroids
    size_t centroids_size = centroids_.size();
    file.write(reinterpret_cast<const char*>(&centroids_size), sizeof(centroids_size));
    file.write(reinterpret_cast<const char*>(centroids_.data()), 
              centroids_size * sizeof(float));
    
    // Write inverted lists and cluster vectors
    for (size_t i = 0; i < nlist_; ++i) {
      size_t list_size = inverted_lists_[i].size();
      file.write(reinterpret_cast<const char*>(&list_size), sizeof(list_size));
      file.write(reinterpret_cast<const char*>(inverted_lists_[i].data()),
                list_size * sizeof(int64_t));
      
      for (size_t j = 0; j < list_size; ++j) {
        file.write(reinterpret_cast<const char*>(cluster_vectors_[i][j].data()),
                  dimension_ * sizeof(float));
      }
    }
    
    logger_.Info("IVF index saved to: " + file_path);
    return true;
    
  } catch (const std::exception& e) {
    logger_.Error("Failed to save IVF index: " + std::string(e.what()));
    return false;
  }
}

bool IVFIndex::LoadFromFile(const std::string& file_path) {
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
    
    if (std::string(header) != "IVF_INDEX") {
      logger_.Error("Invalid IVF index file format");
      return false;
    }
    
    // Read parameters
    file.read(reinterpret_cast<char*>(&dimension_), sizeof(dimension_));
    file.read(reinterpret_cast<char*>(&nlist_), sizeof(nlist_));
    file.read(reinterpret_cast<char*>(&nprobe_), sizeof(nprobe_));
    file.read(reinterpret_cast<char*>(&metric_type_), sizeof(metric_type_));
    
    // Read centroids
    size_t centroids_size;
    file.read(reinterpret_cast<char*>(&centroids_size), sizeof(centroids_size));
    centroids_.resize(centroids_size);
    file.read(reinterpret_cast<char*>(centroids_.data()),
             centroids_size * sizeof(float));
    
    // Initialize data structures
    inverted_lists_.resize(nlist_);
    cluster_vectors_.resize(nlist_);
    vector_to_cluster_.clear();
    
    // Read inverted lists and cluster vectors
    for (size_t i = 0; i < nlist_; ++i) {
      size_t list_size;
      file.read(reinterpret_cast<char*>(&list_size), sizeof(list_size));
      
      inverted_lists_[i].resize(list_size);
      cluster_vectors_[i].resize(list_size);
      
      file.read(reinterpret_cast<char*>(inverted_lists_[i].data()),
               list_size * sizeof(int64_t));
      
      for (size_t j = 0; j < list_size; ++j) {
        cluster_vectors_[i][j].resize(dimension_);
        file.read(reinterpret_cast<char*>(cluster_vectors_[i][j].data()),
                 dimension_ * sizeof(float));
        
        // Rebuild vector to cluster mapping
        vector_to_cluster_[inverted_lists_[i][j]] = i;
      }
    }
    
    InitializeDistanceFunction();
    is_trained_ = true;
    
    logger_.Info("IVF index loaded from: " + file_path);
    return true;
    
  } catch (const std::exception& e) {
    logger_.Error("Failed to load IVF index: " + std::string(e.what()));
    return false;
  }
}

void IVFIndex::Prewarm(const DenseVectorPtr sample_queries,
                      size_t query_count,
                      size_t dim, 
                      size_t k) {
  if (!is_trained_) {
    logger_.Warning("Cannot prewarm untrained IVF index");
    return;
  }
  
  logger_.Info("Prewarming IVF index with " + std::to_string(query_count) + " sample queries");
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  try {
    // Prepare temporary buffers
    std::vector<float> temp_distances(k * query_count);
    std::vector<int64_t> temp_ids(k * query_count);
    
    // Execute warmup queries
    Search(sample_queries, query_count, dim, k, 
          temp_distances.data(), temp_ids.data());
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto warmup_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);
    
    logger_.Info("IVF index prewarming completed in " + std::to_string(warmup_time.count()) + "ms");
    
  } catch (const std::exception& e) {
    logger_.Error("Exception during IVF prewarming: " + std::string(e.what()));
  }
}

bool IVFIndex::AddVectors(size_t nb,
                         const VectorColumnData& data,
                         const int64_t* ids) {
  if (!is_trained_) {
    logger_.Warning("Cannot add vectors to untrained IVF index");
    return false;
  }
  
  try {
    const float* data_ptr = std::get<DenseVectorColumnDataContainer>(data);
    for (size_t i = 0; i < nb; ++i) {
      const float* vector = data_ptr + i * dimension_;
      int64_t vector_id = ids[i];
      
      // Find nearest cluster
      size_t cluster_id = AssignToNearestCentroid(vector);
      
      // Add to inverted list
      inverted_lists_[cluster_id].push_back(vector_id);
      
      // Store vector data
      cluster_vectors_[cluster_id].emplace_back(vector, vector + dimension_);
      
      // Update mapping
      vector_to_cluster_[vector_id] = cluster_id;
    }
    
    stats_.indexed_vectors += nb;
    stats_.memory_usage_bytes = GetMemoryUsage();
    stats_.last_updated = std::chrono::system_clock::now();
    
    logger_.Debug("Added " + std::to_string(nb) + " vectors to IVF index");
    return true;
    
  } catch (const std::exception& e) {
    logger_.Error("Exception adding vectors to IVF index: " + std::string(e.what()));
    return false;
  }
}

bool IVFIndex::RemoveVectors(const int64_t* ids, size_t count) {
  if (!is_trained_) {
    return false;
  }
  
  try {
    size_t removed_count = 0;
    
    for (size_t i = 0; i < count; ++i) {
      int64_t vector_id = ids[i];
      
      auto it = vector_to_cluster_.find(vector_id);
      if (it != vector_to_cluster_.end()) {
        size_t cluster_id = it->second;
        
        // Find and remove from inverted list
        auto& inv_list = inverted_lists_[cluster_id];
        auto& cluster_vecs = cluster_vectors_[cluster_id];
        
        for (size_t j = 0; j < inv_list.size(); ++j) {
          if (inv_list[j] == vector_id) {
            inv_list.erase(inv_list.begin() + j);
            cluster_vecs.erase(cluster_vecs.begin() + j);
            removed_count++;
            break;
          }
        }
        
        vector_to_cluster_.erase(it);
      }
    }
    
    stats_.indexed_vectors -= removed_count;
    stats_.memory_usage_bytes = GetMemoryUsage();
    stats_.last_updated = std::chrono::system_clock::now();
    
    logger_.Debug("Removed " + std::to_string(removed_count) + " vectors from IVF index");
    return removed_count > 0;
    
  } catch (const std::exception& e) {
    logger_.Error("Exception removing vectors from IVF index: " + std::string(e.what()));
    return false;
  }
}

void IVFIndex::TrainKMeans(const VectorColumnData& data, 
                          size_t nb,
                          const IVFBuildParams& params) {
  logger_.Debug("Training k-means with " + std::to_string(nlist_) + " clusters");
  
  std::random_device rd;
  std::mt19937 gen(rd());
  
  // Initialize centroids randomly
  centroids_.resize(nlist_ * dimension_);
  std::uniform_int_distribution<size_t> dist(0, nb - 1);
  
  const float* data_ptr = std::get<DenseVectorColumnDataContainer>(data);
  for (size_t i = 0; i < nlist_; ++i) {
    size_t random_idx = dist(gen);
    const float* random_vector = data_ptr + random_idx * dimension_;
    std::copy(random_vector, random_vector + dimension_, 
             centroids_.data() + i * dimension_);
  }
  
  // K-means iterations
  std::vector<std::vector<size_t>> assignments(nlist_);
  
  for (size_t iter = 0; iter < params.max_iterations; ++iter) {
    // Clear assignments
    for (auto& assignment : assignments) {
      assignment.clear();
    }
    
    // Assign each vector to nearest centroid
    for (size_t i = 0; i < nb; ++i) {
      const float* vector = data_ptr + i * dimension_;
      size_t nearest_cluster = AssignToNearestCentroid(vector);
      assignments[nearest_cluster].push_back(i);
    }
    
    // Update centroids
    size_t changed_centroids = 0;
    for (size_t c = 0; c < nlist_; ++c) {
      if (assignments[c].empty()) continue;
      
      std::vector<float> new_centroid(dimension_, 0.0f);
      
      // Calculate mean
      for (size_t idx : assignments[c]) {
        const float* vector = data_ptr + idx * dimension_;
        for (size_t d = 0; d < dimension_; ++d) {
          new_centroid[d] += vector[d];
        }
      }
      
      for (size_t d = 0; d < dimension_; ++d) {
        new_centroid[d] /= assignments[c].size();
      }
      
      // Check if centroid changed significantly
      float* old_centroid = centroids_.data() + c * dimension_;
      bool changed = false;
      for (size_t d = 0; d < dimension_; ++d) {
        if (std::abs(old_centroid[d] - new_centroid[d]) > 1e-6) {
          changed = true;
          break;
        }
      }
      
      if (changed) {
        std::copy(new_centroid.begin(), new_centroid.end(), old_centroid);
        changed_centroids++;
      }
    }
    
    // Check convergence
    double change_ratio = static_cast<double>(changed_centroids) / nlist_;
    logger_.Debug("K-means iteration " + std::to_string(iter) + 
                 ", changed centroids: " + std::to_string(changed_centroids) + 
                 " (" + std::to_string(change_ratio * 100) + "%)");
    
    if (change_ratio < (1.0 - params.convergence_factor)) {
      logger_.Debug("K-means converged after " + std::to_string(iter + 1) + " iterations");
      break;
    }
  }
}

void IVFIndex::AssignVectorsToClusters(const VectorColumnData& data,
                                      const int64_t* ids,
                                      size_t nb) {
  logger_.Debug("Assigning " + std::to_string(nb) + " vectors to clusters");
  
  // Initialize data structures
  inverted_lists_.resize(nlist_);
  cluster_vectors_.resize(nlist_);
  vector_to_cluster_.clear();
  
  // Assign each vector
  const float* data_ptr = std::get<DenseVectorColumnDataContainer>(data);
  for (size_t i = 0; i < nb; ++i) {
    const float* vector = data_ptr + i * dimension_;
    int64_t vector_id = ids[i];
    
    size_t cluster_id = AssignToNearestCentroid(vector);
    
    inverted_lists_[cluster_id].push_back(vector_id);
    cluster_vectors_[cluster_id].emplace_back(vector, vector + dimension_);
    vector_to_cluster_[vector_id] = cluster_id;
  }
  
  // Log cluster sizes
  for (size_t i = 0; i < nlist_; ++i) {
    if (inverted_lists_[i].size() > 0) {
      logger_.Debug("Cluster " + std::to_string(i) + ": " + 
                   std::to_string(inverted_lists_[i].size()) + " vectors");
    }
  }
}

std::vector<std::pair<size_t, float>> IVFIndex::FindNearestClusters(
    const float* query, 
    size_t nprobe) const {
  
  std::vector<std::pair<float, size_t>> distances;
  distances.reserve(nlist_);
  
  // Calculate distances to all centroids
  for (size_t i = 0; i < nlist_; ++i) {
    const float* centroid = centroids_.data() + i * dimension_;
    float distance = dist_func_(query, centroid, &dimension_);
    distances.emplace_back(distance, i);
  }
  
  // Sort and select top nprobe clusters
  size_t actual_nprobe = std::min(nprobe, nlist_);
  std::partial_sort(distances.begin(), 
                   distances.begin() + actual_nprobe, 
                   distances.end());
  
  std::vector<std::pair<size_t, float>> result;
  result.reserve(actual_nprobe);
  
  for (size_t i = 0; i < actual_nprobe; ++i) {
    result.emplace_back(distances[i].second, distances[i].first);
  }
  
  return result;
}

void IVFIndex::SearchInCluster(size_t cluster_id,
                              const float* query,
                              size_t k,
                              std::vector<std::pair<float, int64_t>>& candidates,
                              ConcurrentBitsetPtr bitset) const {
  
  const auto& cluster_vectors = cluster_vectors_[cluster_id];
  const auto& cluster_ids = inverted_lists_[cluster_id];
  
  for (size_t i = 0; i < cluster_vectors.size(); ++i) {
    int64_t vector_id = cluster_ids[i];
    
    // Check if vector is filtered out
    if (bitset && bitset->test(static_cast<size_t>(vector_id))) {
      continue;
    }
    
    const float* vector = cluster_vectors[i].data();
    float distance = dist_func_(query, vector, &dimension_);
    
    candidates.emplace_back(distance, vector_id);
  }
}

size_t IVFIndex::AssignToNearestCentroid(const float* vector) const {
  float min_distance = std::numeric_limits<float>::max();
  size_t nearest_cluster = 0;
  
  for (size_t i = 0; i < nlist_; ++i) {
    const float* centroid = centroids_.data() + i * dimension_;
    float distance = dist_func_(vector, centroid, &dimension_);
    
    if (distance < min_distance) {
      min_distance = distance;
      nearest_cluster = i;
    }
  }
  
  return nearest_cluster;
}

IVFBuildParams IVFIndex::ExtractIVFParams(const IndexBuildParams& params) const {
  if (const IVFBuildParams* ivf_specific = dynamic_cast<const IVFBuildParams*>(&params)) {
    return *ivf_specific;
  }
  
  // Create default parameters
  IVFBuildParams ivf_params;
  ivf_params.metric_type = params.metric_type;
  ivf_params.selection_strategy = params.selection_strategy;
  ivf_params.target_accuracy = params.target_accuracy;
  
  logger_.Debug("Using default IVF parameters");
  return ivf_params;
}

void IVFIndex::InitializeDistanceFunction() {
  dist_func_ = std::get<DenseVecDistFunc<float>>(
    GetDistFunc(meta::FieldType::VECTOR_FLOAT, metric_type_)
  );
}

} // namespace index
} // namespace engine
} // namespace vectordb