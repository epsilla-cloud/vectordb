#include "db/index/pq_index.hpp"
#include <algorithm>
#include <numeric>
#include <fstream>
#include <cstring>

namespace vectordb {
namespace engine {
namespace index {

PQIndex::PQIndex(size_t dimension, meta::MetricType metric_type)
    : dimension_(dimension)
    , m_(8)  // Default number of subquantizers
    , nbits_(8)  // Default bits per subquantizer
    , ksub_(256)  // 2^8 = 256 centroids per subquantizer
    , dsub_(dimension / 8)  // Dimension of each subvector
    , metric_type_(metric_type)
    , is_trained_(false) {
  
  // Ensure dimension is divisible by m
  if (dimension_ % m_ != 0) {
    // Adjust m to be a divisor of dimension
    for (size_t i = m_; i > 1; --i) {
      if (dimension_ % i == 0) {
        m_ = i;
        break;
      }
    }
    dsub_ = dimension_ / m_;
    logger_.Warning("Adjusted PQ parameters: m=" + std::to_string(m_) + 
                   ", dsub=" + std::to_string(dsub_));
  }
  
  stats_.dimensions = dimension;
  stats_.last_updated = std::chrono::system_clock::now();
  
  logger_.Info("Created PQ index with dimension=" + std::to_string(dimension) + 
              ", m=" + std::to_string(m_) + ", dsub=" + std::to_string(dsub_));
}

size_t PQIndex::Build(size_t nb, 
                     const VectorColumnData& data, 
                     const int64_t* ids,
                     const IndexBuildParams& params) {
  auto start_time = std::chrono::high_resolution_clock::now();
  
  try {
    PQBuildParams pq_params = ExtractPQParams(params);
    
    // Validate parameters
    if (!ValidateParams(pq_params)) {
      logger_.Error("Invalid PQ parameters");
      return 0;
    }
    
    logger_.Info("Building PQ index with " + std::to_string(nb) + " vectors, " +
                "m=" + std::to_string(m_) + ", nbits=" + std::to_string(nbits_));
    
    // Step 1: Train codebooks
    TrainCodebooks(data, nb, pq_params);
    
    // Step 2: Encode vectors
    EncodeVectors(data, ids, nb);
    
    is_trained_ = true;
    
    // Update statistics
    auto end_time = std::chrono::high_resolution_clock::now();
    stats_.build_time = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);
    stats_.indexed_vectors = nb;
    stats_.memory_usage_bytes = GetMemoryUsage();
    
    // Calculate compression ratio
    size_t uncompressed_size = nb * dimension_ * sizeof(float);
    size_t compressed_size = nb * m_ * sizeof(uint8_t);
    stats_.compression_ratio = static_cast<double>(uncompressed_size) / compressed_size;
    
    stats_.last_updated = std::chrono::system_clock::now();
    
    logger_.Info("PQ index built successfully: " + std::to_string(nb) + 
                " vectors indexed in " + std::to_string(stats_.build_time.count()) + "ms, " +
                "compression ratio: " + std::to_string(stats_.compression_ratio) + "x");
    
    return nb;
    
  } catch (const std::exception& e) {
    logger_.Error("Exception during PQ index build: " + std::string(e.what()));
    return 0;
  }
}

void PQIndex::Search(const DenseVectorPtr query,
                    const size_t nq,
                    const size_t dim,
                    const size_t k,
                    float* distances,
                    int64_t* ids,
                    ConcurrentBitsetPtr bitset) {
  
  if (!is_trained_) {
    logger_.Warning("Attempting search on untrained PQ index");
    return;
  }
  
  auto start_time = std::chrono::high_resolution_clock::now();
  
  try {
    for (size_t q = 0; q < nq; ++q) {
      const float* single_query = query + q * dim;
      
      // Compute distance table for this query
      ComputeDistanceTable(single_query);
      
      // Calculate asymmetric distances to all vectors
      std::vector<std::pair<float, int64_t>> candidates;
      candidates.reserve(pq_codes_.size());
      
      for (size_t i = 0; i < pq_codes_.size(); ++i) {
        int64_t vector_id = vector_ids_[i];
        
        // Check if vector is filtered out
        if (bitset && bitset->test(static_cast<size_t>(vector_id))) {
          continue;
        }
        
        float distance = ComputeAsymmetricDistance(pq_codes_[i]);
        candidates.emplace_back(distance, vector_id);
      }
      
      // Sort and select top-k
      size_t result_count = std::min(k, candidates.size());
      std::partial_sort(candidates.begin(), 
                       candidates.begin() + result_count,
                       candidates.end());
      
      // Copy results
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
    
    logger_.Debug("PQ search completed: " + std::to_string(nq) + " queries, k=" + 
                 std::to_string(k) + ", time=" + std::to_string(search_duration.count()) + "μs");
    
  } catch (const std::exception& e) {
    logger_.Error("Exception during PQ search: " + std::string(e.what()));
  }
}

size_t PQIndex::GetMemoryUsage() const {
  size_t total = 0;
  
  // Codebooks
  total += m_ * ksub_ * dsub_ * sizeof(float);
  
  // PQ codes
  total += pq_codes_.size() * m_ * sizeof(uint8_t);
  
  // Vector IDs
  total += vector_ids_.size() * sizeof(int64_t);
  
  // Distance tables (temporary)
  total += m_ * ksub_ * sizeof(float);
  
  // Object overhead
  total += sizeof(*this);
  
  return total;
}

IndexStats PQIndex::GetStats() const {
  IndexStats current_stats = stats_;
  current_stats.memory_usage_bytes = GetMemoryUsage();
  current_stats.last_updated = std::chrono::system_clock::now();
  return current_stats;
}

bool PQIndex::SaveToFile(const std::string& file_path) const {
  if (!is_trained_) {
    logger_.Warning("Cannot save untrained PQ index");
    return false;
  }
  
  try {
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
      logger_.Error("Cannot open file for saving: " + file_path);
      return false;
    }
    
    // Write header
    file.write("PQ_INDEX", 8);
    file.write(reinterpret_cast<const char*>(&dimension_), sizeof(dimension_));
    file.write(reinterpret_cast<const char*>(&m_), sizeof(m_));
    file.write(reinterpret_cast<const char*>(&nbits_), sizeof(nbits_));
    file.write(reinterpret_cast<const char*>(&ksub_), sizeof(ksub_));
    file.write(reinterpret_cast<const char*>(&dsub_), sizeof(dsub_));
    file.write(reinterpret_cast<const char*>(&metric_type_), sizeof(metric_type_));
    
    // Write codebooks
    for (size_t m = 0; m < m_; ++m) {
      for (size_t k = 0; k < ksub_; ++k) {
        file.write(reinterpret_cast<const char*>(codebooks_[m][k].data()),
                  dsub_ * sizeof(float));
      }
    }
    
    // Write vector count
    size_t vector_count = vector_ids_.size();
    file.write(reinterpret_cast<const char*>(&vector_count), sizeof(vector_count));
    
    // Write vector IDs
    file.write(reinterpret_cast<const char*>(vector_ids_.data()),
              vector_count * sizeof(int64_t));
    
    // Write PQ codes
    for (size_t i = 0; i < vector_count; ++i) {
      file.write(reinterpret_cast<const char*>(pq_codes_[i].data()),
                m_ * sizeof(uint8_t));
    }
    
    logger_.Info("PQ index saved to: " + file_path);
    return true;
    
  } catch (const std::exception& e) {
    logger_.Error("Failed to save PQ index: " + std::string(e.what()));
    return false;
  }
}

bool PQIndex::LoadFromFile(const std::string& file_path) {
  try {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
      logger_.Error("Cannot open file for loading: " + file_path);
      return false;
    }
    
    // Read and verify header
    char header[9];
    file.read(header, 8);
    header[8] = '\0';
    
    if (std::string(header) != "PQ_INDEX") {
      logger_.Error("Invalid PQ index file format");
      return false;
    }
    
    // Read parameters
    file.read(reinterpret_cast<char*>(&dimension_), sizeof(dimension_));
    file.read(reinterpret_cast<char*>(&m_), sizeof(m_));
    file.read(reinterpret_cast<char*>(&nbits_), sizeof(nbits_));
    file.read(reinterpret_cast<char*>(&ksub_), sizeof(ksub_));
    file.read(reinterpret_cast<char*>(&dsub_), sizeof(dsub_));
    file.read(reinterpret_cast<char*>(&metric_type_), sizeof(metric_type_));
    
    // Initialize codebooks
    codebooks_.resize(m_);
    for (size_t m = 0; m < m_; ++m) {
      codebooks_[m].resize(ksub_);
      for (size_t k = 0; k < ksub_; ++k) {
        codebooks_[m][k].resize(dsub_);
        file.read(reinterpret_cast<char*>(codebooks_[m][k].data()),
                 dsub_ * sizeof(float));
      }
    }
    
    // Read vector count
    size_t vector_count;
    file.read(reinterpret_cast<char*>(&vector_count), sizeof(vector_count));
    
    // Read vector IDs
    vector_ids_.resize(vector_count);
    file.read(reinterpret_cast<char*>(vector_ids_.data()),
             vector_count * sizeof(int64_t));
    
    // Read PQ codes
    pq_codes_.resize(vector_count);
    for (size_t i = 0; i < vector_count; ++i) {
      pq_codes_[i].resize(m_);
      file.read(reinterpret_cast<char*>(pq_codes_[i].data()),
               m_ * sizeof(uint8_t));
    }
    
    // Initialize distance tables
    distance_tables_.resize(m_);
    for (size_t m = 0; m < m_; ++m) {
      distance_tables_[m].resize(ksub_);
    }
    
    is_trained_ = true;
    
    logger_.Info("PQ index loaded from: " + file_path);
    return true;
    
  } catch (const std::exception& e) {
    logger_.Error("Failed to load PQ index: " + std::string(e.what()));
    return false;
  }
}

void PQIndex::Prewarm(const DenseVectorPtr sample_queries,
                     size_t query_count,
                     size_t dim, 
                     size_t k) {
  if (!is_trained_) {
    logger_.Warning("Cannot prewarm untrained PQ index");
    return;
  }
  
  logger_.Info("Prewarming PQ index with " + std::to_string(query_count) + " sample queries");
  
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
    
    logger_.Info("PQ index prewarming completed in " + std::to_string(warmup_time.count()) + "ms");
    
  } catch (const std::exception& e) {
    logger_.Error("Exception during PQ prewarming: " + std::string(e.what()));
  }
}

bool PQIndex::AddVectors(size_t nb,
                        const VectorColumnData& data,
                        const int64_t* ids) {
  if (!is_trained_) {
    logger_.Warning("Cannot add vectors to untrained PQ index");
    return false;
  }
  
  try {
    const float* data_ptr = std::get<DenseVectorColumnDataContainer>(data);
    for (size_t i = 0; i < nb; ++i) {
      const float* vector = data_ptr + i * dimension_;
      int64_t vector_id = ids[i];
      
      // Encode vector
      std::vector<uint8_t> codes = EncodeVector(vector);
      
      // Add to index
      vector_ids_.push_back(vector_id);
      pq_codes_.push_back(codes);
    }
    
    stats_.indexed_vectors += nb;
    stats_.memory_usage_bytes = GetMemoryUsage();
    stats_.last_updated = std::chrono::system_clock::now();
    
    logger_.Debug("Added " + std::to_string(nb) + " vectors to PQ index");
    return true;
    
  } catch (const std::exception& e) {
    logger_.Error("Exception adding vectors to PQ index: " + std::string(e.what()));
    return false;
  }
}

bool PQIndex::RemoveVectors(const int64_t* ids, size_t count) {
  if (!is_trained_) {
    return false;
  }
  
  try {
    size_t removed_count = 0;
    
    for (size_t i = 0; i < count; ++i) {
      int64_t target_id = ids[i];
      
      // Find vector in the index
      for (size_t j = 0; j < vector_ids_.size(); ++j) {
        if (vector_ids_[j] == target_id) {
          vector_ids_.erase(vector_ids_.begin() + j);
          pq_codes_.erase(pq_codes_.begin() + j);
          removed_count++;
          break;
        }
      }
    }
    
    stats_.indexed_vectors -= removed_count;
    stats_.memory_usage_bytes = GetMemoryUsage();
    stats_.last_updated = std::chrono::system_clock::now();
    
    logger_.Debug("Removed " + std::to_string(removed_count) + " vectors from PQ index");
    return removed_count > 0;
    
  } catch (const std::exception& e) {
    logger_.Error("Exception removing vectors from PQ index: " + std::string(e.what()));
    return false;
  }
}

void PQIndex::TrainCodebooks(const VectorColumnData& data, 
                            size_t nb,
                            const PQBuildParams& params) {
  logger_.Debug("Training PQ codebooks with " + std::to_string(m_) + " subquantizers");
  
  // Initialize codebooks
  codebooks_.resize(m_);
  
  // Train each subquantizer
  for (size_t subq = 0; subq < m_; ++subq) {
    logger_.Debug("Training subquantizer " + std::to_string(subq));
    
    // Extract subvectors for this subquantizer
    std::vector<std::vector<float>> subvectors;
    subvectors.reserve(nb);
    
    const float* data_ptr = std::get<DenseVectorColumnDataContainer>(data);
    for (size_t i = 0; i < nb; ++i) {
      std::vector<float> subvector(dsub_);
      GetSubvector(data_ptr + i * dimension_, subq, subvector.data());
      subvectors.push_back(subvector);
    }
    
    // Train k-means for this subquantizer
    TrainSubquantizer(subq, subvectors, 25);
  }
  
  // Initialize distance tables
  distance_tables_.resize(m_);
  for (size_t m = 0; m < m_; ++m) {
    distance_tables_[m].resize(ksub_);
  }
  
  logger_.Debug("PQ codebook training completed");
}

void PQIndex::EncodeVectors(const VectorColumnData& data,
                           const int64_t* ids,
                           size_t nb) {
  logger_.Debug("Encoding " + std::to_string(nb) + " vectors");
  
  vector_ids_.clear();
  pq_codes_.clear();
  vector_ids_.reserve(nb);
  pq_codes_.reserve(nb);
  
  const float* data_ptr = std::get<DenseVectorColumnDataContainer>(data);
  for (size_t i = 0; i < nb; ++i) {
    const float* vector = data_ptr + i * dimension_;
    int64_t vector_id = ids[i];
    
    std::vector<uint8_t> codes = EncodeVector(vector);
    
    vector_ids_.push_back(vector_id);
    pq_codes_.push_back(codes);
  }
  
  logger_.Debug("Vector encoding completed");
}

void PQIndex::ComputeDistanceTable(const float* query) const {
  for (size_t subq = 0; subq < m_; ++subq) {
    // Extract query subvector
    std::vector<float> query_subvector(dsub_);
    GetSubvector(query, subq, query_subvector.data());
    
    // Compute distances to all centroids in this subquantizer
    for (size_t k = 0; k < ksub_; ++k) {
      distance_tables_[subq][k] = SubvectorDistance(
        query_subvector.data(), 
        codebooks_[subq][k].data(), 
        dsub_);
    }
  }
}

float PQIndex::ComputeAsymmetricDistance(const std::vector<uint8_t>& codes) const {
  float total_distance = 0.0f;
  
  for (size_t subq = 0; subq < m_; ++subq) {
    uint8_t code = codes[subq];
    total_distance += distance_tables_[subq][code];
  }
  
  return total_distance;
}

void PQIndex::TrainSubquantizer(size_t subq_id,
                               const std::vector<std::vector<float>>& subvectors,
                               size_t iterations) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<size_t> dist(0, subvectors.size() - 1);
  
  // Initialize centroids randomly
  codebooks_[subq_id].resize(ksub_);
  for (size_t k = 0; k < ksub_; ++k) {
    size_t random_idx = dist(gen);
    codebooks_[subq_id][k] = subvectors[random_idx];
  }
  
  // K-means iterations
  for (size_t iter = 0; iter < iterations; ++iter) {
    std::vector<std::vector<size_t>> assignments(ksub_);
    
    // Assign each subvector to nearest centroid
    for (size_t i = 0; i < subvectors.size(); ++i) {
      uint8_t nearest = FindNearestCentroid(subq_id, subvectors[i].data());
      assignments[nearest].push_back(i);
    }
    
    // Update centroids
    for (size_t k = 0; k < ksub_; ++k) {
      if (assignments[k].empty()) continue;
      
      std::vector<float> new_centroid(dsub_, 0.0f);
      
      for (size_t idx : assignments[k]) {
        for (size_t d = 0; d < dsub_; ++d) {
          new_centroid[d] += subvectors[idx][d];
        }
      }
      
      for (size_t d = 0; d < dsub_; ++d) {
        new_centroid[d] /= assignments[k].size();
      }
      
      codebooks_[subq_id][k] = new_centroid;
    }
  }
}

std::vector<uint8_t> PQIndex::EncodeVector(const float* vector) const {
  std::vector<uint8_t> codes(m_);
  
  for (size_t subq = 0; subq < m_; ++subq) {
    std::vector<float> subvector(dsub_);
    GetSubvector(vector, subq, subvector.data());
    codes[subq] = FindNearestCentroid(subq, subvector.data());
  }
  
  return codes;
}

uint8_t PQIndex::FindNearestCentroid(size_t subq_id, const float* subvector) const {
  float min_distance = std::numeric_limits<float>::max();
  uint8_t nearest = 0;
  
  for (size_t k = 0; k < ksub_; ++k) {
    float distance = SubvectorDistance(subvector, 
                                      codebooks_[subq_id][k].data(), 
                                      dsub_);
    if (distance < min_distance) {
      min_distance = distance;
      nearest = static_cast<uint8_t>(k);
    }
  }
  
  return nearest;
}

PQBuildParams PQIndex::ExtractPQParams(const IndexBuildParams& params) const {
  if (const PQBuildParams* pq_specific = dynamic_cast<const PQBuildParams*>(&params)) {
    return *pq_specific;
  }
  
  PQBuildParams pq_params;
  pq_params.metric_type = params.metric_type;
  pq_params.selection_strategy = params.selection_strategy;
  pq_params.target_accuracy = params.target_accuracy;
  
  logger_.Debug("Using default PQ parameters");
  return pq_params;
}

bool PQIndex::ValidateParams(const PQBuildParams& params) const {
  if (dimension_ % m_ != 0) {
    logger_.Error("Dimension must be divisible by m");
    return false;
  }
  
  if (nbits_ > 8) {
    logger_.Error("nbits must be <= 8");
    return false;
  }
  
  if (ksub_ != (1u << nbits_)) {
    logger_.Error("ksub must equal 2^nbits");
    return false;
  }
  
  return true;
}

void PQIndex::GetSubvector(const float* vector, size_t subq_id, float* subvector) const {
  size_t start = subq_id * dsub_;
  std::copy(vector + start, vector + start + dsub_, subvector);
}

float PQIndex::SubvectorDistance(const float* a, const float* b, size_t dim) const {
  float distance = 0.0f;
  
  if (metric_type_ == meta::MetricType::EUCLIDEAN) {
    for (size_t i = 0; i < dim; ++i) {
      float diff = a[i] - b[i];
      distance += diff * diff;
    }
  } else if (metric_type_ == meta::MetricType::DOT_PRODUCT) {
    for (size_t i = 0; i < dim; ++i) {
      distance += a[i] * b[i];
    }
    distance = -distance; // Convert to distance (lower is better)
  } else if (metric_type_ == meta::MetricType::COSINE) {
    float dot = 0.0f, norm_a = 0.0f, norm_b = 0.0f;
    for (size_t i = 0; i < dim; ++i) {
      dot += a[i] * b[i];
      norm_a += a[i] * a[i];
      norm_b += b[i] * b[i];
    }
    distance = 1.0f - (dot / (std::sqrt(norm_a) * std::sqrt(norm_b)));
  }
  
  return distance;
}

} // namespace index
} // namespace engine
} // namespace vectordb