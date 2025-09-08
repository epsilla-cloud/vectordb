#pragma once

#include "db/index/base_index.hpp"
#include "db/index/nsg/nsg.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {
namespace index {

/**
 * @brief NSG Index adapter for the new index interface
 */
class NSGIndex : public BaseIndex {
private:
  std::unique_ptr<::vectordb::engine::index::NsgIndex> nsg_impl_;
  size_t dimension_;
  meta::MetricType metric_type_;
  NSGBuildParams build_params_;
  bool is_trained_;
  mutable Logger logger_;

public:
  explicit NSGIndex(size_t dimension, meta::MetricType metric_type);
  virtual ~NSGIndex();

  // BaseIndex interface implementation
  IndexType GetIndexType() const override { 
    return IndexType::NSG; 
  }

  size_t Build(size_t nb, 
              const VectorColumnData& data, 
              const int64_t* ids,
              const IndexBuildParams& params) override;

  void Search(const DenseVectorPtr query,
             const size_t nq,
             const size_t dim,
             const size_t k,
             float* distances,
             int64_t* ids,
             ConcurrentBitsetPtr bitset = nullptr) override;

  size_t GetMemoryUsage() const override;
  IndexStats GetStats() const override;
  bool IsTrained() const override { return is_trained_; }
  size_t GetVectorCount() const override;
  size_t GetDimension() const override { return dimension_; }

  bool SaveToFile(const std::string& file_path) const override;
  bool LoadFromFile(const std::string& file_path) override;

  void Prewarm(const DenseVectorPtr sample_queries,
              size_t query_count,
              size_t dim, 
              size_t k = 10) override;

private:
  /**
   * @brief Convert metric type to NSG metric type
   */
  ::vectordb::engine::index::NsgIndex::Metric_Type ConvertMetricType(meta::MetricType type) const;

  /**
   * @brief Extract NSG parameters from build params
   */
  NSGBuildParams ExtractNSGParams(const IndexBuildParams& params) const;
};

} // namespace index
} // namespace engine
} // namespace vectordb