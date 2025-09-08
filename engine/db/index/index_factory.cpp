#include "db/index/base_index.hpp"
#include "db/index/nsg_index.hpp"
#include "db/index/ivf_index.hpp"  
#include "db/index/pq_index.hpp"
#include "db/index/index_selector.hpp"

namespace vectordb {
namespace engine {
namespace index {

BaseIndexPtr IndexFactory::CreateIndex(IndexType type, 
                                       size_t dimension,
                                       meta::MetricType metric_type) {
  switch (type) {
    case IndexType::NSG:
      return std::make_shared<NSGIndex>(dimension, metric_type);
      
    case IndexType::IVF:
      return std::make_shared<IVFIndex>(dimension, metric_type);
      
    case IndexType::PQ:
      return std::make_shared<PQIndex>(dimension, metric_type);
      
    case IndexType::IVF_PQ:
      // For now, fallback to IVF (could implement combined IVF+PQ later)
      return std::make_shared<IVFIndex>(dimension, metric_type);
      
    case IndexType::FLAT:
      // For now, fallback to NSG with small parameters for brute-force like behavior
      return std::make_shared<NSGIndex>(dimension, metric_type);
      
    case IndexType::AUTO:
    default:
      // Use NSG as default for auto-selection
      return std::make_shared<NSGIndex>(dimension, metric_type);
  }
}

IndexType IndexFactory::SelectBestIndexType(size_t vector_count,
                                           size_t dimension,
                                           const IndexBuildParams& params) {
  // Create selection criteria from build params
  SelectionCriteria criteria;
  criteria.vector_count = vector_count;
  criteria.dimension = dimension;
  criteria.available_memory_mb = params.available_memory_mb;
  criteria.expected_qps = params.expected_query_per_second;
  criteria.target_accuracy = params.target_accuracy;
  criteria.metric_type = params.metric_type;
  criteria.strategy = params.selection_strategy;
  
  return IndexSelector::SelectBestIndex(criteria);
}

BaseIndexPtr IndexFactory::CreateAutoIndex(size_t vector_count,
                                          size_t dimension,
                                          const IndexBuildParams& params) {
  IndexType selected_type = SelectBestIndexType(vector_count, dimension, params);
  return CreateIndex(selected_type, dimension, params.metric_type);
}

} // namespace index
} // namespace engine
} // namespace vectordb