#pragma once

#include <sys/time.h>

#include <boost/format.hpp>
#include <boost/program_options.hpp>
#include <iomanip>
#include <iostream>
#include <variant>

#include "db/ann_graph_segment.hpp"
#include "db/index/index.hpp"
#include "db/index/knn/nndescent.hpp"
#include "db/index/space_cosine.hpp"
#include "db/index/space_ip.hpp"
#include "db/index/space_l2.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {
namespace index {

using node_t = int64_t;
using namespace boost;
using Graph = std::vector<std::vector<node_t>>;

namespace {
class OracleL2 {
 private:
  size_t dim;
  std::unique_ptr<SpaceInterface<float>> space;
  DistFunc fstdistfunc_;
  void *dist_func_param_;
  VectorColumnData m;

 public:
  OracleL2(size_t dim_, VectorColumnData m_, meta::MetricType metricType) : dim(dim_) {
    m = m_;

    if (std::holds_alternative<DenseVectorColumnDataContainer>(m_)) {
      switch (metricType) {
        case meta::MetricType::EUCLIDEAN:
          space = std::make_unique<L2Space>(dim_);
          break;
        case meta::MetricType::COSINE:
          space = std::make_unique<CosineSpace>(dim_);
          break;
        case meta::MetricType::DOT_PRODUCT:
          space = std::make_unique<InnerProductSpace>(dim_);
          break;
        default:
          space = std::make_unique<L2Space>(dim_);
      }
      fstdistfunc_ = space->get_dist_func();
      dist_func_param_ = space->get_dist_func_param();
    } else {
      // hold sparse vector
      switch (metricType) {
        case meta::MetricType::EUCLIDEAN:
          fstdistfunc_ = GetL2Dist;
          break;
        case meta::MetricType::COSINE:
          fstdistfunc_ = GetCosineDist;
          break;
        case meta::MetricType::DOT_PRODUCT:
          fstdistfunc_ = GetInnerProductDist;
          break;
        default:
          fstdistfunc_ = GetL2Dist;
      }
    }
  }
  float operator()(int i, int j) const __attribute__((noinline));
};

float OracleL2::operator()(int p, int q) const {
  // std::cout << fstdistfunc_(m + p * dim, m + q * dim, dist_func_param_) << " ";
  if (std::holds_alternative<DenseVectorPtr>(m)) {
    return std::get<DenseVecDistFunc<float>>(fstdistfunc_)(std::get<DenseVectorPtr>(m) + p * dim, std::get<DenseVectorPtr>(m) + q * dim, dist_func_param_);
  } else {
    auto &v1 = std::get<VariableLenAttrColumnContainer *>(m)->at(p);
    auto &v2 = std::get<VariableLenAttrColumnContainer *>(m)->at(q);
    return std::get<SparseVecDistFunc>(fstdistfunc_)(*std::get<SparseVectorPtr>(v1), *std::get<SparseVectorPtr>(v2));
  }
}
}  // namespace

class KNNGraph {
 public:
  KNNGraph(int N, int D, int K, VectorColumnData data, Graph &knng, meta::MetricType metricType) {
    int I = 1000;
    float T = 0.001;
    float S = 1;

    // if (vm.count("fast") && (vm.count("rho") == 0)) {
    //   S = 0.5;
    // }

    OracleL2 oracle(D, data, metricType);

    NNDescent<OracleL2> nndes(N, K, S, oracle);

    float total = float(N) * (N - 1) / 2;
    for (int it = 0; it < I; ++it) {
      logger_.Debug("Iteration " + std::to_string(it));
      int t = nndes.iterate();
      float rate = float(t) / (K * N);
      logger_.Debug("Converge rate: " + std::to_string(rate));
      if (rate < T) break;
    }

    // Convert the graph.
    const vector<KNN> &nn = nndes.getNN();
#pragma omp parallel for
    for (size_t id = 0; id < nn.size(); ++id) {
      const KNN &knn = nn[id];
      BOOST_FOREACH (KNN::Element const &e, knn) {
        if (e.key != KNNEntry::BAD) {
          knng.at(id).emplace_back(e.key);
        }
      }
    }

    // Debug output
    // const vector<KNN> &nn = nndes.getNN();
    // int i = 0;
    // BOOST_FOREACH (KNN const &knn, nn) {
    //   std::cout << i++;
    //   BOOST_FOREACH (KNN::Element const &e, knn) {
    //     std::cout << ' ' << e.key;
    //   }
    //   std::cout << std::endl;
    // }
    // std::cout << "Finished" << std::endl;
  }

 private:
  vectordb::engine::Logger logger_;
};

}  // namespace index
}  // namespace engine
}  // namespace vectordb
