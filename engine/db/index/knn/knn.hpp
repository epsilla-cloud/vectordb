#pragma once

#include <sys/time.h>

#include <boost/format.hpp>
#include <boost/program_options.hpp>
#include <iomanip>
#include <iostream>

#include "db/index/knn/nndescent.hpp"
#include "db/index/space_cosine.hpp"
#include "db/index/space_ip.hpp"
#include "db/index/space_l2.hpp"

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
  DISTFUNC<float> fstdistfunc_;
  void* dist_func_param_;
  float* m;

 public:
  OracleL2(size_t dim_, float* m_, meta::MetricType metricType) : dim(dim_) {
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
    m = m_;
  }
  float operator()(int i, int j) const __attribute__((noinline));
};

float OracleL2::operator()(int p, int q) const {
  // std::cout << fstdistfunc_(m + p * dim, m + q * dim, dist_func_param_) << " ";
  return fstdistfunc_(m + p * dim, m + q * dim, dist_func_param_);
}
}  // namespace

class KNNGraph {
 public:
  KNNGraph(int N, int D, int K, float* data, Graph& knng, meta::MetricType metricType) {
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
      std::cout << "iterate " << it << std::endl;
      int t = nndes.iterate();
      float rate = float(t) / (K * N);
      std::cout << rate << std::endl;
      if (rate < T) break;
    }

    // Convert the graph.
    const vector<KNN>& nn = nndes.getNN();
#pragma omp parallel for
    for (size_t id = 0; id < nn.size(); ++id) {
      const KNN& knn = nn[id];
      BOOST_FOREACH (KNN::Element const& e, knn) {
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
};

}  // namespace index
}  // namespace engine
}  // namespace vectordb
