#pragma once

#include <sys/time.h>

#include <boost/format.hpp>
#include <boost/program_options.hpp>
#include <iomanip>
#include <iostream>

#include "db/index/knn/nndescent.hpp"

namespace vectordb {
namespace engine {
namespace index {

using namespace boost;

// L2 distance oracle on a dense dataset
// special SSE optimization is implemented for float data

class OracleL2 {
 private:
  size_t dim;
  float* m;

 public:
  OracleL2(size_t dim_, float* m_) : dim(dim_) {
    m = m_;
  }
  float operator()(int i, int j) const __attribute__((noinline));
};

float OracleL2::operator()(int p, int q) const {
  size_t o1 = p * dim;
  size_t o2 = q * dim;
  float r = 0.0;
  for (int i = 0; i < dim; ++i) {
    float v = m[o1 + i] - m[o2 + i];
    r += v * v;
  }
  return sqrt(r);
}

class KNNGraph {
 public:
  KNNGraph(int N, int D, int K, float* data) {
    int I = 100;
    float T = 0.001;
    float S = 1;

    // if (vm.count("fast") && (vm.count("rho") == 0)) {
    //   S = 0.5;
    // }

    OracleL2 oracle(D, data);

    NNDescent<OracleL2> nndes(N, K, S, oracle);

    float total = float(N) * (N - 1) / 2;
    for (int it = 0; it < I; ++it) {
      std::cout << "iterate" << std::endl;
      int t = nndes.iterate();
      float rate = float(t) / (K * N);
      if (rate < T) break;
    }

    // // Debug output
    // const vector<KNN> &nn = nndes.getNN();
    // int i = 0;
    // BOOST_FOREACH (KNN const &knn, nn) {
    //   std::cout << i++;
    //   BOOST_FOREACH (KNN::Element const &e, knn) {
    //     std::cout << ' ' << e.key;
    //   }
    //   std::cout << std::endl;
    // }
    std::cout << "Finished" << std::endl;
  }
};

}  // namespace index
}  // namespace engine
}  // namespace vectordb
