#pragma once

#include <vector>

#include "db/index/knn/nndescent_common.hpp"

namespace vectordb {
namespace engine {
namespace index {

using std::cerr;
using std::swap;
using std::vector;

#ifndef NNDES_SHOW_PROGRESS
#define NNDES_SHOW_PROGRESS 1
#endif

// The main NN-Descent class.
// Instead of the actual dataset, the class takes a distance oracle
// as input.  Given two data item ids, the oracle returns the distance
// between the two.
template <typename ORACLE>
class NNDescent {
 private:
  const ORACLE &oracle;
  int N;           // # points
  int K;           // K-NN to find
  int S;           // # of NNs to use for exploration
  vector<KNN> nn;  // K-NN approximation

  // We maintain old and newly added KNN/RNN items
  // separately for incremental processing:
  // we need to compare two new ones
  // and a new one to an old one, but not two old ones as they
  // must have been compared already.
  vector<vector<int> > nn_old;
  vector<vector<int> > nn_new;
  vector<vector<int> > rnn_old;
  vector<vector<int> > rnn_new;

  // total number of comparisons done.
  long long int cost;

  // This function decides of it's necessary to compare two
  // points.  Obviously a point should not compare against itself.
  // Another potential usage of this function is to record all
  // pairs that have already be compared, so that when seen in the future,
  // then same pair doesn't have be compared again.
  bool mark(int p1, int p2) {
    return p1 == p2;
  }

  // Compare two points and update their KNN list of necessary.
  // Return the number of comparisons done (0 or 1).
  int update(int p1, int p2) {
    if (mark(p1, p2)) return 0;
    // KNN::update is synchronized by a lock
    // keep an order is necessary to avoid deadlock.
    if (p1 > p2) swap(p1, p2);
    float dist = oracle(p1, p2);
    nn[p1].update(KNN::Element(p2, dist, true));
    nn[p2].update(KNN::Element(p1, dist, true));
    return 1;
  }

 public:
  const vector<KNN> &getNN() const {
    return nn;
  }

  long long int getCost() const {
    return cost;
  }

  NNDescent(int N_, int K_, float S_, const ORACLE &oracle_)
      : oracle(oracle_), N(N_), K(K_), S(K * S_), nn(N_), nn_old(N_), nn_new(N_), rnn_old(N_), rnn_new(N_), cost(0) {
    for (int i = 0; i < N; ++i) {
      nn[i].init(K);
      // random initial edges
      nn_new[i].resize(S);
      BOOST_FOREACH (int &u, nn_new[i]) {
        u = rand() % N;
      }

      rnn_new[i].resize(S);
      BOOST_FOREACH (int &u, rnn_new[i]) {
        u = rand() % N;
      }
    }
  }

  // An iteration contains two parts:
  //      local join
  //      identify the newly detected NNs.
  int iterate() {
    long long int cc = 0;
    // local joins
#pragma omp parallel for default(shared) reduction(+ : cc)
    for (int i = 0; i < N; ++i) {
      BOOST_FOREACH (int j, nn_new[i]) {
        BOOST_FOREACH (int k, nn_new[i]) {
          if (j >= k) continue;
          cc += update(j, k);
        }
        BOOST_FOREACH (int k, nn_old[i]) {
          cc += update(j, k);
        }
      }

      BOOST_FOREACH (int j, rnn_new[i]) {
        BOOST_FOREACH (int k, rnn_new[i]) {
          if (j >= k) continue;
          cc += update(j, k);
        }
        BOOST_FOREACH (int k, rnn_old[i]) {
          cc += update(j, k);
        }
      }

      BOOST_FOREACH (int j, nn_new[i]) {
        BOOST_FOREACH (int k, rnn_old[i]) {
          cc += update(j, k);
        }
        BOOST_FOREACH (int k, rnn_new[i]) {
          cc += update(j, k);
        }
      }
      BOOST_FOREACH (int j, nn_old[i]) {
        BOOST_FOREACH (int k, rnn_new[i]) {
          cc += update(j, k);
        }
      }
    }

    cost += cc;

    int t = 0;
#pragma omp parallel for default(shared) reduction(+ : t)
    for (int i = 0; i < N; ++i) {
      nn_old[i].clear();
      nn_new[i].clear();
      rnn_old[i].clear();
      rnn_new[i].clear();

      // find the new ones
      for (int j = 0; j < K; ++j) {
        KNN::Element &e = nn[i][j];
        if (e.key == KNN::Element::BAD) continue;
        if (e.flag) {
          nn_new[i].push_back(j);
        } else {
          nn_old[i].push_back(e.key);
        }
      }

      t += nn_new[i].size();
      // sample
      if (nn_new[i].size() > unsigned(S)) {
        random_shuffle(nn_new[i].begin(), nn_new[i].end());
        nn_new[i].resize(S);
      }
      BOOST_FOREACH (int &v, nn_new[i]) {
        nn[i][v].flag = false;
        v = nn[i][v].key;
      }
    }

    // symmetrize
    for (int i = 0; i < N; ++i) {
      BOOST_FOREACH (int e, nn_old[i]) {
        rnn_old[e].push_back(i);
      }
      BOOST_FOREACH (int e, nn_new[i]) {
        rnn_new[e].push_back(i);
      }
    }

#pragma omp parallel for default(shared) reduction(+ : t)
    for (int i = 0; i < N; ++i) {
      if (rnn_old[i].size() > unsigned(S)) {
        random_shuffle(rnn_old[i].begin(), rnn_old[i].end());
        rnn_old[i].resize(S);
      }
      if (rnn_new[i].size() > unsigned(S)) {
        random_shuffle(rnn_new[i].begin(), rnn_new[i].end());
        rnn_new[i].resize(S);
      }
    }

    return t;
  }
};

}  // namespace index
}  // namespace engine
}  // namespace vectordb
