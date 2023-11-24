#pragma once

#include <boost/dynamic_bitset.hpp>
#include <cstddef>
#include <mutex>
#include <string>
#include <vector>

#include "db/ann_graph_segment.hpp"
#include "db/index/nsg/distance.hpp"
#include "db/index/nsg/neighbor.hpp"
#include "db/sparse_vector.hpp"
#include "utils/concurrent_bitset.hpp"

namespace vectordb {
namespace engine {
namespace index {

// // NSG Params
// constexpr const char* knng = "knng";
// constexpr const char* search_length = "search_length";
// constexpr const char* out_degree = "out_degree";
// constexpr const char* candidate = "candidate_pool_size";

using node_t = int64_t;

struct BuildParams {
  size_t search_length;
  size_t out_degree;
  size_t candidate_pool_size;
};

struct SearchParams {
  size_t search_length;
  size_t k;
};

using Graph = std::vector<std::vector<node_t>>;

class NsgIndex {
 public:
  enum Metric_Type {
    Metric_Type_L2,
    Metric_Type_IP,
    Metric_Type_COSINE,
  };

  size_t dimension;
  size_t ntotal;        // totabl nb of indexed vectors
  int32_t metric_type;  // enum Metric_Type
  Distance* distance_;

  VectorColumnData ori_data_;
  int64_t* ids_;
  Graph nsg;   // final graph
  Graph knng;  // reset after build

  node_t navigation_point;  // offset of node in origin data

  bool is_trained = false;

  /*
   * build and search parameter
   */
  size_t search_length;
  size_t candidate_pool_size;  // search deepth in fullset
  size_t out_degree;

 public:
  explicit NsgIndex(const size_t& dimension, const size_t& n, Metric_Type metric);

  NsgIndex() = default;

  virtual ~NsgIndex();

  void SetKnnGraph(Graph& knng);

  size_t Build(size_t nb, VectorColumnData data, const int64_t* ids, const BuildParams& parameters);

  void Search(const DenseVector query, const unsigned& nq, const unsigned& dim, const unsigned& k, float* dist, int64_t* ids,
              SearchParams& params, ConcurrentBitsetPtr bitset = nullptr);

  int64_t GetSize();

  // Not support yet.
  // virtual void Add() = 0;
  // virtual void Add_with_ids() = 0;
  // virtual void Delete() = 0;
  // virtual void Delete_with_ids() = 0;
  // virtual void Rebuild(size_t nb,
  //                     const float *data,
  //                     const int64_t *ids,
  //                     const Parameters &parameters) = 0;
  // virtual void Build(size_t nb,
  //                   const float *data,
  //                   const BuildParam &parameters);

 protected:
  virtual void InitNavigationPoint();

  // link specify
  void GetNeighbors(const Vector query, std::vector<Neighbor>& resset, std::vector<Neighbor>& fullset,
                    boost::dynamic_bitset<>& has_calculated_dist);

  // FindUnconnectedNode
  void GetNeighbors(const Vector query, std::vector<Neighbor>& resset, std::vector<Neighbor>& fullset);

  // navigation-point
  void GetNeighbors(const Vector query, std::vector<Neighbor>& resset, Graph& graph, SearchParams* param = nullptr);

  // only for search
  // void
  // GetNeighbors(const float* query, node_t* I, float* D, SearchParams* params);

  void Link();

  void SyncPrune(size_t q, std::vector<Neighbor>& pool, boost::dynamic_bitset<>& has_calculated, float* cut_graph_dist);

  void SelectEdge(unsigned& cursor, std::vector<Neighbor>& sort_pool, std::vector<Neighbor>& result, bool limit = false);

  void InterInsert(unsigned n, std::vector<std::mutex>& mutex_vec, float* dist);

  void CheckConnectivity();

  void DFS(size_t root, boost::dynamic_bitset<>& flags, int64_t& count);

  void FindUnconnectedNode(boost::dynamic_bitset<>& flags, int64_t& root);
};

}  // namespace index
}  // namespace engine
}  // namespace vectordb
