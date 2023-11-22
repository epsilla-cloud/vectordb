#include "db/index/nsg/nsg.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stack>
#include <string>
#include <utility>

#include "db/index/nsg/nsg_helper.hpp"
#include "utils/builder_suspend.hpp"

namespace vectordb {
namespace engine {
namespace index {

unsigned int seed = 100;

NsgIndex::NsgIndex(const size_t& dimension, const size_t& n, Metric_Type metric)
    : dimension(dimension), ntotal(n), metric_type(metric) {
  switch (metric) {
    case Metric_Type::Metric_Type_L2:
      distance_ = new DistanceL2;
      break;
    case Metric_Type::Metric_Type_IP:
      distance_ = new DistanceIP;
      break;
    case Metric_Type::Metric_Type_COSINE:
      distance_ = new DistanceCosine;
      break;
    default:
      distance_ = new DistanceL2;
      break;
  }
}

NsgIndex::~NsgIndex() {
  // delete[] ori_data_;
  delete[] ids_;
  delete distance_;
}

size_t NsgIndex::Build(size_t nb, VectorTable data, const int64_t* ids, const BuildParams& parameters) {
  std::cout << "Start build" << std::endl;
  ntotal = nb;
  // ori_data_ = new float[ntotal * dimension];
  ids_ = new int64_t[ntotal];
  // TODO: Check if need to use memcpy
  ori_data_ = data;
  // memcpy((void*)ori_data_, (void*)data, sizeof(float) * ntotal * dimension);
  if (ids == nullptr) {
    for (size_t i = 0; i < nb; i++) {
      ids_[i] = i;
    }
  } else {
    memcpy((void*)ids_, (void*)ids, sizeof(int64_t) * ntotal);
  }

  search_length = parameters.search_length;
  out_degree = parameters.out_degree;
  candidate_pool_size = parameters.candidate_pool_size;

  std::cout << "Init navigation" << std::endl;

  // TimeRecorder rc("NSG", 1);
  InitNavigationPoint();
  // rc.RecordSection("init");

  std::cout << "Link" << std::endl;
  Link();
  // rc.RecordSection("Link");

  std::cout << "Check connectivity" << std::endl;
  CheckConnectivity();
  // rc.RecordSection("Connect");
  // rc.ElapseFromBegin("finish");

  is_trained = true;

  size_t total_degree = 0;
  for (size_t i = 0; i < ntotal; ++i) {
    total_degree += nsg[i].size();
  }

  return total_degree;
  // LOG_DEBUG_ << "Graph physical size: " << total_degree * sizeof(node_t) / 1024 / 1024 << "m";
  // LOG_DEBUG_ << "Average degree: " << total_degree / ntotal;

  // Debug code
  // for (size_t i = 0; i < ntotal; i++) {
  //     auto& x = nsg[i];
  //     for (size_t j = 0; j < x.size(); j++) {
  //         std::cout << "id: " << x[j] << std::endl;
  //     }
  //     std::cout << std::endl;
  // }
}

void NsgIndex::InitNavigationPoint() {
  // calculate the center of vectors
  auto center = new float[dimension];
  memset(center, 0, sizeof(float) * dimension);

  for (size_t i = 0; i < ntotal; i++) {
    for (size_t j = 0; j < dimension; j++) {
      center[j] += std::get<DenseVector>(ori_data_)[i * dimension + j];
    }
  }
  for (size_t j = 0; j < dimension; j++) {
    center[j] /= ntotal;
  }

  // select navigation point
  std::vector<Neighbor> resset;
  navigation_point = rand_r(&seed) % ntotal;  // random initialize navigating point
  GetNeighbors(center, resset, knng);
  navigation_point = resset[0].id;

  // Debug code
  // std::cout << "ep: " << navigation_point << std::endl;
  // for (int k = 0; k < resset.size(); ++k) {
  //     std::cout << "id: " << resset[k].id << ", dis: " << resset[k].distance << std::endl;
  // }
  // std::cout << std::endl;
  //
  // std::cout << "ep: " << navigation_point << std::endl;
  //
  // float r1 = distance_->Compare(center, ori_data_ + navigation_point * dimension, dimension);
  // assert(r1 == resset[0].distance);

  delete[] center;
}

// Specify Link
void NsgIndex::GetNeighbors(const float* query, std::vector<Neighbor>& resset, std::vector<Neighbor>& fullset,
                            boost::dynamic_bitset<>& has_calculated_dist) {
  auto& graph = knng;
  size_t buffer_size = search_length;

  if (buffer_size > ntotal) {
    throw("Build Error, search_length > ntotal");
    // THROW_MSG("Build Error, search_length > ntotal");
  }

  resset.resize(search_length);
  std::vector<node_t> init_ids(buffer_size);
  // std::vector<node_t> init_ids;

  {
    /*
     * copy navigation-point neighbor,  pick random node if less than buffer size
     */
    size_t count = 0;

    // Get all neighbors
    for (size_t i = 0; i < init_ids.size() && i < graph[navigation_point].size(); ++i) {
      // for (size_t i = 0; i < graph[navigation_point].size(); ++i) {
      // init_ids.push_back(graph[navigation_point][i]);
      init_ids[i] = graph[navigation_point][i];
      has_calculated_dist[init_ids[i]] = true;
      ++count;
    }
    while (count < buffer_size) {
      node_t id = rand_r(&seed) % ntotal;
      if (has_calculated_dist[id])
        continue;  // duplicate id
      // init_ids.push_back(id);
      init_ids[count] = id;
      ++count;
      has_calculated_dist[id] = true;
    }
  }

  {
    // resset.resize(init_ids.size());

    // init resset and sort by distance
    for (size_t i = 0; i < init_ids.size(); ++i) {
      node_t id = init_ids[i];

      if (id >= static_cast<node_t>(ntotal)) {
        throw("Build Index Error, id > ntotal");
        // THROW_MSG("Build Index Error, id > ntotal");
        continue;
      }

      float dist = distance_->Compare(std::get<DenseVector>(ori_data_) + dimension * id, query, dimension);
      resset[i] = Neighbor(id, dist, false);

      //// difference from other GetNeighbors
      fullset.push_back(resset[i]);
      ///////////////////////////////////////
    }
    std::sort(resset.begin(), resset.end());  // sort by distance

    // search nearest neighbor
    size_t cursor = 0;
    while (cursor < buffer_size) {
      size_t nearest_updated_pos = buffer_size;

      if (!resset[cursor].has_explored) {
        resset[cursor].has_explored = true;

        node_t start_pos = resset[cursor].id;
        auto& wait_for_search_node_vec = graph[start_pos];
        for (size_t i = 0; i < wait_for_search_node_vec.size(); ++i) {
          node_t id = wait_for_search_node_vec[i];
          if (has_calculated_dist[id])
            continue;
          has_calculated_dist[id] = true;

          float dist = distance_->Compare(query, std::get<DenseVector>(ori_data_) + dimension * id, dimension);
          Neighbor nn(id, dist, false);
          fullset.push_back(nn);

          if (dist >= resset[buffer_size - 1].distance)
            continue;

          size_t pos = InsertIntoPool(resset.data(), buffer_size, nn);  // replace with a closer node
          if (pos < nearest_updated_pos)
            nearest_updated_pos = pos;

          // assert(buffer_size + 1 >= resset.size());
          if (buffer_size + 1 < resset.size())
            ++buffer_size;
        }
      }
      if (cursor >= nearest_updated_pos) {
        cursor = nearest_updated_pos;  // re-search from new pos
      } else {
        ++cursor;
      }
    }
  }
}

// FindUnconnectedNode
void NsgIndex::GetNeighbors(const float* query, std::vector<Neighbor>& resset, std::vector<Neighbor>& fullset) {
  auto& graph = nsg;
  size_t buffer_size = search_length;

  if (buffer_size > ntotal) {
    throw("Build Error, search_length > ntotal");
    // THROW_MSG("Build Error, search_length > ntotal");
  }

  // std::vector<node_t> init_ids;
  std::vector<node_t> init_ids(buffer_size);
  resset.resize(buffer_size);
  boost::dynamic_bitset<> has_calculated_dist{ntotal, 0};

  {
    /*
     * copy navigation-point neighbor,  pick random node if less than buffer size
     */
    size_t count = 0;

    // Get all neighbors
    for (size_t i = 0; i < init_ids.size() && i < graph[navigation_point].size(); ++i) {
      // for (size_t i = 0; i < graph[navigation_point].size(); ++i) {
      // init_ids.push_back(graph[navigation_point][i]);
      init_ids[i] = graph[navigation_point][i];
      has_calculated_dist[init_ids[i]] = true;
      ++count;
    }
    while (count < buffer_size) {
      node_t id = rand_r(&seed) % ntotal;
      if (has_calculated_dist[id])
        continue;  // duplicate id
      // init_ids.push_back(id);
      init_ids[count] = id;
      ++count;
      has_calculated_dist[id] = true;
    }
  }

  {
    // resset.resize(init_ids.size());

    // init resset and sort by distance
    for (size_t i = 0; i < init_ids.size(); ++i) {
      node_t id = init_ids[i];

      if (id >= static_cast<node_t>(ntotal)) {
        throw("Build Index Error, id > ntotal");
        // THROW_MSG("Build Index Error, id > ntotal");
        continue;
      }

      float dist = distance_->Compare(std::get<DenseVector>(ori_data_) + id * dimension, query, dimension);
      resset[i] = Neighbor(id, dist, false);
    }
    std::sort(resset.begin(), resset.end());  // sort by distance

    // search nearest neighbor
    size_t cursor = 0;
    while (cursor < buffer_size) {
      size_t nearest_updated_pos = buffer_size;

      if (!resset[cursor].has_explored) {
        resset[cursor].has_explored = true;

        node_t start_pos = resset[cursor].id;
        auto& wait_for_search_node_vec = graph[start_pos];
        for (size_t i = 0; i < wait_for_search_node_vec.size(); ++i) {
          node_t id = wait_for_search_node_vec[i];
          if (has_calculated_dist[id])
            continue;
          has_calculated_dist[id] = true;

          float dist = distance_->Compare(std::get<DenseVector>(ori_data_) + dimension * id, query, dimension);
          Neighbor nn(id, dist, false);
          fullset.push_back(nn);

          if (dist >= resset[buffer_size - 1].distance)
            continue;

          size_t pos = InsertIntoPool(resset.data(), buffer_size, nn);  // replace with a closer node
          if (pos < nearest_updated_pos)
            nearest_updated_pos = pos;

          // assert(buffer_size + 1 >= resset.size());
          if (buffer_size + 1 < resset.size())
            ++buffer_size;  // trick
        }
      }
      if (cursor >= nearest_updated_pos) {
        cursor = nearest_updated_pos;  // re-search from new pos
      } else {
        ++cursor;
      }
    }
  }
}

void NsgIndex::GetNeighbors(const float* query, std::vector<Neighbor>& resset, Graph& graph, SearchParams* params) {
  size_t buffer_size = params ? params->search_length : search_length;

  if (buffer_size > ntotal) {
    throw("Build Error, search_length > ntotal");
    // THROW_MSG("Build Error, search_length > ntotal");
  }

  std::vector<node_t> init_ids(buffer_size);
  resset.resize(buffer_size);
  boost::dynamic_bitset<> has_calculated_dist{ntotal, 0};

  {
    /*
     * copy navigation-point neighbor,  pick random node if less than buffer size
     */
    size_t count = 0;

    // Get all neighbors
    for (size_t i = 0; i < init_ids.size() && i < graph[navigation_point].size(); ++i) {
      init_ids[i] = graph[navigation_point][i];
      has_calculated_dist[init_ids[i]] = true;
      ++count;
    }
    while (count < buffer_size) {
      node_t id = rand_r(&seed) % ntotal;
      if (has_calculated_dist[id])
        continue;  // duplicate id
      init_ids[count] = id;
      ++count;
      has_calculated_dist[id] = true;
    }
  }

  {
    // resset.resize(init_ids.size());

    // init resset and sort by distance
    for (size_t i = 0; i < init_ids.size(); ++i) {
      node_t id = init_ids[i];

      if (id >= static_cast<node_t>(ntotal)) {
        throw("Build Index Error, id > ntotal");
        // THROW_MSG("Build Index Error, id > ntotal");
      }

      float dist = distance_->Compare(std::get<DenseVector>(ori_data_) + id * dimension, query, dimension);
      resset[i] = Neighbor(id, dist, false);
    }
    std::sort(resset.begin(), resset.end());  // sort by distance

    // search nearest neighbor
    size_t cursor = 0;
    while (cursor < buffer_size) {
      size_t nearest_updated_pos = buffer_size;

      if (!resset[cursor].has_explored) {
        resset[cursor].has_explored = true;

        node_t start_pos = resset[cursor].id;
        auto& wait_for_search_node_vec = graph[start_pos];
        for (size_t i = 0; i < wait_for_search_node_vec.size(); ++i) {
          node_t id = wait_for_search_node_vec[i];
          if (has_calculated_dist[id])
            continue;
          has_calculated_dist[id] = true;

          float dist = distance_->Compare(query, std::get<DenseVector>(ori_data_) + dimension * id, dimension);

          if (dist >= resset[buffer_size - 1].distance)
            continue;

          //// difference from other GetNeighbors
          Neighbor nn(id, dist, false);
          ///////////////////////////////////////

          size_t pos = InsertIntoPool(resset.data(), buffer_size, nn);  // replace with a closer node
          if (pos < nearest_updated_pos)
            nearest_updated_pos = pos;

          //>> Debug code
          /////
          // std::cout << "pos: " << pos << ", nn: " << nn.id << ":" << nn.distance << ", nup: " <<
          // nearest_updated_pos << std::endl;
          /////
          // trick: avoid search query search_length < init_ids.size() ...
          if (buffer_size + 1 < resset.size())
            ++buffer_size;
        }
      }
      if (cursor >= nearest_updated_pos) {
        cursor = nearest_updated_pos;  // re-search from new pos
      } else {
        ++cursor;
      }
    }
  }
}

void NsgIndex::Link() {
  float* cut_graph_dist = new float[ntotal * out_degree];
  nsg.resize(ntotal);

#pragma omp parallel
  {
    std::vector<Neighbor> fullset;
    std::vector<Neighbor> temp;
    boost::dynamic_bitset<> flags{ntotal, 0};
#pragma omp for schedule(dynamic, 100)
    for (size_t n = 0; n < ntotal; ++n) {
      BuilderSuspend::check_wait();
      fullset.clear();
      temp.clear();
      flags.reset();
      GetNeighbors(std::get<DenseVector>(ori_data_) + dimension * n, temp, fullset, flags);
      SyncPrune(n, fullset, flags, cut_graph_dist);
    }

    // Debug code
    // std::cout << "ep: " << 0 << std::endl;
    // for (int k = 0; k < fullset.size(); ++k) {
    //     std::cout << "id: " << fullset[k].id << ", dis: " << fullset[k].distance << std::endl;
    // }
  }
  knng.clear();

  // Debug code
  // for (size_t i = 0; i < ntotal; i++)
  // {
  //     auto& x = nsg[i];
  //     for (size_t j=0; j < x.size(); j++)
  //     {
  //     std::cout << "id: " << x[j] << std::endl;
  //     }
  //     std::cout << std::endl;
  // }

  std::vector<std::mutex> mutex_vec(ntotal);
#pragma omp for schedule(dynamic, 100)
  for (unsigned n = 0; n < ntotal; ++n) {
    BuilderSuspend::check_wait();
    InterInsert(n, mutex_vec, cut_graph_dist);
  }
  delete[] cut_graph_dist;
}

void NsgIndex::SyncPrune(size_t n, std::vector<Neighbor>& pool, boost::dynamic_bitset<>& has_calculated,
                         float* cut_graph_dist) {
  // avoid lose nearest neighbor in knng
  for (size_t i = 0; i < knng[n].size(); ++i) {
    auto id = knng[n][i];
    if (has_calculated[id])
      continue;
    float dist = distance_->Compare(std::get<DenseVector>(ori_data_) + dimension * n, std::get<DenseVector>(ori_data_) + dimension * id, dimension);
    pool.emplace_back(Neighbor(id, dist, true));
  }

  // sort and find closest node
  unsigned cursor = 0;
  std::sort(pool.begin(), pool.end());
  std::vector<Neighbor> result;
  if (pool[cursor].id == static_cast<node_t>(n)) {
    cursor++;
  }
  result.push_back(pool[cursor]);  // init result with nearest neighbor

  SelectEdge(cursor, pool, result, true);

  // filling the cut_graph
  auto& des_id_pool = nsg[n];
  float* des_dist_pool = cut_graph_dist + n * out_degree;
  for (size_t i = 0; i < result.size(); ++i) {
    des_id_pool.push_back(result[i].id);
    des_dist_pool[i] = result[i].distance;
  }
  if (result.size() < out_degree) {
    des_dist_pool[result.size()] = -1;
  }
  //>> Optimize: reserve id_pool capacity
}

//>> Optimize: remove read-lock
void NsgIndex::InterInsert(unsigned n, std::vector<std::mutex>& mutex_vec, float* cut_graph_dist) {
  auto& current = n;

  auto& neighbor_id_pool = nsg[current];
  float* neighbor_dist_pool = cut_graph_dist + current * out_degree;
  for (size_t i = 0; i < out_degree; ++i) {
    if (neighbor_dist_pool[i] == -1)
      break;

    size_t current_neighbor = neighbor_id_pool[i];  // center's neighbor id
    auto& nsn_id_pool = nsg[current_neighbor];      // nsn => neighbor's neighbor
    float* nsn_dist_pool = cut_graph_dist + current_neighbor * out_degree;

    std::vector<Neighbor> wait_for_link_pool;  // maintain candidate neighbor of the current neighbor.
    int duplicate = false;
    {
      LockGuard lk(mutex_vec[current_neighbor]);
      for (size_t j = 0; j < out_degree; ++j) {
        if (nsn_dist_pool[j] == -1)
          break;

        // At least one edge can be connected back
        if (n == nsn_id_pool[j]) {
          duplicate = true;
          break;
        }

        Neighbor nsn(nsn_id_pool[j], nsn_dist_pool[j]);
        wait_for_link_pool.push_back(nsn);
      }
    }
    if (duplicate)
      continue;

    // original: (neighbor) <------- (current)
    // after:    (neighbor) -------> (current)
    // current node as a neighbor of its neighbor
    Neighbor current_as_neighbor(n, neighbor_dist_pool[i]);
    wait_for_link_pool.push_back(current_as_neighbor);

    // re-selectEdge if candidate neighbor num > out_degree
    if (wait_for_link_pool.size() > out_degree) {
      std::vector<Neighbor> result;

      unsigned start = 0;
      std::sort(wait_for_link_pool.begin(), wait_for_link_pool.end());
      result.push_back(wait_for_link_pool[start]);

      SelectEdge(start, wait_for_link_pool, result);

      {
        LockGuard lk(mutex_vec[current_neighbor]);
        for (size_t j = 0; j < result.size(); ++j) {
          nsn_id_pool[j] = result[j].id;
          nsn_dist_pool[j] = result[j].distance;
        }
      }
    } else {
      LockGuard lk(mutex_vec[current_neighbor]);
      for (size_t j = 0; j < out_degree; ++j) {
        if (nsn_dist_pool[j] == -1) {
          nsn_id_pool.push_back(current_as_neighbor.id);
          nsn_dist_pool[j] = current_as_neighbor.distance;
          if (j + 1 < out_degree)
            nsn_dist_pool[j + 1] = -1;
          break;
        }
      }
    }
  }
}

void NsgIndex::SelectEdge(unsigned& cursor, std::vector<Neighbor>& sort_pool, std::vector<Neighbor>& result, bool limit) {
  auto& pool = sort_pool;

  /*
   * edge selection
   *
   * search in pool and search deepth is under candidate_pool_size
   * max result size equal to out_degress
   */
  size_t search_deepth = limit ? candidate_pool_size : pool.size();
  while (result.size() < out_degree && cursor < search_deepth && (++cursor) < pool.size()) {
    auto& p = pool[cursor];
    bool should_link = true;
    for (size_t t = 0; t < result.size(); ++t) {
      float dist =
          distance_->Compare(std::get<DenseVector>(ori_data_) + dimension * result[t].id, std::get<DenseVector>(ori_data_) + dimension * p.id, dimension);

      if (dist < p.distance) {
        should_link = false;
        break;
      }
    }
    if (should_link)
      result.push_back(p);
  }
}

void NsgIndex::CheckConnectivity() {
  auto root = navigation_point;
  boost::dynamic_bitset<> has_linked{ntotal, 0};
  int64_t linked_count = 0;

  while (linked_count < static_cast<int64_t>(ntotal)) {
    BuilderSuspend::check_wait();
    DFS(root, has_linked, linked_count);
    if (linked_count >= static_cast<int64_t>(ntotal)) {
      break;
    }
    FindUnconnectedNode(has_linked, root);
  }
}

void NsgIndex::DFS(size_t root, boost::dynamic_bitset<>& has_linked, int64_t& linked_count) {
  size_t start = root;
  std::stack<size_t> s;
  s.push(root);
  if (!has_linked[root]) {
    linked_count++;  // not link
  }
  has_linked[root] = true;  // link start...

  while (!s.empty()) {
    size_t next = ntotal + 1;

    for (unsigned i = 0; i < nsg[start].size(); i++) {
      if (has_linked[nsg[start][i]] == false) {  // if not link
        next = nsg[start][i];
        break;
      }
    }
    if (next == (ntotal + 1)) {
      s.pop();
      if (s.empty())
        break;
      start = s.top();
      continue;
    }
    start = next;
    has_linked[start] = true;
    s.push(start);
    ++linked_count;
  }
}

void NsgIndex::FindUnconnectedNode(boost::dynamic_bitset<>& has_linked, int64_t& root) {
  // find any of unlinked-node
  size_t id = ntotal;
  for (size_t i = 0; i < ntotal; i++) {  // find not link
    if (has_linked[i] == false) {
      id = i;
      break;
    }
  }

  if (id == ntotal)
    return;  // No Unlinked Node

  // search unlinked-node's neighbor
  std::vector<Neighbor> tmp, pool;
  GetNeighbors(std::get<DenseVector>(ori_data_) + dimension * id, tmp, pool);
  std::sort(pool.begin(), pool.end());

  size_t found = 0;
  for (size_t i = 0; i < pool.size(); i++) {  // find nearest neighbor and add unlinked-node as its neighbor
    if (has_linked[pool[i].id]) {
      root = pool[i].id;
      found = 1;
      break;
    }
  }
  if (found == 0) {
    while (true) {  // random a linked-node and add unlinked-node as its neighbor
      size_t rid = rand_r(&seed) % ntotal;
      if (has_linked[rid]) {
        root = rid;
        break;
      }
    }
  }
  nsg[root].push_back(id);
}

void NsgIndex::Search(const float* query, const unsigned& nq, const unsigned& dim, const unsigned& k, float* dist,
                      int64_t* ids, SearchParams& params, ConcurrentBitsetPtr bitset) {
  std::vector<std::vector<Neighbor>> resset(nq);

  // TimeRecorder rc("NsgIndex::search", 1);
  if (nq == 1) {
    GetNeighbors(query, resset[0], nsg, &params);
  } else {
#pragma omp parallel for
    for (unsigned int i = 0; i < nq; ++i) {
      const float* single_query = query + i * dim;
      GetNeighbors(single_query, resset[i], nsg, &params);
    }
  }
  // rc.RecordSection("search");

  bool is_ip = (metric_type == Metric_Type::Metric_Type_IP);
  for (unsigned int i = 0; i < nq; ++i) {
    unsigned int pos = 0;
    for (unsigned int j = 0; j < resset[i].size(); ++j) {
      if (pos >= k)
        break;  // already top k
      if (!bitset || !bitset->test((ConcurrentBitset::id_type_t)resset[i][j].id)) {
        ids[i * k + pos] = ids_[resset[i][j].id];
        dist[i * k + pos] = is_ip ? -resset[i][j].distance : resset[i][j].distance;
        ++pos;
      }
    }
    // fill with -1
    for (unsigned int j = pos; j < k; ++j) {
      ids[i * k + j] = -1;
      dist[i * k + j] = -1;
    }
  }
  // rc.RecordSection("merge");
}

void NsgIndex::SetKnnGraph(Graph& g) {
  knng = std::move(g);
}

int64_t
NsgIndex::GetSize() {
  int64_t ret = 0;
  ret += sizeof(*this);
  ret += ntotal * dimension * sizeof(float);
  ret += ntotal * sizeof(int64_t);
  ret += sizeof(*distance_);
  for (auto i = 0; i < nsg.size(); ++i) {
    ret += nsg[i].size() * sizeof(node_t);
  }
  for (auto i = 0; i < knng.size(); ++i) {
    ret += knng[i].size() * sizeof(node_t);
  }
  return ret;
}

}  // namespace index
}  // namespace engine
}  // namespace vectordb
