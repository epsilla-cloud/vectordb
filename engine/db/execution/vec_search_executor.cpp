#include "db/execution/vec_search_executor.hpp"
#include "query/expr/expr.hpp"

#include <omp.h>

#include <algorithm>
#include <numeric>

#include "utils/atomic_counter.hpp"

namespace vectordb {
namespace engine {
namespace execution {

namespace {
int64_t GetIndexMedian(const std::vector<int64_t> &ids) {
  std::vector<int64_t> tmp_ids(ids);
  std::sort(tmp_ids.begin(), tmp_ids.end());

  return tmp_ids[tmp_ids.size() / 2];
}

int64_t GetIndexMean(const std::vector<int64_t> &ids) {
  int64_t sum = std::accumulate(ids.begin(), ids.end(), static_cast<int64_t>(0));
  return sum / ids.size();
}
}  // namespace

VecSearchExecutor::VecSearchExecutor(
    const int64_t ntotal,
    const int64_t dimension,
    const int64_t start_search_point,
    std::shared_ptr<ANNGraphSegment> ann_index,
    int64_t *offset_table,
    int64_t *neighbor_list,
    float *vector_table,
    DISTFUNC<float> fstdistfunc,
    void *dist_func_param,
    int num_threads,
    int64_t L_master,
    int64_t L_local,
    int64_t subsearch_iterations)
    : total_indexed_vector_(ntotal),
      dimension_(dimension),
      start_search_point_(start_search_point),
      offset_table_(offset_table),
      neighbor_list_(neighbor_list),
      vector_table_(vector_table),
      fstdistfunc_(fstdistfunc),
      dist_func_param_(dist_func_param),
      num_threads_(num_threads),
      L_master_(L_master),
      L_local_(L_local),
      subsearch_iterations_(subsearch_iterations),
      search_result_(L_master),
      distance_(L_master),
      init_ids_(L_master),
      is_visited_(ntotal),
      set_L_((num_threads - 1) * L_local + L_master),
      local_queues_sizes_(num_threads, 0),
      local_queues_starts_(num_threads),
      brute_force_search_(ntotal < BruteforceThreshold),
      brute_force_queue_(BruteforceThreshold) {
  ann_index_ = ann_index;
  for (int q_i = 0; q_i < num_threads; ++q_i) {
    local_queues_starts_[q_i] = q_i * L_local;
  }
  if (!brute_force_search_) {
    PrepareInitIds(init_ids_, L_master);
  }
  omp_set_num_threads(num_threads_);
}

int64_t VecSearchExecutor::AddIntoQueue(
    std::vector<Candidate> &queue,
    const int64_t queue_start,
    int64_t &queue_size,           // The insertion location starting from queue_start
    const int64_t queue_capacity,  // The maximum capacity of queue, independent with queue_start.
    const Candidate &cand) {
  // If queue is empty, directly add into queue.
  if (queue_size == 0) {
    queue[queue_start + queue_size++] = cand;
    return 0;
  }
  int64_t queue_end = queue_start + queue_size;
  // Find the insert location in the priority queue based on distance ascendingly.
  const auto it_loc = std::lower_bound(queue.begin() + queue_start, queue.begin() + queue_end, cand);
  int64_t insert_loc = it_loc - queue.begin();

  if (insert_loc != queue_end) {
    if (cand.id_ == it_loc->id_) {
      // Duplicate
      return queue_capacity;
    }
    if (queue_size >= queue_capacity) {  // Queue is full
      --queue_size;
      --queue_end;
    }
  } else {                              // insert_loc == queue_end, insert at the end?
    if (queue_size < queue_capacity) {  // Queue is not full
      // Insert at the end
      queue[insert_loc] = cand;
      ++queue_size;
      return queue_size - 1;
    } else {  // Queue is full
      return queue_capacity;
    }
  }
  // Add into queue
  memmove(reinterpret_cast<char *>(queue.data() + insert_loc + 1),
          reinterpret_cast<char *>(queue.data() + insert_loc),
          (queue_end - insert_loc) * sizeof(Candidate));
  queue[insert_loc] = cand;
  ++queue_size;
  return insert_loc - queue_start;
}

void VecSearchExecutor::AddIntoQueueAt(
    const Candidate &cand,
    std::vector<Candidate> &queue,
    const int64_t insert_index,  // The insertion location, independent with queue_start
    const int64_t queue_start,
    int64_t &queue_size,  // The number of elements in queue, independent with queue_start
    const int64_t queue_capacity) {
  const int64_t dest_index = queue_start + insert_index;
  if (queue_size == queue_capacity) {
    --queue_size;
  }
  memmove(reinterpret_cast<char *>(queue.data() + dest_index + 1),
          reinterpret_cast<char *>(queue.data() + dest_index),
          (queue_size - insert_index) * sizeof(Candidate));
  queue[dest_index] = cand;
  ++queue_size;
}

void VecSearchExecutor::InsertOneElementAt(
    const Candidate &cand,
    std::vector<Candidate> &queue,
    const int64_t insert_index,
    const int64_t queue_start,
    const int64_t queue_size) {
  const int64_t dest_index = queue_start + insert_index;
  memmove(reinterpret_cast<char *>(queue.data() + dest_index + 1),
          reinterpret_cast<char *>(queue.data() + dest_index),
          (queue_size - insert_index - 1) * sizeof(Candidate));
  queue[dest_index] = cand;
}

int64_t VecSearchExecutor::MergeTwoQueuesInto1stQueueSeqFixed(
    std::vector<Candidate> &queue1,
    const int64_t queue1_start,
    const int64_t queue1_size,
    std::vector<Candidate> &queue2,
    const int64_t queue2_start,
    const int64_t queue2_size) {
  assert(queue1_size && queue2_size);
  // Record the lowest insert location.
  auto it_loc = std::lower_bound(
      queue1.begin() + queue1_start,
      queue1.begin() + queue1_start + queue1_size,
      queue2[queue2_start]);
  int64_t insert_index = it_loc - (queue1.begin() + queue1_start);
  if (insert_index == queue1_size) {
    return insert_index;
  } else if (insert_index == queue1_size - 1) {
    queue1[queue1_start + insert_index] = queue2[queue2_start];
    return insert_index;
  }

  // Insert the 1st of queue2
  if (queue2[queue2_start].id_ != it_loc->id_) {
    // Not Duplicate
    InsertOneElementAt(
        queue2[queue2_start],
        queue1,
        insert_index,
        queue1_start,
        queue1_size);
  } else if (!queue2[queue2_start].is_checked_ && it_loc->is_checked_) {
    it_loc->is_checked_ = false;
  }
  if (queue2_size == 1) {
    return insert_index;
  }

  // Insert
  int64_t q_i_1 = insert_index + 1 + queue1_start;
  int64_t q_i_2 = queue2_start + 1;
  const int64_t q_i_1_bound = queue1_start + queue1_size;
  const int64_t q_i_2_bound = queue2_start + queue2_size;
  for (int64_t insert_i = insert_index + 1; insert_i < queue1_size; ++insert_i) {
    if (q_i_1 >= q_i_1_bound || q_i_2 >= q_i_2_bound) {
      break;
    } else if (queue1[q_i_1] < queue2[q_i_2]) {
      ++q_i_1;
    } else if (queue2[q_i_2] < queue1[q_i_1]) {
      // Insert queue2[q_i_2] into queue1
      InsertOneElementAt(
          queue2[q_i_2++],
          queue1,
          insert_i,
          queue1_start,
          queue1_size);
      ++q_i_1;
    } else {
      // Duplicate
      if (!queue2[q_i_2].is_checked_ && queue1[q_i_1].is_checked_) {
        queue1[q_i_1].is_checked_ = false;
      }
      ++q_i_2;
      ++q_i_1;
    }
  }

  return insert_index;
}

int64_t VecSearchExecutor::MergeTwoQueuesInto1stQueueSeqIncr(
    std::vector<Candidate> &queue1,
    const int64_t queue1_start,
    int64_t &queue1_size,
    const int64_t queue1_length,
    std::vector<Candidate> &queue2,
    const int64_t queue2_start,
    const int64_t queue2_size) {
  assert(queue1_size && queue2_size);
  // Record the lowest insert location.
  auto it_loc = std::lower_bound(
      queue1.begin() + queue1_start,
      queue1.begin() + queue1_start + queue1_size,
      queue2[queue2_start]);
  int64_t insert_index = it_loc - (queue1.begin() + queue1_start);
  if (insert_index == queue1_size) {
    int64_t copy_count = (queue1_size + queue2_size > queue1_length) ? queue1_length - queue1_size : queue2_size;
    memmove(queue1.data() + queue1_start + queue1_size,
            queue2.data() + queue2_start,
            copy_count * sizeof(Candidate));
    queue1_size += copy_count;
    return insert_index;
  }
  if (queue2[queue2_start].id_ != it_loc->id_) {
    // Not Duplicate
    AddIntoQueueAt(
        queue2[queue2_start],
        queue1,
        insert_index,
        queue1_start,
        queue1_size,
        queue1_length);
  } else if (!queue2[queue2_start].is_checked_ && it_loc->is_checked_) {
    it_loc->is_checked_ = false;
  }
  if (queue2_size == 1) {
    return insert_index;
  }

  // Insert
  int64_t q_i_1 = insert_index + 1 + queue1_start;
  int64_t q_i_2 = queue2_start + 1;
  int64_t q_i_1_bound = queue1_start + queue1_size;  // When queue1_size is updated, so should be q_i_1_bound.
  const int64_t q_i_2_bound = queue2_start + queue2_size;

  for (int64_t insert_i = insert_index + 1; insert_i < queue1_length; ++insert_i) {
    if (q_i_1 >= q_i_1_bound) {
      queue1_size += std::min(queue1_length - insert_i, q_i_2_bound - q_i_2);
      for (; insert_i < queue1_size; ++insert_i) {
        queue1[queue1_start + insert_i] = queue2[q_i_2++];
      }
      break;
    } else if (q_i_2 >= q_i_2_bound) {
      break;
    } else if (queue1[q_i_1] < queue2[q_i_2]) {
      ++q_i_1;
    } else if (queue2[q_i_2] < queue1[q_i_1]) {
      AddIntoQueueAt(
          queue2[q_i_2++],
          queue1,
          insert_i,
          queue1_start,
          queue1_size,
          queue1_length);
      ++q_i_1;
      q_i_1_bound = queue1_start + queue1_size;
    } else {
      // Duplicate
      if (!queue2[q_i_2].is_checked_ && queue1[q_i_1].is_checked_) {
        queue1[q_i_1].is_checked_ = false;
      }
      ++q_i_2;
      ++q_i_1;
    }
  }
  return insert_index;
}

int64_t VecSearchExecutor::MergeAllQueuesToMaster(
    std::vector<Candidate> &set_L,
    const std::vector<int64_t> &local_queues_starts,
    std::vector<int64_t> &local_queues_sizes,
    const int64_t local_queue_capacity,
    const int64_t L) const {
  int64_t nk = L;
  const int64_t master_start = local_queues_starts[num_threads_ - 1];
  const int64_t w_i_bound = num_threads_ - 1;
  for (int64_t w_i = 0; w_i < w_i_bound; ++w_i) {
    const int64_t w_start = local_queues_starts[w_i];
    int64_t &w_size = local_queues_sizes[w_i];
    if (w_size == 0) {
      continue;
    }
    int64_t r = MergeTwoQueuesInto1stQueueSeqFixed(
        set_L,
        master_start,
        L,
        set_L,
        w_start,
        w_size);
    if (r < nk) {
      nk = r;
    }
    w_size = 0;
  }

  return nk;
}

int64_t VecSearchExecutor::PickTopMToWorkers(
    std::vector<Candidate> &set_L,
    const std::vector<int64_t> &local_queues_starts,
    std::vector<int64_t> &local_queues_sizes,
    const int64_t local_queue_capacity,
    const int64_t k_uc) const {
  const int64_t last_queue_start = local_queues_starts[num_threads_ - 1];
  int64_t c_i_start = k_uc + last_queue_start;
  int64_t c_i_bound = last_queue_start + local_queues_sizes[num_threads_ - 1];
  int64_t top_m_count = 0;  // number of unchecked
  int dest_queue = 0;
  for (int64_t c_i = c_i_start; c_i < c_i_bound; ++c_i) {
    if (set_L[c_i].is_checked_) {
      continue;
    }
    ++top_m_count;
    if (num_threads_ - 1 != dest_queue) {
      set_L[local_queues_starts[dest_queue] + local_queues_sizes[dest_queue]++] = set_L[c_i];
      set_L[c_i].is_checked_ = true;
      if (local_queues_sizes[dest_queue] == local_queue_capacity) {
        break;
      }
      ++dest_queue;
    } else {
      dest_queue = 0;
    }
  }
  return top_m_count;
}

void VecSearchExecutor::PickTopMUnchecked(
    const int64_t M,
    const int64_t k_uc,
    std::vector<Candidate> &set_L,
    const int64_t local_queue_start,
    const int64_t local_queue_size,
    std::vector<int64_t> &top_m_candidates,
    int64_t &top_m_candidates_size,
    int64_t &last_k) const {
  int64_t tmp_last_k = local_queue_size;
  int64_t tmc_size = 0;
  int64_t c_i_start = local_queue_start + k_uc;
  int64_t c_i_bound = local_queue_start + local_queue_size;
  // Pick top-M
  for (int64_t c_i = c_i_start; c_i < c_i_bound && tmc_size < M; ++c_i) {
    if (set_L[c_i].is_checked_) {
      continue;
    }
    tmp_last_k = c_i - local_queue_start;  // Record the location of the last candidate selected.
    set_L[c_i].is_checked_ = true;
    top_m_candidates[tmc_size++] = set_L[c_i].id_;
  }
  last_k = tmp_last_k;
  top_m_candidates_size = tmc_size;
}

int64_t VecSearchExecutor::ExpandOneCandidate(
    const int worker_id,
    const int64_t cand_id,
    const float *query_data,
    const float &dist_bound,
    // float &dist_thresh,
    std::vector<Candidate> &set_L,
    const int64_t local_queue_start,
    int64_t &local_queue_size,
    const int64_t &local_queue_capacity,
    boost::dynamic_bitset<> &is_visited,
    uint64_t &local_count_computation) {
  uint64_t tmp_count_computation = 0;

  int64_t nk = local_queue_capacity;

  for (int64_t e_i = offset_table_[cand_id]; e_i < offset_table_[cand_id + 1]; ++e_i) {
    int64_t nb_id = neighbor_list_[e_i];
    {  // Sequential edition
      if (is_visited[nb_id]) {
        continue;
      }
      is_visited[nb_id] = true;
    }

    ++tmp_count_computation;
    float dist = fstdistfunc_(vector_table_ + dimension_ * nb_id, query_data, dist_func_param_);
    if (dist > dist_bound) {
      // if (dist > dist_bound || dist > dist_thresh) {
      continue;
    }
    Candidate cand(nb_id, dist, false);
    // Add to the local queue.
    int64_t r = AddIntoQueue(
        set_L,
        local_queue_start,
        local_queue_size,
        local_queue_capacity,
        cand);
    if (r < nk) {
      nk = r;
    }
  }

  local_count_computation += tmp_count_computation;

  return nk;
}

void VecSearchExecutor::InitializeSetLPara(
    const float *query_data,
    const int64_t L,
    std::vector<Candidate> &set_L,
    const int64_t set_L_start,
    int64_t &set_L_size,
    const std::vector<int64_t> &init_ids,
    boost::dynamic_bitset<> &is_visited) {
  // #pragma omp parallel for
  for (int64_t c_i = 0; c_i < L; ++c_i) {
    is_visited[init_ids[c_i]] = true;
  }

  // Get the distances of all candidates, store in the set set_L.
  uint64_t tmp_count_computation = 0;
#pragma omp parallel for reduction(+ : tmp_count_computation)
  for (int64_t i = 0; i < L; i++) {
    int64_t v_id = init_ids[i];
    ++tmp_count_computation;
    float dist = fstdistfunc_(vector_table_ + dimension_ * v_id, query_data, dist_func_param_);
    set_L[set_L_start + i] = Candidate(v_id, dist, false);  // False means not checked.
  }
  // count_distance_computation_ += tmp_count_computation;

  std::sort(
      set_L.begin() + set_L_start,
      set_L.begin() + set_L_start + L);
  set_L_size = L;
}

void VecSearchExecutor::PrepareInitIds(
    std::vector<int64_t> &init_ids,
    const int64_t L) const {
  boost::dynamic_bitset<> is_selected(total_indexed_vector_);
  int64_t init_ids_end = 0;
  for (
      int64_t e_i = offset_table_[start_search_point_];
      e_i < offset_table_[start_search_point_ + 1] && init_ids_end < L;
      ++e_i) {
    int64_t v_id = neighbor_list_[e_i];
    if (is_selected[v_id]) {
      continue;
    }
    is_selected[v_id] = true;
    init_ids[init_ids_end++] = v_id;
  }

  int64_t tmp_id = start_search_point_ + 1;
  while (init_ids_end < L) {
    if (tmp_id == total_indexed_vector_) {
      tmp_id = 0;
    }
    int64_t v_id = tmp_id++;
    if (is_selected[v_id]) {
      continue;
    }
    is_selected[v_id] = true;
    init_ids[init_ids_end++] = v_id;
  }
}

void VecSearchExecutor::SearchImpl(
    const float *query_data,
    const int64_t K,
    const int64_t L,
    std::vector<Candidate> &set_L,
    const std::vector<int64_t> &init_ids,
    std::vector<int64_t> &set_K,
    const int64_t local_queue_capacity,  // Maximum size of local queue
    const std::vector<int64_t> &local_queues_starts,
    std::vector<int64_t> &local_queues_sizes,  // Sizes of local queue
    boost::dynamic_bitset<> &is_visited,
    const int64_t subsearch_iterations) {
  // Set thread parallel.
  omp_set_num_threads(num_threads_);
  // const int64_t index_threshold) { // AUP optimization.
  const int64_t master_queue_start = local_queues_starts[num_threads_ - 1];
  int64_t &master_queue_size = local_queues_sizes[num_threads_ - 1];

  // Initialization Phase
  InitializeSetLPara(
      query_data,
      L,
      set_L,
      master_queue_start,
      master_queue_size,
      init_ids,
      is_visited);

  const float &last_dist = set_L[master_queue_start + master_queue_size - 1].distance_;
  int64_t iter = 0;

  {
    int64_t k_master = 0;  // Index of first unchecked candidate.
    int64_t para_iter = 0;
    uint64_t tmp_count_computation = 0;

    // Sequential Start
    bool no_need_to_continue = false;
    {
      int64_t r;
      const int64_t seq_iter_bound = 1;
      while (iter < seq_iter_bound) {
        ++iter;
        if (k_master == L) {
          no_need_to_continue = true;
          break;
        }
        // float dist_thresh = last_dist;
        auto &cand = set_L[master_queue_start + k_master];
        if (!cand.is_checked_) {
          cand.is_checked_ = true;
          int64_t cand_id = cand.id_;
          r = ExpandOneCandidate(
              0,
              cand_id,
              query_data,
              last_dist,
              // dist_thresh,
              set_L,
              master_queue_start,
              master_queue_size,
              L,
              is_visited,
              tmp_count_computation);
          // count_distance_computation_ += tmp_count_computation;
          tmp_count_computation = 0;
        } else {
          r = L;
        }
        if (r <= k_master) {
          k_master = r;
        } else {
          ++k_master;
        }
      }
    }

    // AUP optimization.
    // std::vector<int64_t> local_best_index(num_threads_);
    // int checker_id = 0;
    // bool need_merge = false;

    // Parallel Phase
    while (!no_need_to_continue) {
      ++iter;
      ++para_iter;
      // Pick and copy top-M unchecked from Master to other workers
      if (!PickTopMToWorkers(
              set_L,
              local_queues_starts,
              local_queues_sizes,
              local_queue_capacity,
              k_master)) {
        break;
      }
      float dist_thresh = last_dist;
      AtomicCounter thread_counter;
      thread_counter.SetValue(0);
#pragma omp parallel reduction(+ : tmp_count_computation)
      {
        int w_i = thread_counter.GetAndIncrement();
        const int64_t local_queue_start = local_queues_starts[w_i];
        int64_t &local_queue_size = local_queues_sizes[w_i];
        const int64_t queue_capacity = local_queue_capacity;
        int64_t k_uc = num_threads_ - 1 != w_i ? 0 : k_master;
        int64_t cand_id;
        int64_t r;
        int64_t worker_iter = 0;
        // {
        //   local_best_index[w_i] = 0;
        // }
        while (worker_iter < subsearch_iterations && k_uc < local_queue_size) {
          // while (!need_merge && k_uc < local_queue_size) {
          auto &cand = set_L[local_queue_start + k_uc];
          if (!cand.is_checked_) {
            cand.is_checked_ = true;
            ++worker_iter;
            cand_id = cand.id_;
            r = ExpandOneCandidate(
                w_i,
                cand_id,
                query_data,
                last_dist,
                // dist_thresh,
                set_L,
                local_queue_start,
                local_queue_size,
                queue_capacity,
                is_visited,
                tmp_count_computation);
            if (r <= k_uc) {
              k_uc = r;
            } else {
              ++k_uc;
            }
            // AUP optimization.
            // {  // update local_best_firsts
            //   if (r == local_queue_capacity) {
            //     --r;
            //   }
            //   local_best_index[w_i] = r;
            // }
            // {  // Check the metrics
            //   if (w_i == checker_id) {
            //     if (GetIndexMean(local_best_index) >= index_threshold) {
            //       need_merge = true;
            //     }

            //     ++checker_id;
            //     if (checker_id == num_threads_) {
            //       checker_id = 0;
            //     }
            //   }
            // }
          } else {
            ++k_uc;
          }
          if (num_threads_ - 1 == w_i) {
            k_master = k_uc;
          }
        }  // Expand Top-1
      }    // Workers
      // count_distance_computation_ += tmp_count_computation;
      tmp_count_computation = 0;

      // Merge
      {
        int64_t r = MergeAllQueuesToMaster(
            set_L,
            local_queues_starts,
            local_queues_sizes,
            local_queue_capacity,
            L);
        if (r <= k_master) {
          k_master = r;
        }
        // AUP optimization.
        // need_merge = false;
      }

    }  // Search Iterations
  }    // Parallel Phase

  // #pragma omp parallel for
  //   // TODO: exclude deleted and not passing filter records.
  //   for (int64_t k_i = 0; k_i < K; ++k_i) {
  //     set_K[k_i] = set_L[k_i + master_queue_start].id_;
  //   }
  // for (int64_t k_i = 0; k_i < K; ++k_i) {
  //   std::cout << set_L[k_i + master_queue_start].distance_ << " " << std::endl;
  // }
  // std::cout << std::endl;

  {  // Reset
    is_visited.reset();
  }
}

std::unordered_map<std::string, std::any> GenFieldValueMap(
  std::shared_ptr<vectordb::engine::TableSegmentMVP>& table_segment,
  std::unordered_map<std::string, meta::FieldType>& field_name_type_map,
  const int& root_node_index,
  const int& ind
) {
  std::unordered_map<std::string, std::any> field_value_map;

  if (root_node_index >= 0) {
    for (auto& field : field_name_type_map) {
      std::string field_name = field.first;
      meta::FieldType field_type = field.second;
      if (
        field_type == meta::FieldType::VECTOR_DOUBLE ||
        field_type == meta::FieldType::VECTOR_FLOAT ||
        field_type == meta::FieldType::JSON ||
        field_type == meta::FieldType::UNKNOWN
      ) {
        continue;
      } else if (field_type == meta::FieldType::STRING) {
        auto offset = table_segment->field_name_mem_offset_map_[field_name] + ind * table_segment->string_num_;
        field_value_map.insert_or_assign(field_name, table_segment->string_table_[offset]);
      } else {
        auto offset = table_segment->field_name_mem_offset_map_[field_name] + ind * table_segment->primitive_offset_;
        switch (field_type) {
          case meta::FieldType::INT1: {
            int8_t *ptr = reinterpret_cast<int8_t *>(
                &table_segment->attribute_table_[offset]);
            field_value_map.insert_or_assign(field_name, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT2: {
            int16_t *ptr = reinterpret_cast<int16_t *>(
                &table_segment->attribute_table_[offset]);
            field_value_map.insert_or_assign(field_name, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT4: {
            int32_t *ptr = reinterpret_cast<int32_t *>(
                &table_segment->attribute_table_[offset]);
            field_value_map.insert_or_assign(field_name, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::INT8: {
            int64_t *ptr = reinterpret_cast<int64_t *>(
                &table_segment->attribute_table_[offset]);
            field_value_map.insert_or_assign(field_name, (int64_t)(*ptr));
            break;
          }
          case meta::FieldType::FLOAT: {
            float *ptr = reinterpret_cast<float *>(
                &table_segment->attribute_table_[offset]);
            field_value_map.insert_or_assign(field_name, (double)(*ptr));
            break;
          }
          case meta::FieldType::DOUBLE: {
            double *ptr = reinterpret_cast<double *>(
                &table_segment->attribute_table_[offset]);
            field_value_map.insert_or_assign(field_name, *ptr);
            break;
          }
          case meta::FieldType::BOOL: {
            bool *ptr = reinterpret_cast<bool *>(
                &table_segment->attribute_table_[offset]);
            field_value_map.insert_or_assign(field_name, *ptr);
            break;
          }
        }
      }
    }
  }

  return field_value_map;
}

bool VecSearchExecutor::BruteForceSearch(
  const float *query_data,
  const int64_t start,
  const int64_t end,
  const ConcurrentBitset &deleted,
  std::shared_ptr<vectordb::query::expr::ExprEvaluator>& expr_evaluator,
  std::shared_ptr<vectordb::engine::TableSegmentMVP>& table_segment,
  std::unordered_map<std::string, meta::FieldType>& field_name_type_map,
  const int root_node_index
) {
  if (brute_force_queue_.size() < end - start) {
    brute_force_queue_.resize(end - start);
  }
#pragma omp parallel for
  for (int64_t v_id = start; v_id < end; ++v_id) {
    float dist = fstdistfunc_(vector_table_ + dimension_ * v_id, query_data, dist_func_param_);
    brute_force_queue_[v_id - start] = Candidate(v_id, dist, false);
  }

  // compacting the result with 2 pointers by removing the deleted entries
  // iter: the iterator to loop through the brute force queue
  // num_result: the pointer to the current right-most slot in the new result
  int64_t iter = 0,
          num_result = 0;
  // remove the invalid entries
  for (; iter < end - start; ++iter) {
    auto field_value_map = GenFieldValueMap(table_segment, field_name_type_map, root_node_index, iter);
    if (!deleted.test(iter) && expr_evaluator->LogicalEvaluate(root_node_index, field_value_map)) {
      if (iter != num_result) {
        brute_force_queue_[num_result] = brute_force_queue_[iter];
      }
      num_result++;
    }
  }
  brute_force_queue_.resize(num_result);
  std::sort(
      brute_force_queue_.begin(),
      brute_force_queue_.end());
  return true;
}

Status VecSearchExecutor::Search(
  const float *query_data,
  const ConcurrentBitset &deleted,
  const size_t limit,
  std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
  std::shared_ptr<vectordb::engine::TableSegmentMVP>& table_segment,
  std::unordered_map<std::string, meta::FieldType>& field_name_type_map,
  int64_t &result_size
) {
  int64_t total_vector = table_segment->record_number_;
  auto expr_evaluator = std::make_shared<vectordb::query::expr::ExprEvaluator>(filter_nodes);
  int filter_root_index = filter_nodes.size() - 1;
  // currently the max returned result is L_local_
  // TODO: support larger search results
  std::cout << "search with limit: " << limit << " brute force: " << brute_force_search_
            << " indexed_vector: " << total_indexed_vector_ << ", total vector: "
            << total_vector << std::endl;

  if (brute_force_search_) {
    BruteForceSearch(query_data, 0, total_vector, deleted, expr_evaluator, table_segment, field_name_type_map, filter_root_index);
    result_size = std::min({brute_force_queue_.size(), limit, size_t(L_local_)});
    for (int64_t k_i = 0; k_i < result_size; ++k_i) {
      search_result_[k_i] = brute_force_queue_[k_i].id_;
      distance_[k_i] = brute_force_queue_[k_i].distance_;
    }
  } else {
    // warning, this value cannot exceed LocalQueueSize (we don't check it here because it will
    // create circular dependency)
    auto searchLimit = std::min({size_t(total_indexed_vector_), limit, size_t(L_local_)});
    SearchImpl(
        query_data,
        searchLimit,
        L_master_,
        set_L_,
        init_ids_,
        search_result_,
        L_local_,
        local_queues_starts_,
        local_queues_sizes_,
        is_visited_,
        subsearch_iterations_);
    if (total_vector > total_indexed_vector_) {
      // Need to brute force search the newly added but haven't been indexed vectors.
      BruteForceSearch(query_data, total_indexed_vector_, total_vector, deleted, expr_evaluator, table_segment, field_name_type_map, filter_root_index);
      // Merge the brute force results into the search result.
      const int64_t master_queue_start = local_queues_starts_[num_threads_ - 1];
      auto bruteForceQueueSize = std::min({brute_force_queue_.size(), limit});
      MergeTwoQueuesInto1stQueueSeqFixed(
          set_L_,
          master_queue_start,
          searchLimit,
          brute_force_queue_,
          0,
          bruteForceQueueSize);

      result_size = 0;
      auto candidateNum = std::min({size_t(L_master_), size_t(total_vector)});
      for (int64_t k_i = 0; k_i < candidateNum && result_size < searchLimit; ++k_i) {
        auto id = set_L_[k_i + master_queue_start].id_;
        auto field_value_map = GenFieldValueMap(table_segment, field_name_type_map, filter_root_index, id);
        if (deleted.test(id) && !expr_evaluator->LogicalEvaluate(filter_root_index, field_value_map)) {
          continue;
        }
        search_result_[result_size] = set_L_[k_i + master_queue_start].id_;
        distance_[result_size] = set_L_[k_i + master_queue_start].distance_;
        result_size++;
      }
    } else {
      result_size = 0;
      auto candidateNum = std::min({size_t(L_master_), size_t(total_indexed_vector_)});
      const int64_t master_queue_start = local_queues_starts_[num_threads_ - 1];
      for (int64_t k_i = 0; k_i < candidateNum && result_size < searchLimit; ++k_i) {
        auto id = set_L_[k_i + master_queue_start].id_;
        auto field_value_map = GenFieldValueMap(table_segment, field_name_type_map, filter_root_index, id);
        if (deleted.test(id) && !expr_evaluator->LogicalEvaluate(filter_root_index, field_value_map)) {
          continue;
        }
        search_result_[result_size] = set_L_[k_i + master_queue_start].id_;
        distance_[result_size] = set_L_[k_i + master_queue_start].distance_;
        result_size++;
      }
    }
    search_result_.resize(result_size);
    distance_.resize(result_size);
  }
  return Status::OK();
}

}  // namespace execution
}  // namespace engine
}  // namespace vectordb
