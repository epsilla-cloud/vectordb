#include "db/execution/vec_search_executor.hpp"
#include "utils/safe_memory_ops.hpp"
#include "utils/memory_pool.hpp"
#include "config/config.hpp"  // For ConfigLimits::DetectCgroupCpuQuota()

#include <omp.h>

#include <algorithm>
#include <atomic>
#include <numeric>
#include <cstdlib>  // For std::getenv
#include <cstdio>   // For fprintf

#include "query/expr/expr.hpp"
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
    const int64_t dimension,
    const int64_t start_search_point,
    std::shared_ptr<ANNGraphSegment> ann_index,
    int64_t *offset_table,
    int64_t *neighbor_list,
    std::variant<DenseVectorColumnDataContainer, VariableLenAttrColumnContainer *> vector_column,
    DistFunc fstdistfunc,
    void *dist_func_param,
    int num_threads,
    int64_t L_master,
    int64_t L_local,
    int64_t subsearch_iterations,
    bool prefilter_enabled,
    const std::string& vector_field_name)
    : total_indexed_vector_(ann_index->record_number_),
      dimension_(dimension),
      start_search_point_(start_search_point),
      offset_table_(offset_table),
      neighbor_list_(neighbor_list),
      vector_column_(vector_column),
      vector_field_name_(vector_field_name),
      fstdistfunc_(fstdistfunc),
      dist_func_param_(dist_func_param),
      num_threads_(num_threads),
      L_master_(L_master),
      L_local_(L_local),
      subsearch_iterations_(subsearch_iterations),
      search_result_(L_master),
      distance_(L_master),
      init_ids_(L_master),
      is_visited_(ann_index->record_number_),
      set_L_((num_threads - 1) * L_local + L_master),
      local_queues_sizes_(num_threads, 0),
      local_queues_starts_(num_threads),
      brute_force_search_(ann_index->record_number_ < BruteforceThreshold),
      brute_force_queue_(BruteforceThreshold),
      prefilter_enabled_(prefilter_enabled) {
  ann_index_ = ann_index;
  
  // Initialize memory pool (singleton, only initialized once)
  static bool memory_pool_initialized = false;
  if (!memory_pool_initialized) {
    utils::MemoryPoolConfig config;
    config.initial_size = 128 * 1024 * 1024;  // 128MB
    config.max_size = 1024 * 1024 * 1024;     // 1GB
    config.enable_thread_cache = true;
    config.enable_stats = true;
    utils::MemoryPool::GetInstance().Initialize(config);
    memory_pool_initialized = true;
  }
  
  // Pre-allocate vectors to reduce runtime allocations
  // These are already sized in the initializer list, but ensure capacity
  search_result_.reserve(L_master_);
  distance_.reserve(L_master_);
  init_ids_.reserve(L_master_);
  is_visited_.reserve(ann_index->record_number_);
  set_L_.reserve((num_threads - 1) * L_local + L_master);
  local_queues_sizes_.reserve(num_threads);
  local_queues_starts_.reserve(num_threads);
  if (brute_force_search_) {
    brute_force_queue_.reserve(ann_index->record_number_);
  }
  
  // Log thread configuration for debugging - only in debug builds or when explicitly enabled
#ifdef VECTORDB_DEBUG_BUILD
  static std::atomic<int> executor_count(0);
  int executor_id = executor_count.fetch_add(1, std::memory_order_seq_cst);
  printf("[VecSearchExecutor %d] Created with num_threads=%d (dimension=%ld, indexed_vectors=%ld)\n",
         executor_id, num_threads_, dimension_, total_indexed_vector_);
#else
  // In production, only log if explicitly enabled via environment variable
  static bool log_enabled = std::getenv("VECTORDB_LOG_EXECUTOR") != nullptr;
  if (log_enabled) {
    static std::atomic<int> executor_count(0);
    static std::atomic<int> log_count(0);
    // Sample logging: only log every 100th instance to reduce overhead
    if (log_count.fetch_add(1) % 100 == 0) {
      int executor_id = executor_count.fetch_add(1, std::memory_order_seq_cst);
      fprintf(stderr, "[VecSearchExecutor %d] Created with num_threads=%d (dimension=%ld, indexed_vectors=%ld) [SAMPLED]\n",
              executor_id, num_threads_, dimension_, total_indexed_vector_);
    }
  }
#endif
  
  for (int q_i = 0; q_i < num_threads; ++q_i) {
    local_queues_starts_[q_i] = q_i * L_local;
  }
  if (!brute_force_search_) {
    PrepareInitIds(init_ids_, L_master);
  }

  // CRITICAL FIX: Removed global omp_set_num_threads() call
  // Problem: This setting is global and conflicts with other OpenMP regions
  // Solution: Use num_threads(N) clause in each #pragma omp parallel directive
  // OpenMP will also respect OMP_NUM_THREADS environment variable set in K8s
  // Note: num_threads_ is stored and used in parallel regions via num_threads clause

  // K8s-aware nested parallelism configuration
  // In K8s low-core environments (1-2 cores), nested parallelism adds overhead without benefit
  // Check environment variable first, then auto-detect based on CPU quota
  const char* omp_max_levels_env = std::getenv("OMP_MAX_ACTIVE_LEVELS");
  if (omp_max_levels_env) {
    int max_levels = std::atoi(omp_max_levels_env);
    omp_set_max_active_levels(max_levels);
    fprintf(stderr, "[VecSearchExecutor] Using OMP_MAX_ACTIVE_LEVELS=%d from environment\n", max_levels);
  } else {
    // Auto-detect based on CPU quota (K8s-aware)
    size_t cpu_quota = ConfigLimits::DetectCgroupCpuQuota();
    if (cpu_quota <= 2) {
      // Low core count: disable nested parallelism
      omp_set_max_active_levels(1);
      fprintf(stderr, "[VecSearchExecutor] Disabled nested parallelism (cpu_quota=%zu)\n", cpu_quota);
    } else {
      // Higher core count: allow 2 levels of nesting
      omp_set_max_active_levels(2);
    }
  }
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
  // Add into queue with bounds checking
  size_t elements_to_move = queue_end - insert_loc;
  if (elements_to_move > 0 && insert_loc + 1 + elements_to_move <= queue.size()) {
    utils::SafeMemoryOps::SafeMemmove(
        queue.data() + insert_loc + 1,
        queue.data() + insert_loc,
        elements_to_move,
        queue.size() - insert_loc - 1,
        queue.size() - insert_loc);
  }
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
  size_t elements_to_move = queue_size - insert_index;
  if (elements_to_move > 0 && dest_index + 1 + elements_to_move <= queue.size()) {
    utils::SafeMemoryOps::SafeMemmove(
        queue.data() + dest_index + 1,
        queue.data() + dest_index,
        elements_to_move,
        queue.size() - dest_index - 1,
        queue.size() - dest_index);
  }
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
  size_t elements_to_move = queue_size - insert_index - 1;
  if (elements_to_move > 0 && dest_index + 1 + elements_to_move <= queue.size()) {
    utils::SafeMemoryOps::SafeMemmove(
        queue.data() + dest_index + 1,
        queue.data() + dest_index,
        elements_to_move,
        queue.size() - dest_index - 1,
        queue.size() - dest_index);
  }
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
    const VectorPtr query_data,
    const float &dist_bound,
    // float &dist_thresh,
    std::vector<Candidate> &set_L,
    const int64_t local_queue_start,
    int64_t &local_queue_size,
    const int64_t &local_queue_capacity,
    std::vector<bool> &is_visited,
    uint64_t &local_count_computation) {
  uint64_t tmp_count_computation = 0;

  int64_t nk = local_queue_capacity;

  // Boundary check: ensure cand_id is valid and offset_table_[cand_id + 1] exists
  if (cand_id < 0 || cand_id >= total_indexed_vector_) {
    // Invalid candidate ID, cannot expand
    local_count_computation += tmp_count_computation;
    return nk;
  }

  for (int64_t e_i = offset_table_[cand_id]; e_i < offset_table_[cand_id + 1]; ++e_i) {
    int64_t nb_id = neighbor_list_[e_i];

    // Boundary check: ensure neighbor ID is valid
    if (nb_id < 0 || nb_id >= total_indexed_vector_) {
      continue;
    }

    {  // Sequential edition
      if (is_visited[nb_id]) {
        continue;
      }
      is_visited[nb_id] = true;
    }

    ++tmp_count_computation;
    float dist;
    if (std::holds_alternative<DenseVectorPtr>(vector_column_)) {
      dist = std::get<DenseVecDistFunc<float>>(fstdistfunc_)(
          std::get<DenseVectorPtr>(vector_column_) + dimension_ * nb_id,
          std::get<DenseVectorPtr>(query_data),
          dist_func_param_);
    } else {
      // it holds sparse vector
      auto &vecData = std::get<VariableLenAttrColumnContainer *>(vector_column_)->at(nb_id);
      dist = std::get<SparseVecDistFunc>(fstdistfunc_)(
          *std::get<SparseVectorPtr>(vecData),
          *std::get<SparseVectorPtr>(query_data));
    }

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
    const VectorPtr query_data,
    const int64_t L,
    std::vector<Candidate> &set_L,
    const int64_t set_L_start,
    int64_t &set_L_size,
    const std::vector<int64_t> &init_ids,
    std::vector<bool> &is_visited) {
  // #pragma omp parallel for
  for (int64_t c_i = 0; c_i < L; ++c_i) {
    is_visited[init_ids[c_i]] = true;
  }

  // Get the distances of all candidates, store in the set set_L.
  uint64_t tmp_count_computation = 0;
#pragma omp parallel for num_threads(num_threads_) reduction(+ : tmp_count_computation)
  for (int64_t i = 0; i < L; i++) {
    int64_t v_id = init_ids[i];
    ++tmp_count_computation;

    float dist;
    if (std::holds_alternative<DenseVectorPtr>(vector_column_)) {
      dist = std::get<DenseVecDistFunc<float>>(fstdistfunc_)(
          std::get<DenseVectorPtr>(vector_column_) + dimension_ * v_id,
          std::get<DenseVectorPtr>(query_data),
          dist_func_param_);
    } else {
      // it holds sparse vector
      auto &vecData = std::get<VariableLenAttrColumnContainer *>(vector_column_)->at(v_id);
      dist = std::get<SparseVecDistFunc>(fstdistfunc_)(*std::get<SparseVectorPtr>(vecData), *std::get<SparseVectorPtr>(query_data));
    }
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
  // Boundary check: ensure we have valid data and navigation point
  if (total_indexed_vector_ == 0) {
    // No vectors indexed, cannot prepare init IDs
    return;
  }

  std::vector<bool> is_selected(total_indexed_vector_);
  int64_t init_ids_end = 0;

  // Boundary check: ensure offset_table_[start_search_point_ + 1] exists
  // offset_table_ size is record_number_ + 1, so we need start_search_point_ + 1 < record_number_ + 1
  // which means start_search_point_ < record_number_ (i.e., < total_indexed_vector_)
  if (start_search_point_ < total_indexed_vector_) {
    for (
        int64_t e_i = offset_table_[start_search_point_];
        e_i < offset_table_[start_search_point_ + 1] && init_ids_end < L;
        ++e_i) {
      int64_t v_id = neighbor_list_[e_i];
      // Additional safety check for valid neighbor ID
      if (v_id < 0 || v_id >= total_indexed_vector_) {
        continue;
      }
      if (is_selected[v_id]) {
        continue;
      }
      is_selected[v_id] = true;
      init_ids[init_ids_end++] = v_id;
    }
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
    const VectorPtr query_data,
    const int64_t K,
    const int64_t L,
    std::vector<Candidate> &set_L,
    const std::vector<int64_t> &init_ids,
    std::vector<int64_t> &set_K,
    const int64_t local_queue_capacity,  // Maximum size of local queue
    const std::vector<int64_t> &local_queues_starts,
    std::vector<int64_t> &local_queues_sizes,  // Sizes of local queue
    std::vector<bool> &is_visited,
    const int64_t subsearch_iterations) {
  // CRITICAL FIX: Removed global omp_set_num_threads() call
  // Use num_threads clause in #pragma omp parallel directives instead
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
#pragma omp parallel num_threads(num_threads_) reduction(+ : tmp_count_computation)
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
    is_visited.clear();
    is_visited.resize(total_indexed_vector_);
  }
}

bool VecSearchExecutor::BruteForceSearch(
    const VectorPtr query_data,
    const int64_t start,
    const int64_t end,
    const ConcurrentBitset &deleted,
    vectordb::query::expr::ExprEvaluator &expr_evaluator,
    vectordb::engine::TableSegment *table_segment,
    const int root_node_index) {
  if (brute_force_queue_.size() < end - start) {
    brute_force_queue_.resize(end - start);
  }

  // Determine whether to use indexed data or table segment data
  // When total_indexed_vector_ == 0, the index hasn't been built yet,
  // so we need to use table_segment's vector data instead of vector_column_
  bool use_table_segment_data = (total_indexed_vector_ == 0 && !vector_field_name_.empty());

  // Use thread-local computation to avoid race conditions
  if (std::holds_alternative<DenseVectorPtr>(vector_column_)) {
    DenseVectorPtr data_ptr;

    if (use_table_segment_data) {
      // Use TableSegment's vector data when index not built
      auto field_offset_it = table_segment->field_name_mem_offset_map_.find(vector_field_name_);
      if (field_offset_it == table_segment->field_name_mem_offset_map_.end()) {
        return false;  // Field not found
      }
      size_t field_offset = field_offset_it->second;
      if (field_offset >= table_segment->vector_tables_.size()) {
        return false;  // Invalid field offset
      }
      data_ptr = table_segment->vector_tables_[field_offset]->GetData();
    } else {
      // Use ANNGraphSegment's vector data (indexed data)
      data_ptr = std::get<DenseVectorPtr>(vector_column_);
    }

    #pragma omp parallel for num_threads(num_threads_) schedule(static)
    for (int64_t v_id = start; v_id < end; ++v_id) {
      float dist = std::get<DenseVecDistFunc<float>>(fstdistfunc_)(
          data_ptr + dimension_ * v_id,
          std::get<DenseVectorPtr>(query_data),
          dist_func_param_);
      // Direct array write is safe with static scheduling
      brute_force_queue_[v_id - start] = Candidate(v_id, dist, false);
    }
  } else {
    VariableLenAttrColumnContainer* data_container;

    if (use_table_segment_data) {
      // Use TableSegment's sparse vector data when index not built
      auto field_offset_it = table_segment->field_name_mem_offset_map_.find(vector_field_name_);
      if (field_offset_it == table_segment->field_name_mem_offset_map_.end()) {
        return false;  // Field not found
      }
      size_t field_offset = field_offset_it->second;
      if (field_offset >= table_segment->var_len_attr_table_.size()) {
        return false;  // Invalid field offset
      }
      data_container = &table_segment->var_len_attr_table_[field_offset];
    } else {
      // Use ANNGraphSegment's sparse vector data (indexed data)
      data_container = std::get<VariableLenAttrColumnContainer *>(vector_column_);
    }

    #pragma omp parallel for num_threads(num_threads_) schedule(static)
    for (int64_t v_id = start; v_id < end; ++v_id) {
      auto &vecData = data_container->at(v_id);
      float dist = std::get<SparseVecDistFunc>(fstdistfunc_)(
          *std::get<SparseVectorPtr>(vecData),
          *std::get<SparseVectorPtr>(query_data));
      // Direct array write is safe with static scheduling
      brute_force_queue_[v_id - start] = Candidate(v_id, dist, false);
    }
  }

  // compacting the result with 2 pointers by removing the deleted entries
  // iter: the iterator to loop through the brute force queue
  // num_result: the pointer to the current right-most slot in the new result
  int64_t iter = 0,
          num_result = 0;
  // remove the invalid entries
  for (; iter < end - start; ++iter) {
    if (!deleted.test(iter + start) && expr_evaluator.LogicalEvaluate(root_node_index, iter + start, brute_force_queue_[iter].distance_)) {
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

bool VecSearchExecutor::PreFilterBruteForceSearch(
    const VectorPtr query_data,
    const int64_t start,
    const int64_t end,
    const ConcurrentBitset &deleted,
    vectordb::query::expr::ExprEvaluator &expr_evaluator,
    vectordb::engine::TableSegment *table_segment,
    const int root_node_index) {
  if (brute_force_queue_.size() < end - start) {
    brute_force_queue_.resize(end - start);
  }

  // Determine whether to use indexed data or table segment data
  bool use_table_segment_data = (total_indexed_vector_ == 0 && !vector_field_name_.empty());

  // Use thread-local computation with static scheduling to avoid race conditions
  if (std::holds_alternative<DenseVectorPtr>(vector_column_)) {
    DenseVectorPtr data_ptr;

    if (use_table_segment_data) {
      auto field_offset_it = table_segment->field_name_mem_offset_map_.find(vector_field_name_);
      if (field_offset_it == table_segment->field_name_mem_offset_map_.end()) {
        return false;
      }
      size_t field_offset = field_offset_it->second;
      if (field_offset >= table_segment->vector_tables_.size()) {
        return false;
      }
      data_ptr = table_segment->vector_tables_[field_offset]->GetData();
    } else {
      data_ptr = std::get<DenseVectorPtr>(vector_column_);
    }

    #pragma omp parallel for num_threads(num_threads_) schedule(static)
    for (int64_t v_id = start; v_id < end; ++v_id) {
      // FIXME: pre-filter combined with @distance filter is not supported.
      if (!deleted.test(v_id) && expr_evaluator.LogicalEvaluate(root_node_index, v_id)) {
        float dist = std::get<DenseVecDistFunc<float>>(fstdistfunc_)(
            data_ptr + dimension_ * v_id,
            std::get<DenseVectorPtr>(query_data),
            dist_func_param_);
        // Direct array write is safe with static scheduling
        brute_force_queue_[v_id - start] = Candidate(v_id, dist, false);
      } else {
        brute_force_queue_[v_id - start] = Candidate(v_id, std::numeric_limits<float>::max(), true);
      }
    }
  } else {
    VariableLenAttrColumnContainer* data_container;

    if (use_table_segment_data) {
      auto field_offset_it = table_segment->field_name_mem_offset_map_.find(vector_field_name_);
      if (field_offset_it == table_segment->field_name_mem_offset_map_.end()) {
        return false;
      }
      size_t field_offset = field_offset_it->second;
      if (field_offset >= table_segment->var_len_attr_table_.size()) {
        return false;
      }
      data_container = &table_segment->var_len_attr_table_[field_offset];
    } else {
      data_container = std::get<VariableLenAttrColumnContainer *>(vector_column_);
    }

    #pragma omp parallel for num_threads(num_threads_) schedule(static)
    for (int64_t v_id = start; v_id < end; ++v_id) {
      // FIXME: pre-filter combined with @distance filter is not supported.
      if (!deleted.test(v_id) && expr_evaluator.LogicalEvaluate(root_node_index, v_id)) {
        auto &vecData = data_container->at(v_id);
        float dist = std::get<SparseVecDistFunc>(fstdistfunc_)(
            *std::get<SparseVectorPtr>(vecData),
            *std::get<SparseVectorPtr>(query_data));
        // Direct array write is safe with static scheduling
        brute_force_queue_[v_id - start] = Candidate(v_id, dist, false);
      } else {
        brute_force_queue_[v_id - start] = Candidate(v_id, std::numeric_limits<float>::max(), true);
      }
    }
  }

  // compacting the result with 2 pointers by removing the not passing filter entries
  // iter: the iterator to loop through the brute force queue
  // num_result: the pointer to the current right-most slot in the new result
  int64_t iter = 0,
          num_result = 0;
  // remove the invalid entries
  for (; iter < end - start; ++iter) {
    if (!brute_force_queue_[iter].is_checked_) {
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
    const VectorPtr query_data,
    vectordb::engine::TableSegment *table_segment,
    const size_t limit,
    std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
    int64_t &result_size) {
  int64_t total_vector = table_segment->record_number_;
  ConcurrentBitset &deleted = *(table_segment->deleted_);
  vectordb::query::expr::ExprEvaluator expr_evaluator(
      filter_nodes,
      table_segment->field_name_mem_offset_map_,
      table_segment->primitive_offset_,
      table_segment->var_len_attr_num_,
      table_segment->attribute_table_->GetData(),
      table_segment->var_len_attr_table_);
  int filter_root_index = filter_nodes.size() - 1;
  // currently the max returned result is L_local_
  // TODO: support larger search results
  // std::cout << "search with limit: " << limit << " brute force: " << brute_force_search_
  //           << " indexed_vector: " << total_indexed_vector_ << ", total vector: "
  //           << total_vector << std::endl;

  // CRITICAL FIX: If total_vector > total_indexed_vector_, the index is out of date
  // This happens when data is inserted but rebuild hasn't completed yet
  bool use_brute_force = brute_force_search_ && (total_vector <= total_indexed_vector_);

  // Special case: if no vectors are indexed yet, use brute force search
  // This provides results even when the index hasn't been built yet
  if (total_indexed_vector_ == 0 && total_vector > 0 && !vector_field_name_.empty()) {
    fprintf(stderr, "[VecSearchExecutor] Using brute force fallback: total_indexed=%ld, total_vector=%ld, field=%s\n",
            total_indexed_vector_, total_vector, vector_field_name_.c_str());
    BruteForceSearch(query_data, 0, total_vector, deleted, expr_evaluator, table_segment, filter_root_index);
    result_size = std::min({brute_force_queue_.size(), limit, size_t(L_local_)});
    fprintf(stderr, "[VecSearchExecutor] Brute force returned %ld results\n", result_size);
    for (int64_t k_i = 0; k_i < result_size; ++k_i) {
      search_result_[k_i] = brute_force_queue_[k_i].id_;
      distance_[k_i] = brute_force_queue_[k_i].distance_;
    }
    return Status::OK();
  }

  // If total_indexed_vector_ == 0 but vector_field_name_ is empty, we can't do brute force
  // This shouldn't happen in normal operation, but handle it gracefully
  if (total_indexed_vector_ == 0) {
    result_size = 0;
    return Status::OK();
  }

  if (prefilter_enabled_) {
    PreFilterBruteForceSearch(query_data, 0, std::min(total_vector, total_indexed_vector_), deleted, expr_evaluator, table_segment, filter_root_index);
    result_size = std::min({brute_force_queue_.size(), limit});
    for (int64_t k_i = 0; k_i < result_size; ++k_i) {
      search_result_[k_i] = brute_force_queue_[k_i].id_;
      distance_[k_i] = brute_force_queue_[k_i].distance_;
    }
  } else if (use_brute_force) {
    BruteForceSearch(query_data, 0, total_indexed_vector_, deleted, expr_evaluator, table_segment, filter_root_index);
    result_size = std::min({brute_force_queue_.size(), limit, size_t(L_local_)});
    for (int64_t k_i = 0; k_i < result_size; ++k_i) {
      search_result_[k_i] = brute_force_queue_[k_i].id_;
      distance_[k_i] = brute_force_queue_[k_i].distance_;
    }
  } else {
    // warning, this value cannot exceed LocalQueueSize (we don't check it here because it will
    // create circular dependency)
    auto searchLimit = std::min({size_t(total_indexed_vector_), limit, size_t(L_local_)});
    fprintf(stderr, "[VecSearchExecutor] Using NSG search: total_indexed=%ld, total_vector=%ld, searchLimit=%zu, L_master=%ld, L_local=%ld\n",
            total_indexed_vector_, total_vector, searchLimit, L_master_, L_local_);
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
      fprintf(stderr, "[VecSearchExecutor] Index out of date: adding brute force for vectors [%ld, %ld)\n",
              total_indexed_vector_, total_vector);
      // Need to brute force search the newly added but haven't been indexed vectors.
      BruteForceSearch(query_data, total_indexed_vector_, total_vector, deleted, expr_evaluator, table_segment, filter_root_index);
      // Merge the brute force results into the search result.
      const int64_t master_queue_start = local_queues_starts_[num_threads_ - 1];
      auto bruteForceQueueSize = std::min({brute_force_queue_.size(), limit});
      size_t candidateNum;
      if (bruteForceQueueSize > 0) {
        // brute force happened with non-empty result
        MergeTwoQueuesInto1stQueueSeqFixed(
            set_L_,
            master_queue_start,
            searchLimit,
            brute_force_queue_,
            0,
            bruteForceQueueSize);
        candidateNum = std::min({size_t(L_master_), size_t(total_vector)});
      } else {
        candidateNum = std::min({size_t(L_master_), size_t(total_indexed_vector_)});
      }
      result_size = 0;
      for (int64_t k_i = 0; k_i < candidateNum && result_size < searchLimit; ++k_i) {
        auto id = set_L_[k_i + master_queue_start].id_;
        if (deleted.test(id) || !expr_evaluator.LogicalEvaluate(filter_root_index, id, set_L_[k_i + master_queue_start].distance_)) {
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
        if (deleted.test(id) || !expr_evaluator.LogicalEvaluate(filter_root_index, id, set_L_[k_i + master_queue_start].distance_)) {
          continue;
        }
        search_result_[result_size] = set_L_[k_i + master_queue_start].id_;
        distance_[result_size] = set_L_[k_i + master_queue_start].distance_;
        result_size++;
      }
    }
    if (result_size > L_master_) {
      search_result_.resize(result_size);
      distance_.resize(result_size);
    }
  }
  return Status::OK();
}

Status VecSearchExecutor::SearchByAttribute(
    meta::TableSchema &table_schema,
    vectordb::engine::TableSegment *table_segment,
    const size_t skip,
    const size_t raw_limit,
    vectordb::Json &primary_keys,
    std::vector<vectordb::query::expr::ExprNodePtr> &filter_nodes,
    int64_t &result_size) {
  // TODO: leverage multithread to accelerate
  int64_t counter = -1;
  int64_t total_vector = table_segment->record_number_;
  ConcurrentBitset &deleted = *(table_segment->deleted_);
  vectordb::query::expr::ExprEvaluator expr_evaluator(
      filter_nodes,
      table_segment->field_name_mem_offset_map_,
      table_segment->primitive_offset_,
      table_segment->var_len_attr_num_,
      table_segment->attribute_table_->GetData(),
      table_segment->var_len_attr_table_);
  int filter_root_index = filter_nodes.size() - 1;
  result_size = 0;
  int64_t limit = raw_limit;
  if (limit > total_vector) {
    limit = total_vector;
  }
  if (limit > L_master_) {
    search_result_.resize(limit);
  }

  int64_t record_size = primary_keys.GetSize();
  if (record_size > 0) {
    // Has id list.
    size_t id = 0;
    for (int64_t i = 0; i < record_size; ++i) {
      auto pkField = primary_keys.GetArrayElement(i);
      if (table_segment->PK2ID(pkField, id)) {
        if (deleted.test(id) || !expr_evaluator.LogicalEvaluate(filter_root_index, id)) {
          continue;
        }
        counter++;
        if (counter >= skip && counter < skip + limit) {
          search_result_[result_size++] = id;
        }
        if (counter >= skip + limit) {
          break;
        }
      }
    }
  } else {
    // Loop through geo-indices to filter out records.
    bool use_geo_index = false;
    for (auto field: table_schema.fields_) {
      if (field.field_type_ == meta::FieldType::GEO_POINT) {
        int64_t node_idx = expr_evaluator.UpliftingGeoIndex(field.name_, filter_root_index);
        if (node_idx != -1) {
          use_geo_index = true;
          auto index = table_segment->geospatial_indices_[field.name_];
          std::vector<vectordb::engine::index::GeospatialIndex::value_t> ids;
          auto lat = expr_evaluator.NumEvaluate(filter_nodes[node_idx]->arguments[1], -1, 0);
          auto lon = expr_evaluator.NumEvaluate(filter_nodes[node_idx]->arguments[2], -1, 0);
          auto dist = expr_evaluator.NumEvaluate(filter_nodes[node_idx]->arguments[3], -1, 0);
          index->searchWithinRadius(lat, lon, dist, ids);
          for (const auto& result : ids) {
            int64_t id = result.second;
            if (deleted.test(id) || !expr_evaluator.LogicalEvaluate(filter_root_index, id)) {
              continue;
            }
            counter++;
            if (counter >= skip && counter < skip + limit) {
              search_result_[result_size++] = id;
            }
            if (counter >= skip + limit) {
              break;
            }
          }
        }
      }
    }

    if (!use_geo_index) {
      for (int64_t id = 0; id < total_vector; ++id) {
        if (deleted.test(id) || !expr_evaluator.LogicalEvaluate(filter_root_index, id)) {
          continue;
        }
        counter++;
        if (counter >= skip && counter < skip + limit) {
          search_result_[result_size++] = id;
        }
        if (counter >= skip + limit) {
          break;
        }
      }
    }
  }
  return Status::OK();
}

}  // namespace execution
}  // namespace engine
}  // namespace vectordb
