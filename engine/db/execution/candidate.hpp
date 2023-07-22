#pragma once
namespace vectordb {
namespace engine {
namespace execution {

// A search expansion/result candidate.
struct Candidate {
  int64_t id_;       // internal id
  float distance_;   // distance to search target vector
  bool is_checked_;  // whether this candidate is checked in the queue

  Candidate() = default;
  Candidate(int64_t id, float dist, bool is_checked) : id_(id), distance_(dist), is_checked_(is_checked) {}

  // Comparison operator
  bool operator<(const Candidate& other) const noexcept {
    if (this->distance_ != other.distance_) {
      return this->distance_ < other.distance_;
    } else {
      return this->id_ < other.id_;
    }
  }
};

}  // namespace execution
}  // namespace engine
}  // namespace vectordb
