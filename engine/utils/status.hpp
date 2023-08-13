#pragma once

#include <string>

#include "utils/error.hpp"

namespace vectordb {

class Status;

using StatusCode = ErrorCode;

class Status {
 public:
  Status(StatusCode code, const std::string& msg);
  Status();
  ~Status();

  Status(const Status& s);

  Status& operator=(const Status& s);

  Status(Status&& s);

  Status& operator=(Status&& s);

  static Status OK() { return Status(); }

  bool ok() const { return code() == 0; }

  StatusCode code() const {
    return (state_ == nullptr) ? 0 : *(StatusCode*)(state_);
  }

  std::string message() const;

  std::string ToString() const;

 private:
  inline void CopyFrom(const Status& s);

  inline void MoveFrom(Status& s);

 private:
  char* state_ = nullptr;
};

inline Status StatusCheck(const Status& status) {
  if (!status.ok()) {
    return status;
  }
  return Status::OK();
}

}  // namespace vectordb
