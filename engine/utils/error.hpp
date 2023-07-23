#pragma once

#include <cstdint>
#include <exception>
#include <string>

namespace vectordb {

using ErrorCode = int32_t;

constexpr ErrorCode INFRA_SUCCESS = 0;
constexpr ErrorCode INFRA_ERROR_CODE_BASE = 40000;
constexpr ErrorCode DB_SUCCESS = 0;
constexpr ErrorCode DB_ERROR_CODE_BASE = 50000;

constexpr ErrorCode ToInfraErrorCode(const int32_t error_code) {
  return INFRA_ERROR_CODE_BASE + error_code;
}

constexpr ErrorCode ToDbErrorCode(const int32_t error_code) {
  return DB_ERROR_CODE_BASE + error_code;
}

// infra error code
constexpr ErrorCode INFRA_UNEXPECTED_ERROR = ToInfraErrorCode(1);
constexpr ErrorCode INFRA_UNSUPPORTED_ERROR = ToInfraErrorCode(2);

// db error code
constexpr ErrorCode DB_UNEXPECTED_ERROR = ToDbErrorCode(1);
constexpr ErrorCode DB_UNSUPPORTED_ERROR = ToDbErrorCode(2);
constexpr ErrorCode DB_NOT_FOUND = ToDbErrorCode(3);
constexpr ErrorCode TABLE_ALREADY_EXISTS = ToDbErrorCode(4);
constexpr ErrorCode TABLE_NOT_FOUND = ToDbErrorCode(5);
constexpr ErrorCode INVALID_RECORD = ToDbErrorCode(6);

namespace server {

class ServerException : public std::exception {
 public:
  explicit ServerException(ErrorCode error_code, const std::string& message = std::string())
      : error_code_(error_code), message_(message) {
  }

  ErrorCode error_code() const {
    return error_code_;
  }

  virtual const char* what() const noexcept {
    return message_.c_str();
  }

 private:
  ErrorCode error_code_;
  std::string message_;
};

}  // namespace server

}  // namespace vectordb