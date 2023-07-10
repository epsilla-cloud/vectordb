#include "utils/status.hpp"

#include <cstring>

namespace vectordb {

constexpr int CODE_WIDTH = sizeof(StatusCode);

Status::Status(StatusCode code, const std::string& msg) {
  // 4 bytes store code
  // 4 bytes store message length
  // the left bytes store message string
  const uint32_t length = (uint32_t)msg.size();
  auto result = new char[length + sizeof(length) + CODE_WIDTH];
  std::memcpy(result, &code, CODE_WIDTH);
  std::memcpy(result + CODE_WIDTH, &length, sizeof(length));
  memcpy(result + sizeof(length) + CODE_WIDTH, msg.data(), length);

  state_ = result;
}

Status::Status() : state_(nullptr) {
}

Status::~Status() {
  if (state_ != nullptr) {
    delete[] state_;
  }
}

Status::Status(const Status& s) : state_(nullptr) {
  CopyFrom(s);
}

Status&
Status::operator=(const Status& s) {
  CopyFrom(s);
  return *this;
}

Status::Status(Status&& s) : state_(nullptr) {
  MoveFrom(s);
}

Status&
Status::operator=(Status&& s) {
  MoveFrom(s);
  return *this;
}

void Status::CopyFrom(const Status& s) {
  delete[] state_;
  state_ = nullptr;
  if (s.state_ == nullptr) {
    return;
  }

  uint32_t length = 0;
  memcpy(&length, s.state_ + CODE_WIDTH, sizeof(length));
  int buff_len = length + sizeof(length) + CODE_WIDTH;
  state_ = new char[buff_len];
  memcpy(state_, s.state_, buff_len);
}

void Status::MoveFrom(Status& s) {
  delete[] state_;
  state_ = s.state_;
  s.state_ = nullptr;
}

std::string
Status::message() const {
  if (state_ == nullptr) {
    return "OK";
  }

  std::string msg;
  uint32_t length = 0;
  memcpy(&length, state_ + CODE_WIDTH, sizeof(length));
  if (length > 0) {
    msg.append(state_ + sizeof(length) + CODE_WIDTH, length);
  }

  return msg;
}

std::string
Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  }

  std::string result;
  switch (code()) {
    case DB_SUCCESS:
      result = "OK ";
      break;
    case DB_UNEXPECTED_ERROR:
      result = "Unexpected Error: ";
      break;
    case DB_UNSUPPORTED_ERROR:
      result = "Unsupported error: ";
      break;
    default:
      result = "Error code(" + std::to_string(code()) + "): ";
      break;
  }

  result += message();
  return result;
}

}  // namespace vectordb