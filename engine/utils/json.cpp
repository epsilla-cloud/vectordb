#include "json.hpp"

#include <iostream>

#include "thirdparty/rapidjson/stringbuffer.h"
#include "thirdparty/rapidjson/writer.h"

namespace vectordb {

Json::Json() : doc_(nlohmann::json()) {}

bool Json::LoadFromString(const std::string& json_string) {
  try {
    doc_ = nlohmann::json::parse(json_string);
  } catch (nlohmann::json::parse_error& e) {
    // Optionally: Log or print e.what() to get a detailed error message
    return false;
  }
  return true;
}

std::string Json::DumpToString() {
  return doc_.dump();
}

std::string Json::GetString(const std::string& key) const {
  if (doc_.contains(key) && doc_[key].is_string()) {
    return doc_[key].get<std::string>();
  }
  return "";
}

int64_t Json::GetInt(const std::string& key) const {
  if (doc_.contains(key) && doc_[key].is_number_integer()) {
    return doc_[key].get<int64_t>();
  }
  return 0;
}

double Json::GetDouble(const std::string& key) const {
  if (doc_.contains(key) && (doc_[key].is_number_float() || doc_[key].is_number_integer())) {
    return doc_[key].get<double>();
  }
  return 0.0;
}

bool Json::GetBool(const std::string& key) const {
  if (doc_.contains(key) && doc_[key].is_boolean()) {
    return doc_[key].get<bool>();
  }
  return false;
}

std::string Json::GetString() const {
  return doc_.get<std::string>();
}

int64_t Json::GetInt() const {
  return doc_.get<int64_t>();
}

bool Json::IsNumber() const {
  return doc_.is_number();
}

bool Json::IsString() const {
  return doc_.is_string();
}

double Json::GetDouble() const {
  return doc_.get<double>();
}

bool Json::GetBool() const {
  return doc_.get<bool>();
}

size_t Json::GetSize() const {
  if (doc_.is_array()) {
    return doc_.size();
  } else {
    // Handle error: the JSON value is not an array
    return 0;
  }
}

Json Json::GetObject(const std::string& key) const {
  if (doc_.contains(key) && doc_[key].is_object()) {
    Json json;
    json.doc_ = doc_[key];
    return json;
  }
  return Json();
}

Json Json::Get(const std::string& key) const {
  if (doc_.contains(key)) {
    Json json;
    json.doc_ = doc_[key];
    return json;
  }
  return Json();
}

size_t Json::GetArraySize(const std::string& key) const {
  if (doc_[key].is_array()) {
    return doc_[key].size();
  } else {
    // Handle error: the value associated with key is not an array
    return 0;
  }
}

Json Json::GetArray(const std::string& key) const {
  Json json;
  if (doc_[key].is_array()) {
    json.doc_ = doc_[key];
  }
  return json;
}

Json Json::GetArrayElement(const std::string& key, size_t index) const {
  Json json;
  if (doc_[key].is_array() && index < doc_[key].size()) {
    json.doc_ = doc_[key][index];
  }
  return json;
}

Json Json::GetArrayElement(size_t index) const {
  Json json;
  if (doc_.is_array() && index < doc_.size()) {
    json.doc_ = doc_[index];
  }
  return json;
}

bool Json::HasMember(const std::string& key) const {
  return doc_.contains(key);
}

void Json::SetString(const std::string& key, const std::string& value) {
  doc_[key] = value;
}

void Json::SetInt(const std::string& key, int64_t value) {
  doc_[key] = value;
}

void Json::SetDouble(const std::string& key, double value) {
  doc_[key] = value;
}

void Json::SetBool(const std::string& key, bool value) {
  doc_[key] = value;
}

void Json::SetObject(const std::string& key, const Json& object) {
  doc_[key] = object.doc_;
}

void Json::SetArray(const std::string& key, const std::vector<Json>& array) {
  doc_[key] = nlohmann::json::array();
  for (const auto& item : array) {
    doc_[key].push_back(item.doc_);
  }
}

void Json::AddStringToArray(const std::string& key, const std::string& value) {
  doc_[key].push_back(value);
}

void Json::AddIntToArray(const std::string& key, int64_t value) {
  doc_[key].push_back(value);
}

void Json::AddDoubleToArray(const std::string& key, double value) {
  doc_[key].push_back(value);
}

void Json::AddBoolToArray(const std::string& key, bool value) {
  doc_[key].push_back(value);
}

void Json::AddObjectToArray(const std::string& key, const Json& object) {
  doc_[key].push_back(object.doc_);
}

void Json::AddStringToArray(const std::string& value) {
  doc_.push_back(value);
}

void Json::AddIntToArray(int64_t value) {
  doc_.push_back(value);
}

void Json::AddDoubleToArray(double value) {
  doc_.push_back(value);
}

void Json::AddBoolToArray(bool value) {
  doc_.push_back(value);
}

void Json::AddObjectToArray(const Json& object) {
  doc_.push_back(object.doc_);
}

}  // namespace vectordb
