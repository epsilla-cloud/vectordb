#include "json.hpp"

#include "thirdparty/rapidjson/stringbuffer.h"
#include "thirdparty/rapidjson/writer.h"

namespace vectordb {

Json::Json() : doc_(new rapidjson::Document()), val_(doc_.get()) {}  // val initially points to the document
Json::Json(rapidjson::Value* value) : val_(value) {}                 // For nested objects, val points to a value within the document

bool Json::LoadFromString(const std::string& json_string) {
  doc_->Parse(json_string.c_str());
  val_ = doc_.get();  // After parsing, val points to the document
  if (doc_->HasParseError()) {
    return false;
  }
  return true;
}

std::string Json::DumpToString() {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  val_->Accept(writer);  // Write the value pointed to by val

  return buffer.GetString();
}

std::string Json::GetString(const std::string& key) const {
  if ((*val_)[key.c_str()].IsString()) {
    return (*val_)[key.c_str()].GetString();
  }
  return "";
}

int64_t Json::GetInt(const std::string& key) const {
  if ((*val_)[key.c_str()].IsInt64()) {
    return (*val_)[key.c_str()].GetInt64();
  }
  return 0;
}

double Json::GetDouble(const std::string& key) const {
  if ((*val_)[key.c_str()].IsDouble()) {
    return (*val_)[key.c_str()].GetDouble();
  }
  return 0.0;
}

bool Json::GetBool(const std::string& key) const {
  if ((*val_)[key.c_str()].IsBool()) {
    return (*val_)[key.c_str()].GetBool();
  }
  return false;  // Default value if key not found or not a boolean
}

std::string Json::GetString() const {
  if (val_->IsString()) {
    return val_->GetString();
  }
  return "";
}

int64_t Json::GetInt() const {
  if (val_->IsInt64()) {
    return val_->GetInt64();
  }
  return 0;
}

double Json::GetDouble() const {
  if (val_->IsDouble()) {
    return val_->GetDouble();
  }
  return 0.0;
}

bool Json::GetBool() const {
  if (val_->IsBool()) {
    return val_->GetBool();
  }
  return false;  // Default value if key not found or not a boolean
}

size_t Json::GetSize() const {
  if (!val_->IsArray()) {
    // Handle error
    return 0;
  }
  return val_->Size();
}

Json Json::GetObject(const std::string& key) const {
  if ((*val_)[key.c_str()].IsObject()) {
    return Json(&(*val_)[key.c_str()]);
  }
  return Json(nullptr);  // Return an invalid Json object if key not found or not an object
}

size_t Json::GetArraySize(const std::string& key) const {
  rapidjson::Value& arr = (*val_)[key.c_str()];
  if (!arr.IsArray()) {
    // Handle error
    return 0;
  }
  return arr.Size();
}

Json Json::GetArrayElement(const std::string& key, size_t index) const {
  if ((*val_)[key.c_str()].IsArray()) {
    auto& array = (*val_)[key.c_str()];
    if (index < array.Size()) {
      return Json(&array[(rapidjson::SizeType)index]);
    }
  }
  return Json(nullptr);  // Return an invalid Json object if key not found, not an array, or index out of range
}

bool Json::HasMember(const std::string& key) const {
  if (!val_->IsObject()) {
    return false;
  }
  return val_->HasMember(key.c_str());
}

void Json::SetString(const std::string& key, const std::string& value) {
  rapidjson::Value key_value(rapidjson::StringRef(key.c_str()), doc_->GetAllocator());
  rapidjson::Value str_value;
  str_value.SetString(value.c_str(), doc_->GetAllocator());
  val_->AddMember(key_value, str_value, doc_->GetAllocator());
}

void Json::SetInt(const std::string& key, int64_t value) {
  rapidjson::Value key_value(rapidjson::StringRef(key.c_str()), doc_->GetAllocator());
  rapidjson::Value int_value;
  int_value.SetInt64(value);
  val_->AddMember(key_value, int_value, doc_->GetAllocator());
}

void Json::SetDouble(const std::string& key, double value) {
  rapidjson::Value key_value(rapidjson::StringRef(key.c_str()), doc_->GetAllocator());
  rapidjson::Value double_value;
  double_value.SetDouble(value);
  val_->AddMember(key_value, double_value, doc_->GetAllocator());
}

void Json::SetBool(const std::string& key, bool value) {
  rapidjson::Value key_value(rapidjson::StringRef(key.c_str()), doc_->GetAllocator());
  rapidjson::Value bool_value;
  bool_value.SetBool(value);
  val_->AddMember(key_value, bool_value, doc_->GetAllocator());
}

void Json::SetObject(const std::string& key, const Json& object) {
  rapidjson::Value key_value(rapidjson::StringRef(key.c_str()), doc_->GetAllocator());
  rapidjson::Value object_value;
  object_value.CopyFrom(*object.val_, doc_->GetAllocator());
  val_->AddMember(key_value, object_value, doc_->GetAllocator());
}

void Json::SetArray(const std::string& key, const std::vector<Json>& array) {
  rapidjson::Value key_value(rapidjson::StringRef(key.c_str()), doc_->GetAllocator());
  rapidjson::Value arr(rapidjson::kArrayType);
  for (const auto& item : array) {
    rapidjson::Value item_val;
    item_val.CopyFrom(*item.val_, doc_->GetAllocator());
    arr.PushBack(item_val, doc_->GetAllocator());
  }
  val_->AddMember(key_value, arr, doc_->GetAllocator());
}

void Json::AddStringToArray(const std::string& key, const std::string& value) {
  rapidjson::Value str_value;
  str_value.SetString(value.c_str(), doc_->GetAllocator());
  (*val_)[key.c_str()].PushBack(str_value, doc_->GetAllocator());
}

void Json::AddIntToArray(const std::string& key, int64_t value) {
  (*val_)[key.c_str()].PushBack(value, doc_->GetAllocator());
}

void Json::AddDoubleToArray(const std::string& key, double value) {
  (*val_)[key.c_str()].PushBack(value, doc_->GetAllocator());
}

void Json::AddBoolToArray(const std::string& key, bool value) {
  (*val_)[key.c_str()].PushBack(value, doc_->GetAllocator());
}

void Json::AddObjectToArray(const std::string& key, const Json& object) {
  rapidjson::Value objectVal;
  objectVal.CopyFrom(*object.val_, doc_->GetAllocator());
  (*val_)[key.c_str()].PushBack(objectVal, doc_->GetAllocator());
}

}  // namespace vectordb
