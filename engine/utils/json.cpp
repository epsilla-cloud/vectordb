#include "json.hpp"

#include "thirdparty/rapidjson/stringbuffer.h"
#include "thirdparty/rapidjson/writer.h"

namespace vectordb {

Json::Json() : doc(new rapidjson::Document()), val(doc.get()) {}  // val initially points to the document
Json::Json(rapidjson::Value* value) : val(value) {}               // For nested objects, val points to a value within the document

bool Json::loadFromString(const std::string& jsonString) {
  doc->Parse(jsonString.c_str());
  val = doc.get();  // After parsing, val points to the document
  if (doc->HasParseError()) {
    return false;
  }
  return true;
}

std::string Json::dumpToString() {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  val->Accept(writer);  // Write the value pointed to by val

  return buffer.GetString();
}

std::string Json::getString(const std::string& key) {
  if ((*val)[key.c_str()].IsString()) {
    return (*val)[key.c_str()].GetString();
  }
  return "";
}

int64_t Json::getInt(const std::string& key) {
  if ((*val)[key.c_str()].IsInt64()) {
    return (*val)[key.c_str()].GetInt64();
  }
  return 0;
}

double Json::getDouble(const std::string& key) {
  if ((*val)[key.c_str()].IsDouble()) {
    return (*val)[key.c_str()].GetDouble();
  }
  return 0.0;
}

bool Json::getBool(const std::string& key) {
  if ((*val)[key.c_str()].IsBool()) {
    return (*val)[key.c_str()].GetBool();
  }
  return false;  // Default value if key not found or not a boolean
}

std::string Json::getString() {
  if (val->IsString()) {
    return val->GetString();
  }
  return "";
}

int64_t Json::getInt() {
  if (val->IsInt64()) {
    return val->GetInt64();
  }
  return 0;
}

double Json::getDouble() {
  if (val->IsDouble()) {
    return val->GetDouble();
  }
  return 0.0;
}

bool Json::getBool() {
  if (val->IsBool()) {
    return val->GetBool();
  }
  return false;  // Default value if key not found or not a boolean
}

Json Json::getObject(const std::string& key) {
  if ((*val)[key.c_str()].IsObject()) {
    return Json(&(*val)[key.c_str()]);
  }
  return Json(nullptr);  // Return an invalid Json object if key not found or not an object
}

Json Json::getArrayElement(const std::string& key, size_t index) {
  if ((*val)[key.c_str()].IsArray()) {
    auto& array = (*val)[key.c_str()];
    if (index < array.Size()) {
      return Json(&array[(rapidjson::SizeType)index]);
    }
  }
  return Json(nullptr);  // Return an invalid Json object if key not found, not an array, or index out of range
}

}  // namespace vectordb
