#pragma once

#include <memory>
#include <string>

#include "thirdparty/rapidjson/document.h"

namespace vectordb {

class Json {
 public:
  Json();
  Json(rapidjson::Value* value);  // Constructor for nested objects

  bool loadFromString(const std::string& jsonString);
  std::string dumpToString();
  std::string getString(const std::string& key);
  int64_t getInt(const std::string& key);
  double getDouble(const std::string& key);
  bool getBool(const std::string& key);
  std::string getString();
  int64_t getInt();
  double getDouble();
  bool getBool();
  Json getObject(const std::string& key);                      // Get nested object
  Json getArrayElement(const std::string& key, size_t index);  // Get specific element from array

 private:
  std::unique_ptr<rapidjson::Document> doc;
  rapidjson::Value* val;  // Pointer to a value within the document (for nested objects)
};

}  // namespace vectordb

/**
 * Usage example:
 * 
  std::string jsonString = R"({
        "name": "John Doe",
        "age": 30,
        "isMarried": true,
        "spouse": {
            "name": "Jane Doe",
            "age": 28
        },
        "childrenNames": ["Alice", "Bob"]
    })";

    vectordb::Json json;
    bool loadSuccess = json.loadFromString(jsonString);
    if (!loadSuccess) {
        std::cout << "Failed to load JSON\n";
        return 1;
    }

    std::string name = json.getString("name");
    int64_t age = json.getInt("age");
    bool isMarried = json.getBool("isMarried");
    std::string spouseName = json.getObject("spouse").getString("name");
    int spouseAge = json.getObject("spouse").getInt("age");
    std::string firstChildName = json.getArrayElement("childrenNames", 0).getString();

    std::cout << "Name: " << name << "\n";
    std::cout << "Age: " << age << "\n";
    std::cout << "Is Married: " << (isMarried ? "Yes" : "No") << "\n";
    std::cout << "Spouse's Name: " << spouseName << "\n";
    std::cout << "Spouse's Age: " << spouseAge << "\n";
    std::cout << "First Child's Name: " << firstChildName << "\n";
*/
