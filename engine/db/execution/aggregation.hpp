#pragma once
#include <iostream>
#include <unordered_map>
#include <variant>
#include <string>
#include <limits>
#include <type_traits>
#include <optional>

namespace vectordb {
namespace engine {
namespace execution {

template<typename KeyType>
class BaseAggregator {
protected:
  std::unordered_map<KeyType, double> aggregatedValues;

public:
  using Iterator = typename std::unordered_map<KeyType, double>::const_iterator;

  virtual void addValue(const KeyType& key, const std::variant<int64_t, double>& value) = 0;
  virtual void printResult() const = 0;
  virtual ~BaseAggregator() {}

  Iterator begin() const { return aggregatedValues.begin(); }
  Iterator end() const { return aggregatedValues.end(); }

  double getValue(const KeyType& key) const {
    auto it = aggregatedValues.find(key);
    if (it != aggregatedValues.end()) {
      return it->second;
    } else {
      throw "System error: accessing non-existing key in aggregator.";
    }
  }
};

template<typename KeyType>
class MinAggregator : public BaseAggregator<KeyType> {
public:
  void addValue(const KeyType& key, const std::variant<int64_t, double>& value) override {
    double numericValue = std::visit([](auto&& arg) -> double {
      return static_cast<double>(arg);
    }, value);

    if (this->aggregatedValues.find(key) == this->aggregatedValues.end() || numericValue < this->aggregatedValues[key]) {
      this->aggregatedValues[key] = numericValue;
    }
  }

  void printResult() const override {
    for (const auto& [key, val] : this->aggregatedValues) {
      std::cout << "Key: " << key << ", MIN: " << val << std::endl;
    }
  }
};

template<typename KeyType>
class MaxAggregator : public BaseAggregator<KeyType> {
public:
  void addValue(const KeyType& key, const std::variant<int64_t, double>& value) override {
    double numericValue = std::visit([](auto&& arg) -> double {
      return static_cast<double>(arg);
    }, value);

    if (this->aggregatedValues.find(key) == this->aggregatedValues.end() || numericValue > this->aggregatedValues[key]) {
      this->aggregatedValues[key] = numericValue;
    }
  }

  void printResult() const override {
    for (const auto& [key, val] : this->aggregatedValues) {
      std::cout << "Key: " << key << ", MAX: " << val << std::endl;
    }
  }
};

template<typename KeyType>
class SumAggregator : public BaseAggregator<KeyType> {
public:
  void addValue(const KeyType& key, const std::variant<int64_t, double>& value) override {
    double numericValue = std::visit([](auto&& arg) -> double {
      return static_cast<double>(arg);
    }, value);

    // Since we are doing summation, we can simply add the numericValue
    // to the current sum for the given key. If the key doesn't exist yet,
    // this will initialize the sum to numericValue.
    this->aggregatedValues[key] += numericValue;
  }

  void printResult() const override {
    for (const auto& [key, val] : this->aggregatedValues) {
      std::cout << "Key: " << key << ", SUM: " << val << std::endl;
    }
  }
};

template<typename KeyType>
class CountAggregator : public BaseAggregator<KeyType> {
public:
  void addValue(const KeyType& key, const std::variant<int64_t, double>& value) override {
    // For counting, we're not interested in the value itself.
    // Increment the count for the key. If the key doesn't exist yet,
    // this will initialize the count to 1.
    this->aggregatedValues[key] += 1;
  }

  void printResult() const override {
    for (const auto& [key, val] : this->aggregatedValues) {
      std::cout << "Key: " << key << ", COUNT: " << val << std::endl;
    }
  }
};

}
}
}
