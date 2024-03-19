#pragma once
#include <iostream>
#include <unordered_map>
#include <variant>
#include <string>
#include <limits>
#include <type_traits>
#include <optional>
#include <vector>
#include <memory>
#include "query/expr/expr_evaluator.hpp"
#include "query/expr/expr_types.hpp"
#include "utils/status.hpp"
#include "utils/json.hpp"
#include "db/table_segment_mvp.hpp"

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

class FacetExecutor {
protected:
  bool global_group_by;
  std::vector<std::unique_ptr<BaseAggregator<int64_t>>> int_aggregators_;
  std::vector<std::unique_ptr<BaseAggregator<double>>> double_aggregators_;
  std::vector<std::unique_ptr<BaseAggregator<bool>>> bool_aggregators_;
  std::vector<std::unique_ptr<BaseAggregator<std::string>>> string_aggregators_;
  std::vector<std::string> group_by_exprs;
  std::vector<std::vector<query::expr::ExprNodePtr>> group_by_evals;
  std::vector<std::string> aggregation_exprs;
  std::vector<std::vector<query::expr::ExprNodePtr>> aggregation_evals;

public:
  // Facet JSON example:
  // {
  //   "group": [
  //     "age",
  //     "hobby"
  //   ],
  //   "aggregate": [
  //     "SUM(age)",
  //     "COUNT(*)",
  //   ]
  // }
  // Output will be something like:
  // [
  //   {
  //     "age": 20,
  //     "hobby": "football",
  //     "SUM(age)": 100,
  //     "COUNT(*)": 5
  //   },
  //   {
  //     "age": 20,
  //     "hobby": "basketball",
  //     "SUM(age)": 200,
  //     "COUNT(*)": 10
  //   }
  // ]
  FacetExecutor(
    bool global_group_by,
    std::vector<std::string>& group_by_exprs,
    std::vector<std::vector<query::expr::ExprNodePtr>>& group_by_evals,
    std::vector<query::expr::NodeType>& aggregation_types,
    std::vector<std::string>& aggregation_exprs,
    std::vector<std::vector<query::expr::ExprNodePtr>>& aggregation_evals
  ) {
    this->global_group_by = global_group_by;
    this->group_by_exprs = group_by_exprs;
    this->group_by_evals = group_by_evals;
    this->aggregation_exprs = aggregation_exprs;
    this->aggregation_evals = aggregation_evals;
    // Single group by, directly use the first group by evals to determine the type
    if (group_by_evals.size() == 1) {
      auto& group = group_by_evals[0][group_by_evals[0].size() - 1];
      if (group->value_type == query::expr::ValueType::INT) {
        for (size_t i = 0; i < aggregation_evals.size(); ++i) {
          auto& agg = aggregation_evals[i][aggregation_evals[i].size() - 1];
          if (aggregation_types[i] == query::expr::NodeType::SumAggregation) {
            int_aggregators_.emplace_back(std::make_unique<SumAggregator<int64_t>>());
          } else if (aggregation_types[i] == query::expr::NodeType::CountAggregation) {
            int_aggregators_.emplace_back(std::make_unique<CountAggregator<int64_t>>());
          } else if (aggregation_types[i] == query::expr::NodeType::MinAggregation) {
            int_aggregators_.emplace_back(std::make_unique<MinAggregator<int64_t>>());
          } else if (aggregation_types[i] == query::expr::NodeType::MaxAggregation) {
            int_aggregators_.emplace_back(std::make_unique<MaxAggregator<int64_t>>());
          }
        }
      } else if (group->value_type == query::expr::ValueType::DOUBLE) {
        for (size_t i = 0; i < aggregation_evals.size(); ++i) {
          auto& agg = aggregation_evals[i][aggregation_evals[i].size() - 1];
          if (aggregation_types[i] == query::expr::NodeType::SumAggregation) {
            double_aggregators_.emplace_back(std::make_unique<SumAggregator<double>>());
          } else if (aggregation_types[i] == query::expr::NodeType::CountAggregation) {
            double_aggregators_.emplace_back(std::make_unique<CountAggregator<double>>());
          } else if (aggregation_types[i] == query::expr::NodeType::MinAggregation) {
            double_aggregators_.emplace_back(std::make_unique<MinAggregator<double>>());
          } else if (aggregation_types[i] == query::expr::NodeType::MaxAggregation) {
            double_aggregators_.emplace_back(std::make_unique<MaxAggregator<double>>());
          }
        }
      } else if (group->value_type == query::expr::ValueType::BOOL) {
        for (size_t i = 0; i < aggregation_evals.size(); ++i) {
          auto& agg = aggregation_evals[i][aggregation_evals[i].size() - 1];
          if (aggregation_types[i] == query::expr::NodeType::SumAggregation) {
            bool_aggregators_.emplace_back(std::make_unique<SumAggregator<bool>>());
          } else if (aggregation_types[i] == query::expr::NodeType::CountAggregation) {
            bool_aggregators_.emplace_back(std::make_unique<CountAggregator<bool>>());
          } else if (aggregation_types[i] == query::expr::NodeType::MinAggregation) {
            bool_aggregators_.emplace_back(std::make_unique<MinAggregator<bool>>());
          } else if (aggregation_types[i] == query::expr::NodeType::MaxAggregation) {
            bool_aggregators_.emplace_back(std::make_unique<MaxAggregator<bool>>());
          }
        }
      } else if (group->value_type == query::expr::ValueType::STRING) {
        for (size_t i = 0; i < aggregation_evals.size(); ++i) {
          auto& agg = aggregation_evals[i][aggregation_evals[i].size() - 1];
          if (aggregation_types[i] == query::expr::NodeType::SumAggregation) {
            string_aggregators_.emplace_back(std::make_unique<SumAggregator<std::string>>());
          } else if (aggregation_types[i] == query::expr::NodeType::CountAggregation) {
            string_aggregators_.emplace_back(std::make_unique<CountAggregator<std::string>>());
          } else if (aggregation_types[i] == query::expr::NodeType::MinAggregation) {
            string_aggregators_.emplace_back(std::make_unique<MinAggregator<std::string>>());
          } else if (aggregation_types[i] == query::expr::NodeType::MaxAggregation) {
            string_aggregators_.emplace_back(std::make_unique<MaxAggregator<std::string>>());
          }
        }
      }
    } else {
      throw "System error: multiple group by is not supported yet.";
    }
  }

  void Aggregate(
    vectordb::engine::TableSegmentMVP *table_segment,
    int64_t idlist_size,
    std::vector<int64_t> ids,
    bool has_distance,
    std::vector<double> distances
  ) {
    // TODO: support multiple group by
    vectordb::query::expr::ExprEvaluator group_by_evaluator(
      group_by_evals[0],
      table_segment->field_name_mem_offset_map_,
      table_segment->primitive_offset_,
      table_segment->var_len_attr_num_,
      table_segment->attribute_table_,
      table_segment->var_len_attr_table_);
    int group_by_root_index = group_by_evals[0].size() - 1;
    std::vector<vectordb::query::expr::ExprEvaluator> aggregation_evaluators;
    for (size_t i = 0; i < aggregation_evals.size(); ++i) {
      aggregation_evaluators.emplace_back(
        aggregation_evals[i],
        table_segment->field_name_mem_offset_map_,
        table_segment->primitive_offset_,
        table_segment->var_len_attr_num_,
        table_segment->attribute_table_,
        table_segment->var_len_attr_table_);
    }
    // Loop through the ids, conduct evaluation, and aggregate
    bool from_id_list = true;
    if (idlist_size == -1) {
      idlist_size = table_segment->record_number_.load();
      from_id_list = false;
    }
    for (size_t i = 0; i < idlist_size; ++i) {
      int64_t id = from_id_list ? ids[i] : i;
      if (group_by_evals[0][group_by_root_index]->value_type == query::expr::ValueType::INT) {
        int64_t key = (int64_t)(group_by_evaluator.NumEvaluate(group_by_root_index, id, has_distance ? distances[i] : 0.0));
        for (size_t j = 0; j < aggregation_evaluators.size(); ++j) {
          auto value = aggregation_evaluators[j].NumEvaluate(aggregation_evals[j].size() - 1, id, has_distance ? distances[i] : 0.0);
          int_aggregators_[j]->addValue(key, value);
        }
      } else if (group_by_evals[0][group_by_root_index]->value_type == query::expr::ValueType::DOUBLE) {
        double key = group_by_evaluator.NumEvaluate(group_by_root_index, id, has_distance ? distances[i] : 0.0);
        for (size_t j = 0; j < aggregation_evaluators.size(); ++j) {
          auto value = aggregation_evaluators[j].NumEvaluate(aggregation_evals[j].size() - 1, id, has_distance ? distances[i] : 0.0);
          double_aggregators_[j]->addValue(key, value);
        }
      } else if (group_by_evals[0][group_by_root_index]->value_type == query::expr::ValueType::BOOL) {
        bool key = group_by_evaluator.LogicalEvaluate(group_by_root_index, id, has_distance ? distances[i] : 0.0);
        for (size_t j = 0; j < aggregation_evaluators.size(); ++j) {
          auto value = aggregation_evaluators[j].NumEvaluate(aggregation_evals[j].size() - 1, id, has_distance ? distances[i] : 0.0);
          bool_aggregators_[j]->addValue(key, value);
        }
      } else if (group_by_evals[0][group_by_root_index]->value_type == query::expr::ValueType::STRING) {
        std::string key = group_by_evaluator.StrEvaluate(group_by_root_index, id);
        for (size_t j = 0; j < aggregation_evaluators.size(); ++j) {
          auto value = aggregation_evaluators[j].NumEvaluate(aggregation_evals[j].size() - 1, id, has_distance ? distances[i] : 0.0);
          string_aggregators_[j]->addValue(key, value);
        }
      }
    }
  }

  void Project(vectordb::Json &result) {
    // TODO: support multiple group by
    result.LoadFromString("[]");
    int group_by_root_index = group_by_evals[0].size() - 1;
    if (global_group_by) {
      for (auto it = int_aggregators_[0]->begin(); it != int_aggregators_[0]->end(); ++it) {
        vectordb::Json obj;
        obj.LoadFromString("{}");
        for (size_t i = 0; i < aggregation_exprs.size(); ++i) {
          auto type = aggregation_evals[i][aggregation_evals[i].size() - 1]->value_type;
          if (type == vectordb::query::expr::ValueType::INT) {
            obj.SetInt(aggregation_exprs[i], (int64_t)(int_aggregators_[i]->getValue(it->first)));
          } else {
            obj.SetDouble(aggregation_exprs[i], int_aggregators_[i]->getValue(it->first));
          }
        }
        result.AddObjectToArray(std::move(obj));
      }
    } else if (group_by_evals[0][group_by_root_index]->value_type == query::expr::ValueType::INT) {
      for (auto it = int_aggregators_[0]->begin(); it != int_aggregators_[0]->end(); ++it) {
        vectordb::Json obj;
        obj.LoadFromString("{}");
        obj.SetInt(group_by_exprs[0], it->first);
        for (size_t i = 0; i < aggregation_exprs.size(); ++i) {
          auto type = aggregation_evals[i][aggregation_evals[i].size() - 1]->value_type;
          if (type == vectordb::query::expr::ValueType::INT) {
            obj.SetInt(aggregation_exprs[i], (int64_t)(int_aggregators_[i]->getValue(it->first)));
          } else {
            obj.SetDouble(aggregation_exprs[i], int_aggregators_[i]->getValue(it->first));
          }
        }
        result.AddObjectToArray(std::move(obj));
      }
    } else if (group_by_evals[0][group_by_root_index]->value_type == query::expr::ValueType::DOUBLE) {
      for (auto it = double_aggregators_[0]->begin(); it != double_aggregators_[0]->end(); ++it) {
        vectordb::Json obj;
        obj.LoadFromString("{}");
        obj.SetDouble(group_by_exprs[0], it->first);
        for (size_t i = 0; i < aggregation_exprs.size(); ++i) {
          auto type = aggregation_evals[i][aggregation_evals[i].size() - 1]->value_type;
          if (type == vectordb::query::expr::ValueType::INT) {
            obj.SetInt(aggregation_exprs[i], (int64_t)(double_aggregators_[i]->getValue(it->first)));
          } else {
            obj.SetDouble(aggregation_exprs[i], double_aggregators_[i]->getValue(it->first));
          }
        }
        result.AddObjectToArray(std::move(obj));
      }
    } else if (group_by_evals[0][group_by_root_index]->value_type == query::expr::ValueType::BOOL) {
      for (auto it = bool_aggregators_[0]->begin(); it != bool_aggregators_[0]->end(); ++it) {
        vectordb::Json obj;
        obj.LoadFromString("{}");
        obj.SetBool(group_by_exprs[0], it->first);
        for (size_t i = 0; i < aggregation_exprs.size(); ++i) {
          auto type = aggregation_evals[i][aggregation_evals[i].size() - 1]->value_type;
          if (type == vectordb::query::expr::ValueType::INT) {
            obj.SetInt(aggregation_exprs[i], (int64_t)(bool_aggregators_[i]->getValue(it->first)));
          } else {
            obj.SetDouble(aggregation_exprs[i], bool_aggregators_[i]->getValue(it->first));
          }
        }
        result.AddObjectToArray(std::move(obj));
      }
    } else if (group_by_evals[0][group_by_root_index]->value_type == query::expr::ValueType::STRING) {
      for (auto it = string_aggregators_[0]->begin(); it != string_aggregators_[0]->end(); ++it) {
        vectordb::Json obj;
        obj.LoadFromString("{}");
        obj.SetString(group_by_exprs[0], it->first);
        for (size_t i = 0; i < aggregation_exprs.size(); ++i) {
          auto type = aggregation_evals[i][aggregation_evals[i].size() - 1]->value_type;
          if (type == vectordb::query::expr::ValueType::INT) {
            obj.SetInt(aggregation_exprs[i], (int64_t)(string_aggregators_[i]->getValue(it->first)));
          } else {
            obj.SetDouble(aggregation_exprs[i], string_aggregators_[i]->getValue(it->first));
          }
        }
        result.AddObjectToArray(std::move(obj));
      }
    } 
  }
};

}
}
}
// vectordb::engine::execution::MinAggregator<std::string> minAggregator;
// minAggregator.addValue("group1", 100);
// minAggregator.addValue("group1", 50.5);
// minAggregator.addValue("group2", 200);

// std::cout << "Min Aggregation Results:" << std::endl;
// for (auto it = minAggregator.begin(); it != minAggregator.end(); ++it) {
//     std::cout << "Key: " << it->first << ", Value: " << it->second << std::endl;
// }

// // Example with a range-based for loop
// vectordb::engine::execution::MaxAggregator<std::string> maxAggregator;
// maxAggregator.addValue("group1", 150);
// maxAggregator.addValue("group1", 200.5);
// maxAggregator.addValue("group2", 100);

// std::cout << "Max Aggregation Results:" << std::endl;
// for (const auto& pair : maxAggregator) {
//     std::cout << "Key: " << pair.first << ", Value: " << pair.second << std::endl;
// }

// vectordb::engine::execution::MinAggregator<double> minAggregator2;
// minAggregator2.addValue(1.5, 100);
// minAggregator2.addValue(1.5, 50.5);
// minAggregator2.addValue(2.3, 200);

// std::cout << "Min Aggregation Results:" << std::endl;
// for (auto it = minAggregator2.begin(); it != minAggregator2.end(); ++it) {
//     std::cout << "Key: " << it->first << ", Value: " << it->second << std::endl;
// }
