#pragma once

#include <cstdint>
#include <exception>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <chrono>
#include <functional>
#include <sstream>
#include <thread>

#include "utils/json.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace error {

/**
 * @brief Enhanced error code system with categories and subcategories
 */
enum class ErrorCategory : uint8_t {
  SUCCESS = 0,
  INFRASTRUCTURE = 1,
  DATABASE = 2,
  NETWORK = 3,
  VALIDATION = 4,
  AUTHENTICATION = 5,
  AUTHORIZATION = 6,
  RESOURCE = 7,
  CONFIGURATION = 8,
  INTERNAL = 9,
  USER_INPUT = 10,
  EXTERNAL_SERVICE = 11
};

/**
 * @brief Enhanced error code structure
 */
struct EnhancedErrorCode {
  ErrorCategory category;
  uint8_t subcategory;
  uint16_t specific_code;
  
  EnhancedErrorCode(ErrorCategory cat = ErrorCategory::SUCCESS, 
                   uint8_t sub = 0, 
                   uint16_t code = 0) 
    : category(cat), subcategory(sub), specific_code(code) {}
  
  // Convert to 32-bit integer for compatibility
  int32_t toInt32() const {
    return (static_cast<int32_t>(category) << 24) |
           (static_cast<int32_t>(subcategory) << 16) |
           static_cast<int32_t>(specific_code);
  }
  
  static EnhancedErrorCode fromInt32(int32_t code) {
    return EnhancedErrorCode(
      static_cast<ErrorCategory>((code >> 24) & 0xFF),
      static_cast<uint8_t>((code >> 16) & 0xFF),
      static_cast<uint16_t>(code & 0xFFFF)
    );
  }
  
  bool operator==(const EnhancedErrorCode& other) const {
    return category == other.category && 
           subcategory == other.subcategory && 
           specific_code == other.specific_code;
  }
  
  bool isSuccess() const { return category == ErrorCategory::SUCCESS; }
};

/**
 * @brief Error context information
 */
struct ErrorContext {
  std::string function_name;
  std::string file_name;
  int line_number;
  std::string operation;
  std::unordered_map<std::string, std::string> metadata;
  std::chrono::system_clock::time_point timestamp;
  
  ErrorContext() : line_number(0), timestamp(std::chrono::system_clock::now()) {}
  
  ErrorContext(const std::string& func, const std::string& file, int line, const std::string& op = "")
    : function_name(func), file_name(file), line_number(line), operation(op), 
      timestamp(std::chrono::system_clock::now()) {}
  
  ErrorContext& addMetadata(const std::string& key, const std::string& value) {
    metadata[key] = value;
    return *this;
  }
  
  std::string toString() const {
    std::ostringstream oss;
    oss << "[" << function_name << " at " << file_name << ":" << line_number << "]";
    if (!operation.empty()) {
      oss << " Operation: " << operation;
    }
    if (!metadata.empty()) {
      oss << " Metadata: {";
      bool first = true;
      for (const auto& [key, value] : metadata) {
        if (!first) oss << ", ";
        oss << key << ": " << value;
        first = false;
      }
      oss << "}";
    }
    return oss.str();
  }
};

/**
 * @brief Error recovery action
 */
class ErrorRecoveryAction {
public:
  virtual ~ErrorRecoveryAction() = default;
  virtual bool canRecover(const EnhancedErrorCode& error) const = 0;
  virtual bool attempt(const ErrorContext& context) = 0;
  virtual std::string description() const = 0;
};

/**
 * @brief Retry recovery action
 */
class RetryRecoveryAction : public ErrorRecoveryAction {
private:
  int max_attempts_;
  std::chrono::milliseconds delay_;
  std::function<bool()> operation_;
  
public:
  RetryRecoveryAction(int max_attempts, std::chrono::milliseconds delay, std::function<bool()> op)
    : max_attempts_(max_attempts), delay_(delay), operation_(op) {}
  
  bool canRecover(const EnhancedErrorCode& error) const override {
    // Can retry for network, resource, or external service errors
    return error.category == ErrorCategory::NETWORK ||
           error.category == ErrorCategory::RESOURCE ||
           error.category == ErrorCategory::EXTERNAL_SERVICE;
  }
  
  bool attempt(const ErrorContext& context) override {
    for (int i = 0; i < max_attempts_; ++i) {
      if (i > 0) {
        std::this_thread::sleep_for(delay_);
      }
      
      if (operation_()) {
        return true;
      }
    }
    return false;
  }
  
  std::string description() const override {
    return "Retry up to " + std::to_string(max_attempts_) + " times with " + 
           std::to_string(delay_.count()) + "ms delay";
  }
};

/**
 * @brief Enhanced status with rich error information and recovery capabilities
 */
class EnhancedStatus {
private:
  EnhancedErrorCode error_code_;
  std::string message_;
  std::vector<ErrorContext> context_stack_;
  std::vector<std::shared_ptr<ErrorRecoveryAction>> recovery_actions_;
  mutable vectordb::engine::Logger logger_;
  
public:
  EnhancedStatus() : error_code_(ErrorCategory::SUCCESS) {}
  
  EnhancedStatus(const EnhancedErrorCode& code, 
                const std::string& message,
                const ErrorContext& context = ErrorContext())
    : error_code_(code), message_(message) {
    if (!context.function_name.empty()) {
      context_stack_.push_back(context);
    }
  }
  
  // Compatibility constructor
  EnhancedStatus(int32_t code, const std::string& message)
    : error_code_(EnhancedErrorCode::fromInt32(code)), message_(message) {}
  
  static EnhancedStatus OK() { return EnhancedStatus(); }
  
  static EnhancedStatus Error(ErrorCategory category, uint8_t subcategory, uint16_t specific_code,
                             const std::string& message, const ErrorContext& context = ErrorContext()) {
    return EnhancedStatus(EnhancedErrorCode(category, subcategory, specific_code), message, context);
  }
  
  // Predefined error creators
  static EnhancedStatus DatabaseError(uint16_t code, const std::string& msg, const ErrorContext& ctx = ErrorContext()) {
    return Error(ErrorCategory::DATABASE, 0, code, msg, ctx);
  }
  
  static EnhancedStatus ValidationError(uint16_t code, const std::string& msg, const ErrorContext& ctx = ErrorContext()) {
    return Error(ErrorCategory::VALIDATION, 0, code, msg, ctx);
  }
  
  static EnhancedStatus NetworkError(uint16_t code, const std::string& msg, const ErrorContext& ctx = ErrorContext()) {
    return Error(ErrorCategory::NETWORK, 0, code, msg, ctx);
  }
  
  static EnhancedStatus ResourceError(uint16_t code, const std::string& msg, const ErrorContext& ctx = ErrorContext()) {
    return Error(ErrorCategory::RESOURCE, 0, code, msg, ctx);
  }
  
  static EnhancedStatus ConfigError(uint16_t code, const std::string& msg, const ErrorContext& ctx = ErrorContext()) {
    return Error(ErrorCategory::CONFIGURATION, 0, code, msg, ctx);
  }

  bool ok() const { return error_code_.isSuccess(); }
  
  const EnhancedErrorCode& code() const { return error_code_; }
  
  int32_t legacyCode() const { return error_code_.toInt32(); }
  
  const std::string& message() const { return message_; }
  
  const std::vector<ErrorContext>& contextStack() const { return context_stack_; }

  /**
   * @brief Add context to error (for error propagation)
   */
  EnhancedStatus& addContext(const ErrorContext& context) {
    context_stack_.push_back(context);
    return *this;
  }
  
  /**
   * @brief Add recovery action
   */
  EnhancedStatus& addRecoveryAction(std::shared_ptr<ErrorRecoveryAction> action) {
    recovery_actions_.push_back(action);
    return *this;
  }
  
  /**
   * @brief Attempt automatic recovery
   */
  bool attemptRecovery() {
    for (auto& action : recovery_actions_) {
      if (action->canRecover(error_code_)) {
        logger_.Info("Attempting recovery: " + action->description());
        
        ErrorContext recovery_context("attemptRecovery", __FILE__, __LINE__, "error_recovery");
        if (action->attempt(recovery_context)) {
          logger_.Info("Recovery successful");
          return true;
        }
      }
    }
    logger_.Warning("No recovery actions available or all failed");
    return false;
  }

  /**
   * @brief Get detailed error description
   */
  std::string detailedDescription() const {
    std::ostringstream oss;
    
    // Basic error info
    oss << "Error: " << categoryToString(error_code_.category);
    if (error_code_.subcategory > 0) {
      oss << "." << static_cast<int>(error_code_.subcategory);
    }
    oss << "." << error_code_.specific_code;
    oss << " - " << message_ << "\n";
    
    // Context stack
    if (!context_stack_.empty()) {
      oss << "Context Stack:\n";
      for (size_t i = 0; i < context_stack_.size(); ++i) {
        oss << "  " << (i + 1) << ". " << context_stack_[i].toString() << "\n";
      }
    }
    
    // Recovery actions
    if (!recovery_actions_.empty()) {
      oss << "Available Recovery Actions:\n";
      for (size_t i = 0; i < recovery_actions_.size(); ++i) {
        oss << "  " << (i + 1) << ". " << recovery_actions_[i]->description() << "\n";
      }
    }
    
    return oss.str();
  }

  /**
   * @brief Convert to JSON for structured logging
   */
  vectordb::Json toJson() const {
    vectordb::Json json;
    
    json.SetInt("error_code", error_code_.toInt32());
    json.SetString("category", categoryToString(error_code_.category));
    json.SetInt("subcategory", error_code_.subcategory);
    json.SetInt("specific_code", error_code_.specific_code);
    json.SetString("message", message_);
    
    if (!context_stack_.empty()) {
      vectordb::Json context_array;
      context_array.LoadFromString("[]");
      // Add context items (would need proper JSON array support)
    }
    
    return json;
  }

  /**
   * @brief Convert error category to string
   */
  static std::string categoryToString(ErrorCategory category) {
    switch (category) {
      case ErrorCategory::SUCCESS: return "SUCCESS";
      case ErrorCategory::INFRASTRUCTURE: return "INFRASTRUCTURE";
      case ErrorCategory::DATABASE: return "DATABASE";
      case ErrorCategory::NETWORK: return "NETWORK";
      case ErrorCategory::VALIDATION: return "VALIDATION";
      case ErrorCategory::AUTHENTICATION: return "AUTHENTICATION";
      case ErrorCategory::AUTHORIZATION: return "AUTHORIZATION";
      case ErrorCategory::RESOURCE: return "RESOURCE";
      case ErrorCategory::CONFIGURATION: return "CONFIGURATION";
      case ErrorCategory::INTERNAL: return "INTERNAL";
      case ErrorCategory::USER_INPUT: return "USER_INPUT";
      case ErrorCategory::EXTERNAL_SERVICE: return "EXTERNAL_SERVICE";
      default: return "UNKNOWN";
    }
  }

  // Compatibility with old Status class
  std::string ToString() const {
    return detailedDescription();
  }
};

/**
 * @brief Enhanced error registry for centralized error management
 */
class ErrorRegistry {
private:
  std::unordered_map<int32_t, std::string> error_descriptions_;
  std::unordered_map<int32_t, std::vector<std::shared_ptr<ErrorRecoveryAction>>> default_recovery_actions_;
  vectordb::engine::Logger logger_;
  
public:
  static ErrorRegistry& instance() {
    static ErrorRegistry registry;
    return registry;
  }

  void registerError(const EnhancedErrorCode& code, const std::string& description) {
    error_descriptions_[code.toInt32()] = description;
  }
  
  void registerRecoveryAction(const EnhancedErrorCode& code, std::shared_ptr<ErrorRecoveryAction> action) {
    default_recovery_actions_[code.toInt32()].push_back(action);
  }
  
  std::string getDescription(const EnhancedErrorCode& code) const {
    auto it = error_descriptions_.find(code.toInt32());
    return (it != error_descriptions_.end()) ? it->second : "Unknown error";
  }
  
  EnhancedStatus createStatus(const EnhancedErrorCode& code, const std::string& message = "",
                             const ErrorContext& context = ErrorContext()) {
    std::string final_message = message.empty() ? getDescription(code) : message;
    EnhancedStatus status(code, final_message, context);
    
    // Add default recovery actions
    auto it = default_recovery_actions_.find(code.toInt32());
    if (it != default_recovery_actions_.end()) {
      for (const auto& action : it->second) {
        status.addRecoveryAction(action);
      }
    }
    
    return status;
  }

private:
  ErrorRegistry() {
    // Register common errors
    registerCommonErrors();
  }
  
  void registerCommonErrors() {
    // Database errors
    registerError(EnhancedErrorCode(ErrorCategory::DATABASE, 0, 1), "Database connection failed");
    registerError(EnhancedErrorCode(ErrorCategory::DATABASE, 0, 2), "Table not found");
    registerError(EnhancedErrorCode(ErrorCategory::DATABASE, 0, 3), "Record not found");
    registerError(EnhancedErrorCode(ErrorCategory::DATABASE, 0, 4), "Table already exists");
    registerError(EnhancedErrorCode(ErrorCategory::DATABASE, 0, 5), "Invalid query");
    registerError(EnhancedErrorCode(ErrorCategory::DATABASE, 0, 6), "Transaction failed");
    
    // Validation errors
    registerError(EnhancedErrorCode(ErrorCategory::VALIDATION, 0, 1), "Invalid input format");
    registerError(EnhancedErrorCode(ErrorCategory::VALIDATION, 0, 2), "Required field missing");
    registerError(EnhancedErrorCode(ErrorCategory::VALIDATION, 0, 3), "Value out of range");
    registerError(EnhancedErrorCode(ErrorCategory::VALIDATION, 0, 4), "Invalid data type");
    
    // Network errors
    registerError(EnhancedErrorCode(ErrorCategory::NETWORK, 0, 1), "Connection timeout");
    registerError(EnhancedErrorCode(ErrorCategory::NETWORK, 0, 2), "Connection refused");
    registerError(EnhancedErrorCode(ErrorCategory::NETWORK, 0, 3), "Network unreachable");
    registerError(EnhancedErrorCode(ErrorCategory::NETWORK, 0, 4), "Host not found");
    
    // Resource errors
    registerError(EnhancedErrorCode(ErrorCategory::RESOURCE, 0, 1), "Out of memory");
    registerError(EnhancedErrorCode(ErrorCategory::RESOURCE, 0, 2), "Disk full");
    registerError(EnhancedErrorCode(ErrorCategory::RESOURCE, 0, 3), "File not found");
    registerError(EnhancedErrorCode(ErrorCategory::RESOURCE, 0, 4), "Permission denied");
    registerError(EnhancedErrorCode(ErrorCategory::RESOURCE, 0, 5), "Resource busy");
    
    // Configuration errors
    registerError(EnhancedErrorCode(ErrorCategory::CONFIGURATION, 0, 1), "Invalid configuration");
    registerError(EnhancedErrorCode(ErrorCategory::CONFIGURATION, 0, 2), "Configuration file not found");
    registerError(EnhancedErrorCode(ErrorCategory::CONFIGURATION, 0, 3), "Configuration parse error");
  }
};

// Convenience macros for creating error context
#define ERROR_CONTEXT() ErrorContext(__FUNCTION__, __FILE__, __LINE__)
#define ERROR_CONTEXT_OP(op) ErrorContext(__FUNCTION__, __FILE__, __LINE__, op)

// Convenience functions for common error types
inline EnhancedStatus DatabaseError(uint16_t code, const std::string& message) {
  return EnhancedStatus::DatabaseError(code, message, ERROR_CONTEXT());
}

inline EnhancedStatus ValidationError(uint16_t code, const std::string& message) {
  return EnhancedStatus::ValidationError(code, message, ERROR_CONTEXT());
}

inline EnhancedStatus NetworkError(uint16_t code, const std::string& message) {
  return EnhancedStatus::NetworkError(code, message, ERROR_CONTEXT());
}

inline EnhancedStatus ResourceError(uint16_t code, const std::string& message) {
  return EnhancedStatus::ResourceError(code, message, ERROR_CONTEXT());
}

inline EnhancedStatus ConfigError(uint16_t code, const std::string& message) {
  return EnhancedStatus::ConfigError(code, message, ERROR_CONTEXT());
}

} // namespace error

// Note:
// We intentionally avoid aliasing vectordb::Status to EnhancedStatus here to prevent clashes
// with the legacy Status class defined in utils/status.hpp. Use error::EnhancedStatus directly
// or the adapter helpers in utils/status_adapters.hpp to convert between representations.

} // namespace vectordb