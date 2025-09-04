#pragma once

#include <string>
#include <regex>
#include <filesystem>
#include <algorithm>
#include <set>
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

class PathValidator {
public:
  // Validate and sanitize a file/directory path
  static Status ValidatePath(const std::string& path, std::string& sanitized_path, bool allow_relative = false) {
    if (path.empty()) {
      return Status(INVALID_PAYLOAD, "Path cannot be empty");
    }
    
    // Check for null bytes
    if (path.find('\0') != std::string::npos) {
      return Status(INVALID_PAYLOAD, "Path contains null bytes");
    }
    
    // Check for dangerous patterns
    if (ContainsDangerousPatterns(path)) {
      return Status(INVALID_PAYLOAD, "Path contains dangerous patterns");
    }
    
    // Normalize path using filesystem library
    try {
      std::filesystem::path fs_path(path);
      
      // Check if path is absolute or relative
      if (!allow_relative && !fs_path.is_absolute()) {
        return Status(INVALID_PAYLOAD, "Path must be absolute");
      }
      
      // Get canonical path (resolves .., ., and symlinks)
      if (std::filesystem::exists(fs_path)) {
        sanitized_path = std::filesystem::canonical(fs_path).string();
      } else {
        // For non-existent paths, normalize without canonical
        sanitized_path = std::filesystem::absolute(fs_path).lexically_normal().string();
      }
      
      // Additional check for path traversal after normalization
      if (!allow_relative && !IsPathSafe(sanitized_path)) {
        return Status(INVALID_PAYLOAD, "Path traversal detected");
      }
      
    } catch (const std::filesystem::filesystem_error& e) {
      return Status(INVALID_PAYLOAD, "Invalid path: " + std::string(e.what()));
    }
    
    return Status::OK();
  }
  
  // Validate database name
  static Status ValidateDbName(const std::string& name, std::string& sanitized_name) {
    if (name.empty()) {
      return Status(INVALID_PAYLOAD, "Database name cannot be empty");
    }
    
    if (name.length() > 255) {
      return Status(INVALID_PAYLOAD, "Database name too long (max 255 characters)");
    }
    
    // Allow only alphanumeric, underscore, and hyphen
    static const std::regex valid_name_regex("^[a-zA-Z0-9_-]+$");
    if (!std::regex_match(name, valid_name_regex)) {
      return Status(INVALID_PAYLOAD, "Database name contains invalid characters. Only alphanumeric, underscore, and hyphen are allowed");
    }
    
    // Check for reserved names
    static const std::set<std::string> reserved_names = {
      ".", "..", "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4",
      "com5", "com6", "com7", "com8", "com9", "lpt1", "lpt2", "lpt3", "lpt4",
      "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"
    };
    
    std::string lower_name = name;
    std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
    if (reserved_names.find(lower_name) != reserved_names.end()) {
      return Status(INVALID_PAYLOAD, "Database name is reserved");
    }
    
    sanitized_name = name;
    return Status::OK();
  }
  
  // Validate table name
  static Status ValidateTableName(const std::string& name, std::string& sanitized_name) {
    // Same rules as database name
    return ValidateDbName(name, sanitized_name);
  }
  
  // Validate field name
  static Status ValidateFieldName(const std::string& name, std::string& sanitized_name) {
    if (name.empty()) {
      return Status(INVALID_PAYLOAD, "Field name cannot be empty");
    }
    
    if (name.length() > 255) {
      return Status(INVALID_PAYLOAD, "Field name too long (max 255 characters)");
    }
    
    // Allow alphanumeric, underscore, and basic punctuation
    static const std::regex valid_field_regex("^[a-zA-Z][a-zA-Z0-9_]*$");
    if (!std::regex_match(name, valid_field_regex)) {
      return Status(INVALID_PAYLOAD, "Field name must start with a letter and contain only alphanumeric characters and underscores");
    }
    
    sanitized_name = name;
    return Status::OK();
  }
  
private:
  static bool ContainsDangerousPatterns(const std::string& path) {
    // Check for directory traversal patterns
    if (path.find("../") != std::string::npos || path.find("..\\") != std::string::npos) {
      return true;
    }
    
    // Check for absolute path indicators that shouldn't be there
    static const std::vector<std::string> dangerous_patterns = {
      "~", // Home directory expansion
      "${", // Environment variable expansion
      "$(", // Command substitution
      "`",  // Command substitution
      "|",  // Pipe
      "&",  // Background execution
      ";",  // Command separator
      ">",  // Redirect
      "<",  // Redirect
      "*",  // Glob (when not expected)
      "?",  // Glob (when not expected)
      "[",  // Glob (when not expected)
      "\n", // Newline
      "\r", // Carriage return
    };
    
    for (const auto& pattern : dangerous_patterns) {
      if (path.find(pattern) != std::string::npos) {
        return true;
      }
    }
    
    return false;
  }
  
  static bool IsPathSafe(const std::string& path) {
    // Ensure path doesn't escape to sensitive directories
    static const std::vector<std::string> sensitive_prefixes = {
      "/etc",
      "/sys",
      "/proc",
      "/dev",
      "/boot",
      "/root",
      "/usr/bin",
      "/usr/sbin",
      "/bin",
      "/sbin"
    };
    
    for (const auto& prefix : sensitive_prefixes) {
      if (path.find(prefix) == 0) {
        return false;
      }
    }
    
    return true;
  }
};

} // namespace engine
} // namespace vectordb