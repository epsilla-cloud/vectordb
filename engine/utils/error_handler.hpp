#pragma once

#include <string>
#include <exception>
#include <functional>
#include <sstream>
#include "logger/logger.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace engine {

// Custom exception classes for better error categorization
class VectorDBException : public std::exception {
public:
    VectorDBException(ErrorCode code, const std::string& message) 
        : code_(code), message_(message) {
        full_message_ = "[Error " + std::to_string(code) + "] " + message;
    }
    
    const char* what() const noexcept override {
        return full_message_.c_str();
    }
    
    ErrorCode code() const { return code_; }
    const std::string& message() const { return message_; }
    
private:
    ErrorCode code_;
    std::string message_;
    std::string full_message_;
};

class MemoryException : public VectorDBException {
public:
    MemoryException(const std::string& message) 
        : VectorDBException(DB_OUT_OF_MEMORY, message) {}
};

class IOException : public VectorDBException {
public:
    IOException(const std::string& message) 
        : VectorDBException(DB_ERROR, message) {}
};

class ValidationException : public VectorDBException {
public:
    ValidationException(const std::string& message) 
        : VectorDBException(INVALID_PAYLOAD, message) {}
};

class ConcurrencyException : public VectorDBException {
public:
    ConcurrencyException(const std::string& message) 
        : VectorDBException(DB_UNEXPECTED_ERROR, message) {}
};

// RAII-based error context for better error reporting
class ErrorContext {
public:
    ErrorContext(const std::string& operation) : operation_(operation) {
        start_time_ = std::chrono::steady_clock::now();
    }
    
    ~ErrorContext() {
        if (!completed_ && !std::uncaught_exceptions()) {
            logger_.Warn("Operation '" + operation_ + "' exited without completion marker");
        }
    }
    
    void Complete() { completed_ = true; }
    
    void AddContext(const std::string& key, const std::string& value) {
        context_[key] = value;
    }
    
    std::string GetContext() const {
        std::stringstream ss;
        ss << "Operation: " << operation_;
        for (const auto& [key, value] : context_) {
            ss << ", " << key << ": " << value;
        }
        auto duration = std::chrono::steady_clock::now() - start_time_;
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        ss << ", Duration: " << ms << "ms";
        return ss.str();
    }
    
private:
    std::string operation_;
    std::map<std::string, std::string> context_;
    std::chrono::steady_clock::time_point start_time_;
    bool completed_ = false;
    Logger logger_;
};

// Error handler with retry logic
class ErrorHandler {
public:
    template<typename Func>
    static Status ExecuteWithRetry(
        const std::string& operation,
        Func&& func,
        int max_retries = 3,
        std::chrono::milliseconds retry_delay = std::chrono::milliseconds(100)) {
        
        Logger logger;
        ErrorContext context(operation);
        
        for (int attempt = 0; attempt <= max_retries; ++attempt) {
            try {
                Status status = func();
                if (status.ok()) {
                    context.Complete();
                    return status;
                }
                
                // Don't retry on validation errors
                if (status.code() == INVALID_PAYLOAD || 
                    status.code() == INVALID_TABLE_NAME ||
                    status.code() == INVALID_FIELD_NAME) {
                    logger.Error(operation + " failed with validation error: " + status.message());
                    return status;
                }
                
                // Log and retry for other errors
                if (attempt < max_retries) {
                    logger.Warn(operation + " failed (attempt " + std::to_string(attempt + 1) + 
                               "/" + std::to_string(max_retries + 1) + "): " + status.message());
                    std::this_thread::sleep_for(retry_delay);
                    retry_delay *= 2; // Exponential backoff
                } else {
                    logger.Error(operation + " failed after " + std::to_string(max_retries + 1) + 
                                " attempts: " + status.message());
                    return status;
                }
                
            } catch (const VectorDBException& e) {
                logger.Error(operation + " threw exception: " + std::string(e.what()));
                if (attempt >= max_retries) {
                    return Status(e.code(), e.message() + " [Context: " + context.GetContext() + "]");
                }
                std::this_thread::sleep_for(retry_delay);
                retry_delay *= 2;
                
            } catch (const std::bad_alloc& e) {
                std::string msg = operation + " failed with memory allocation error";
                logger.Error(msg);
                return Status(DB_OUT_OF_MEMORY, msg + " [Context: " + context.GetContext() + "]");
                
            } catch (const std::exception& e) {
                std::string msg = operation + " failed with exception: " + std::string(e.what());
                logger.Error(msg);
                if (attempt >= max_retries) {
                    return Status(DB_UNEXPECTED_ERROR, msg + " [Context: " + context.GetContext() + "]");
                }
                std::this_thread::sleep_for(retry_delay);
                retry_delay *= 2;
                
            } catch (...) {
                std::string msg = operation + " failed with unknown exception";
                logger.Error(msg);
                return Status(DB_UNEXPECTED_ERROR, msg + " [Context: " + context.GetContext() + "]");
            }
        }
        
        return Status(DB_UNEXPECTED_ERROR, operation + " failed after all retries");
    }
    
    // Execute with cleanup on failure
    template<typename Func, typename Cleanup>
    static Status ExecuteWithCleanup(
        const std::string& operation,
        Func&& func,
        Cleanup&& cleanup) {
        
        Logger logger;
        ErrorContext context(operation);
        
        try {
            Status status = func();
            if (status.ok()) {
                context.Complete();
                return status;
            }
            
            // Cleanup on failure
            logger.Info("Executing cleanup for failed operation: " + operation);
            cleanup();
            return status;
            
        } catch (const std::exception& e) {
            logger.Error(operation + " failed with exception: " + std::string(e.what()));
            try {
                cleanup();
            } catch (const std::exception& cleanup_e) {
                logger.Error("Cleanup also failed: " + std::string(cleanup_e.what()));
            }
            throw;
        }
    }
    
    // Safe file operations with proper error handling
    static Status SafeFileWrite(
        const std::string& file_path,
        const std::string& content,
        bool create_backup = true) {
        
        Logger logger;
        std::string backup_path;
        
        if (create_backup && std::filesystem::exists(file_path)) {
            backup_path = file_path + ".backup";
            try {
                std::filesystem::copy_file(file_path, backup_path, 
                    std::filesystem::copy_options::overwrite_existing);
            } catch (const std::filesystem::filesystem_error& e) {
                return Status(DB_ERROR, "Failed to create backup: " + std::string(e.what()));
            }
        }
        
        return ExecuteWithCleanup(
            "Write file " + file_path,
            [&]() -> Status {
                std::string tmp_path = file_path + ".tmp";
                
                // Write to temporary file first
                FILE* file = fopen(tmp_path.c_str(), "wb");
                if (!file) {
                    return Status(DB_ERROR, "Cannot open temporary file: " + tmp_path);
                }
                
                size_t written = fwrite(content.c_str(), 1, content.size(), file);
                fclose(file);
                
                if (written != content.size()) {
                    std::filesystem::remove(tmp_path);
                    return Status(DB_ERROR, "Failed to write complete content");
                }
                
                // Atomic rename
                try {
                    std::filesystem::rename(tmp_path, file_path);
                } catch (const std::filesystem::filesystem_error& e) {
                    std::filesystem::remove(tmp_path);
                    return Status(DB_ERROR, "Failed to rename file: " + std::string(e.what()));
                }
                
                // Remove backup on success
                if (!backup_path.empty()) {
                    std::filesystem::remove(backup_path);
                }
                
                return Status::OK();
            },
            [&]() {
                // Restore from backup on failure
                if (!backup_path.empty() && std::filesystem::exists(backup_path)) {
                    try {
                        std::filesystem::rename(backup_path, file_path);
                        logger.Info("Restored file from backup: " + file_path);
                    } catch (const std::filesystem::filesystem_error& e) {
                        logger.Error("Failed to restore backup: " + std::string(e.what()));
                    }
                }
            }
        );
    }
    
    // Validate and throw if invalid
    template<typename T>
    static void Validate(bool condition, const std::string& message) {
        if (!condition) {
            throw T(message);
        }
    }
    
    static void ValidateNotNull(const void* ptr, const std::string& name) {
        if (ptr == nullptr) {
            throw ValidationException(name + " cannot be null");
        }
    }
    
    static void ValidateRange(int64_t value, int64_t min, int64_t max, const std::string& name) {
        if (value < min || value > max) {
            throw ValidationException(name + " must be between " + 
                std::to_string(min) + " and " + std::to_string(max));
        }
    }
};

// Macro for easier error context management
#define ERROR_CONTEXT(operation) ErrorContext _error_context(operation)
#define ADD_ERROR_CONTEXT(key, value) _error_context.AddContext(key, value)
#define COMPLETE_OPERATION() _error_context.Complete()

// Macro for validation with proper error messages
#define VALIDATE(condition, message) \
    do { \
        if (!(condition)) { \
            return Status(INVALID_PAYLOAD, message); \
        } \
    } while(0)

#define VALIDATE_NOT_NULL(ptr, name) \
    do { \
        if ((ptr) == nullptr) { \
            return Status(INVALID_PAYLOAD, std::string(name) + " cannot be null"); \
        } \
    } while(0)

#define VALIDATE_RANGE(value, min, max, name) \
    do { \
        if ((value) < (min) || (value) > (max)) { \
            return Status(INVALID_PAYLOAD, std::string(name) + " must be between " + \
                         std::to_string(min) + " and " + std::to_string(max)); \
        } \
    } while(0)

} // namespace engine
} // namespace vectordb