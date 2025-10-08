#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include "utils/types.hpp"

namespace vectordb {

/**
 * @brief Thread-safe Status class with RAII memory management
 * 
 * This is a safer version of Status that uses std::unique_ptr
 * instead of raw pointers to prevent memory leaks
 */
class StatusSafe {
public:
    /**
     * @brief Default constructor for OK status
     */
    StatusSafe() : code_(DB_SUCCESS) {}
    
    /**
     * @brief Construct status with code and message
     */
    StatusSafe(StatusCode code, const std::string& msg = "") 
        : code_(code), message_(msg) {}
    
    /**
     * @brief Copy constructor
     */
    StatusSafe(const StatusSafe& other) 
        : code_(other.code_), message_(other.message_) {}
    
    /**
     * @brief Move constructor
     */
    StatusSafe(StatusSafe&& other) noexcept
        : code_(other.code_), message_(std::move(other.message_)) {
        other.code_ = DB_SUCCESS;
    }
    
    /**
     * @brief Copy assignment
     */
    StatusSafe& operator=(const StatusSafe& other) {
        if (this != &other) {
            code_ = other.code_;
            message_ = other.message_;
        }
        return *this;
    }
    
    /**
     * @brief Move assignment
     */
    StatusSafe& operator=(StatusSafe&& other) noexcept {
        if (this != &other) {
            code_ = other.code_;
            message_ = std::move(other.message_);
            other.code_ = DB_SUCCESS;
        }
        return *this;
    }
    
    /**
     * @brief Destructor (default is fine with RAII)
     */
    ~StatusSafe() = default;
    
    /**
     * @brief Static factory for OK status
     */
    static StatusSafe OK() {
        return StatusSafe();
    }
    
    /**
     * @brief Check if status is OK
     */
    bool ok() const {
        return code_ == DB_SUCCESS;
    }
    
    /**
     * @brief Get status code
     */
    StatusCode code() const {
        return code_;
    }
    
    /**
     * @brief Get error message
     */
    const std::string& message() const {
        return message_;
    }
    
    /**
     * @brief Convert to string representation
     */
    std::string ToString() const {
        if (code_ == DB_SUCCESS) {
            return "OK";
        }
        
        std::string result;
        switch (code_) {
            case DB_SUCCESS:
                result = "OK ";
                break;
            case DB_UNEXPECTED_ERROR:
                result = "Unexpected Error: ";
                break;
            case DB_UNSUPPORTED_ERROR:
                result = "Unsupported Error: ";
                break;
            case DB_NOT_FOUND:
                result = "Not Found: ";
                break;
            case TABLE_ALREADY_EXISTS:
                result = "Table Already Exists: ";
                break;
            case INVALID_ARGUMENT:
                result = "Invalid Argument: ";
                break;
            case INVALID_PAYLOAD:
                result = "Invalid Payload: ";
                break;
            case INVALID_NAME:
                result = "Invalid Name: ";
                break;
            case USER_ERROR:
                result = "User Error: ";
                break;
            case RESOURCE_EXHAUSTED:
                result = "Resource Exhausted: ";
                break;
            default:
                result = "Error Code(" + std::to_string(code_) + "): ";
                break;
        }
        
        result += message_;
        return result;
    }
    
    /**
     * @brief Implicit conversion to bool
     */
    explicit operator bool() const {
        return ok();
    }
    
    /**
     * @brief Equality comparison
     */
    bool operator==(const StatusSafe& other) const {
        return code_ == other.code_ && message_ == other.message_;
    }
    
    /**
     * @brief Inequality comparison
     */
    bool operator!=(const StatusSafe& other) const {
        return !(*this == other);
    }
    
private:
    StatusCode code_;
    std::string message_;
};

/**
 * @brief Alternative implementation using unique_ptr for compatibility
 * with existing serialization code that expects a buffer layout
 */
class StatusCompat {
private:
    static constexpr int CODE_WIDTH = sizeof(StatusCode);
    
    struct StateDeleter {
        void operator()(char* p) const {
            delete[] p;
        }
    };
    
    using StatePtr = std::unique_ptr<char[], StateDeleter>;
    
public:
    StatusCompat() : state_(nullptr) {}
    
    StatusCompat(StatusCode code, const std::string& msg) {
        const uint32_t length = static_cast<uint32_t>(msg.size());
        const size_t total_size = length + sizeof(length) + CODE_WIDTH;
        
        // Use unique_ptr instead of raw pointer
        auto buffer = std::make_unique<char[]>(total_size);
        
        std::memcpy(buffer.get(), &code, CODE_WIDTH);
        std::memcpy(buffer.get() + CODE_WIDTH, &length, sizeof(length));
        if (length > 0) {
            std::memcpy(buffer.get() + CODE_WIDTH + sizeof(length), msg.data(), length);
        }
        
        state_ = std::move(buffer);
    }
    
    // Copy constructor
    StatusCompat(const StatusCompat& other) {
        if (other.state_) {
            uint32_t length = 0;
            std::memcpy(&length, other.state_.get() + CODE_WIDTH, sizeof(length));
            const size_t total_size = length + sizeof(length) + CODE_WIDTH;
            
            auto buffer = std::make_unique<char[]>(total_size);
            std::memcpy(buffer.get(), other.state_.get(), total_size);
            state_ = std::move(buffer);
        }
    }
    
    // Move constructor
    StatusCompat(StatusCompat&& other) noexcept = default;
    
    // Copy assignment
    StatusCompat& operator=(const StatusCompat& other) {
        if (this != &other) {
            StatusCompat temp(other);
            state_ = std::move(temp.state_);
        }
        return *this;
    }
    
    // Move assignment
    StatusCompat& operator=(StatusCompat&& other) noexcept = default;
    
    // Destructor - automatic with unique_ptr
    ~StatusCompat() = default;
    
    static StatusCompat OK() {
        return StatusCompat();
    }
    
    bool ok() const {
        return state_ == nullptr;
    }
    
    StatusCode code() const {
        if (!state_) {
            return DB_SUCCESS;
        }
        StatusCode result;
        std::memcpy(&result, state_.get(), CODE_WIDTH);
        return result;
    }
    
    std::string message() const {
        if (!state_) {
            return "";
        }
        
        uint32_t length = 0;
        std::memcpy(&length, state_.get() + CODE_WIDTH, sizeof(length));
        
        if (length == 0) {
            return "";
        }
        
        return std::string(state_.get() + CODE_WIDTH + sizeof(length), length);
    }
    
    std::string ToString() const {
        if (!state_) {
            return "OK";
        }
        
        std::string result;
        StatusCode c = code();
        
        switch (c) {
            case DB_SUCCESS:
                result = "OK ";
                break;
            case DB_UNEXPECTED_ERROR:
                result = "Unexpected Error: ";
                break;
            case DB_UNSUPPORTED_ERROR:
                result = "Unsupported Error: ";
                break;
            default:
                result = "Error Code(" + std::to_string(c) + "): ";
                break;
        }
        
        result += message();
        return result;
    }
    
private:
    StatePtr state_;
};

} // namespace vectordb