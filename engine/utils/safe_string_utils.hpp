#pragma once

#include <cstdio>
#include <cstdarg>
#include <string>
#include <vector>
#include <sstream>

namespace vectordb {
namespace utils {

/**
 * @brief Safe string formatting utilities to replace unsafe sprintf
 */
class SafeStringFormatter {
public:
    /**
     * @brief Safe sprintf replacement using snprintf
     * 
     * @param buffer Output buffer
     * @param size Buffer size
     * @param format Format string
     * @param ... Variable arguments
     * @return Number of characters written (excluding null terminator)
     */
    static int safe_sprintf(char* buffer, size_t size, const char* format, ...) {
        va_list args;
        va_start(args, format);
        int result = vsnprintf(buffer, size, format, args);
        va_end(args);
        
        // Ensure null termination
        if (result >= 0 && result < size) {
            return result;
        } else if (size > 0) {
            buffer[size - 1] = '\0';
            return size - 1;
        }
        return 0;
    }
    
    /**
     * @brief Safe string formatting to std::string
     * 
     * @param format Format string
     * @param ... Variable arguments
     * @return Formatted string
     */
    template<typename... Args>
    static std::string format(const char* format, Args... args) {
        // First, determine the required buffer size
        int size_needed = std::snprintf(nullptr, 0, format, args...);
        if (size_needed <= 0) {
            return "";
        }
        
        // Allocate buffer and format string
        std::vector<char> buffer(size_needed + 1);
        std::snprintf(buffer.data(), buffer.size(), format, args...);
        
        return std::string(buffer.data());
    }
    
    /**
     * @brief Safe array-to-string conversion for SIMD debugging
     * 
     * @param data Array of data
     * @param count Number of elements
     * @param format Format string for each element
     * @return Formatted string
     */
    template<typename T>
    static std::string array_to_string(const T* data, size_t count, const char* format) {
        std::ostringstream oss;
        for (size_t i = 0; i < count; ++i) {
            if (i > 0) oss << ",";
            
            // Use type-safe formatting
            if constexpr (std::is_floating_point_v<T>) {
                char buffer[64];
                std::snprintf(buffer, sizeof(buffer), format, static_cast<double>(data[i]));
                oss << buffer;
            } else if constexpr (std::is_integral_v<T>) {
                char buffer[32];
                std::snprintf(buffer, sizeof(buffer), format, static_cast<long long>(data[i]));
                oss << buffer;
            } else {
                oss << data[i];
            }
        }
        return oss.str();
    }
    
    /**
     * @brief Safe SIMD vector to string conversion
     */
    template<typename T>
    static std::string simd_to_string(const T* data, size_t count, const char* separator = ",") {
        std::ostringstream oss;
        for (size_t i = 0; i < count; ++i) {
            if (i > 0) oss << separator;
            oss << data[i];
        }
        return oss.str();
    }
};

/**
 * @brief Safe replacement macros for common unsafe functions
 */

// Replace sprintf with safe version
#define SAFE_SPRINTF(buffer, size, ...) \
    vectordb::utils::SafeStringFormatter::safe_sprintf(buffer, size, __VA_ARGS__)

// Replace sprintf for string building
#define SAFE_FORMAT(...) \
    vectordb::utils::SafeStringFormatter::format(__VA_ARGS__)

/**
 * @brief RAII string buffer for safe string building
 */
class SafeStringBuffer {
public:
    explicit SafeStringBuffer(size_t initial_capacity = 1024) 
        : capacity_(initial_capacity), used_(0) {
        buffer_ = std::make_unique<char[]>(capacity_);
        buffer_[0] = '\0';
    }
    
    /**
     * @brief Append formatted string to buffer
     */
    template<typename... Args>
    bool append(const char* format, Args... args) {
        size_t remaining = capacity_ - used_;
        
        // Try to format
        int written = std::snprintf(buffer_.get() + used_, remaining, format, args...);
        
        if (written < 0) {
            return false;  // Format error
        }
        
        if (written >= remaining) {
            // Need to grow buffer
            size_t new_capacity = capacity_ * 2;
            while (new_capacity < used_ + written + 1) {
                new_capacity *= 2;
            }
            
            auto new_buffer = std::make_unique<char[]>(new_capacity);
            std::memcpy(new_buffer.get(), buffer_.get(), used_);
            
            // Retry formatting
            written = std::snprintf(new_buffer.get() + used_, 
                                  new_capacity - used_, format, args...);
            
            buffer_ = std::move(new_buffer);
            capacity_ = new_capacity;
        }
        
        used_ += written;
        return true;
    }
    
    /**
     * @brief Get the built string
     */
    std::string str() const {
        return std::string(buffer_.get(), used_);
    }
    
    /**
     * @brief Get C-string
     */
    const char* c_str() const {
        return buffer_.get();
    }
    
    /**
     * @brief Clear the buffer
     */
    void clear() {
        used_ = 0;
        buffer_[0] = '\0';
    }
    
    /**
     * @brief Get current size
     */
    size_t size() const {
        return used_;
    }
    
private:
    std::unique_ptr<char[]> buffer_;
    size_t capacity_;
    size_t used_;
};

/**
 * @brief Path sanitization utilities
 */
class PathSanitizer {
public:
    /**
     * @brief Sanitize a file path to prevent directory traversal
     * 
     * @param path Input path
     * @return Sanitized path
     */
    static std::string sanitize(const std::string& path) {
        std::string result;
        result.reserve(path.size());
        
        bool last_was_slash = false;
        int dot_count = 0;
        
        for (char c : path) {
            if (c == '/' || c == '\\') {
                if (!last_was_slash) {
                    result += '/';
                    last_was_slash = true;
                }
                dot_count = 0;
            } else if (c == '.') {
                dot_count++;
                if (dot_count <= 1) {
                    result += c;
                }
                // Ignore multiple dots (../)
            } else {
                result += c;
                last_was_slash = false;
                dot_count = 0;
            }
        }
        
        return result;
    }
    
    /**
     * @brief Check if path is safe (no directory traversal)
     */
    static bool is_safe(const std::string& path) {
        return path.find("..") == std::string::npos &&
               path.find("~") == std::string::npos;
    }
};

} // namespace utils
} // namespace vectordb