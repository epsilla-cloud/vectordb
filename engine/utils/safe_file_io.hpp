#pragma once

#include <fstream>
#include <string>
#include <vector>
#include <memory>
#include "utils/status.hpp"
#include "logger/logger.hpp"

namespace vectordb {
namespace engine {

// RAII wrapper for safe file operations
class SafeFileWriter {
public:
    SafeFileWriter(const std::string& path, bool binary = true) 
        : path_(path), temp_path_(path + ".tmp"), success_(false) {
        
        // Open temporary file
        auto flags = std::ios::out;
        if (binary) flags |= std::ios::binary;
        
        file_.open(temp_path_, flags);
        if (!file_.good()) {
            error_msg_ = "Failed to open file for writing: " + temp_path_;
        }
    }
    
    ~SafeFileWriter() {
        if (file_.is_open()) {
            file_.close();
        }
        
        // If not successful, remove temp file
        if (!success_ && !temp_path_.empty()) {
            std::remove(temp_path_.c_str());
        }
    }
    
    // Write data to file
    template<typename T>
    bool Write(const T* data, size_t count) {
        if (!file_.good()) return false;
        
        file_.write(reinterpret_cast<const char*>(data), sizeof(T) * count);
        return file_.good();
    }
    
    bool Write(const void* data, size_t bytes) {
        if (!file_.good()) return false;
        
        file_.write(static_cast<const char*>(data), bytes);
        return file_.good();
    }
    
    bool WriteString(const std::string& str) {
        if (!file_.good()) return false;
        
        size_t len = str.length();
        if (!Write(&len, 1)) return false;
        if (len > 0) {
            file_.write(str.data(), len);
        }
        return file_.good();
    }
    
    // Commit the file (rename temp to final)
    Status Commit() {
        if (!file_.good()) {
            return Status(DB_UNEXPECTED_ERROR, error_msg_.empty() ? 
                         "File write failed" : error_msg_);
        }
        
        file_.flush();
        if (!file_.good()) {
            return Status(DB_UNEXPECTED_ERROR, "Failed to flush file: " + temp_path_);
        }
        
        file_.close();
        if (!file_.good()) {
            return Status(DB_UNEXPECTED_ERROR, "Failed to close file: " + temp_path_);
        }
        
        // Atomic rename
        if (std::rename(temp_path_.c_str(), path_.c_str()) != 0) {
            return Status(DB_UNEXPECTED_ERROR, "Failed to rename temp file to: " + path_);
        }
        
        success_ = true;
        return Status::OK();
    }
    
    bool IsGood() const { return file_.good() && error_msg_.empty(); }
    std::string GetError() const { return error_msg_; }
    
private:
    std::string path_;
    std::string temp_path_;
    std::ofstream file_;
    bool success_;
    std::string error_msg_;
};

// RAII wrapper for safe file reading
class SafeFileReader {
public:
    SafeFileReader(const std::string& path, bool binary = true) 
        : path_(path) {
        
        auto flags = std::ios::in;
        if (binary) flags |= std::ios::binary;
        
        file_.open(path_, flags);
        if (!file_.good()) {
            error_msg_ = "Failed to open file for reading: " + path_;
        }
    }
    
    ~SafeFileReader() {
        if (file_.is_open()) {
            file_.close();
        }
    }
    
    // Read data from file
    template<typename T>
    bool Read(T* data, size_t count) {
        if (!file_.good()) return false;
        
        file_.read(reinterpret_cast<char*>(data), sizeof(T) * count);
        return file_.good() && file_.gcount() == static_cast<std::streamsize>(sizeof(T) * count);
    }
    
    bool Read(void* data, size_t bytes) {
        if (!file_.good()) return false;
        
        file_.read(static_cast<char*>(data), bytes);
        return file_.good() && file_.gcount() == static_cast<std::streamsize>(bytes);
    }
    
    bool ReadString(std::string& str) {
        if (!file_.good()) return false;
        
        size_t len;
        if (!Read(&len, 1)) return false;
        
        if (len > 0) {
            str.resize(len);
            file_.read(&str[0], len);
            return file_.good() && file_.gcount() == static_cast<std::streamsize>(len);
        }
        
        str.clear();
        return true;
    }
    
    bool SeekTo(size_t pos) {
        file_.seekg(pos);
        return file_.good();
    }
    
    size_t GetPosition() {
        return static_cast<size_t>(file_.tellg());
    }
    
    bool IsGood() const { return file_.good() && error_msg_.empty(); }
    std::string GetError() const { return error_msg_; }
    
    // Get file size
    size_t GetFileSize() {
        if (!file_.good()) return 0;
        
        auto current = file_.tellg();
        file_.seekg(0, std::ios::end);
        auto size = file_.tellg();
        file_.seekg(current);
        
        return static_cast<size_t>(size);
    }
    
private:
    std::string path_;
    std::ifstream file_;
    std::string error_msg_;
};

} // namespace engine  
} // namespace vectordb