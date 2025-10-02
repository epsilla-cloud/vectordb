#pragma once

#include <optional>
#include <utility>
#include <type_traits>
#include <string>

#include "utils/status.hpp"
#include "utils/enhanced_error.hpp"

namespace vectordb {

// Lightweight StatusOr<T> supporting legacy Status for return-value errors
// Usage: return StatusOr<Foo>(Status(DB_UNEXPECTED_ERROR, "...")); or return Foo{...};
// Access: if (!so.ok()) return so.status(); else use *so or so.value();

template <typename T>
class StatusOr {
public:
    StatusOr(const T& value) : value_(value), has_value_(true) {}
    StatusOr(T&& value) : value_(std::move(value)), has_value_(true) {}
    StatusOr(const Status& status) : status_(status), has_value_(false) {}
    StatusOr(Status&& status) : status_(std::move(status)), has_value_(false) {}

    bool ok() const { return has_value_; }

    const Status& status() const {
        if (has_value_) {
            static Status ok = Status::OK();
            return ok;
        }
        return status_;
    }

    T& value() { return *value_; }
    const T& value() const { return *value_; }

    T& operator*() { return *value_; }
    const T& operator*() const { return *value_; }

    T* operator->() { return &(*value_); }
    const T* operator->() const { return &(*value_); }

private:
    std::optional<T> value_;
    Status status_;
    bool has_value_ = false;
};

// Converters between legacy Status and EnhancedStatus
namespace status_adapter {

inline error::EnhancedStatus ToEnhanced(const Status& s) {
    if (s.ok()) return error::EnhancedStatus::OK();
    // Map legacy numeric code into EnhancedErrorCode INTERNAL/0/<code>
    auto code = error::EnhancedErrorCode(error::ErrorCategory::INTERNAL, 0,
                                         static_cast<uint16_t>(s.code() & 0xFFFF));
    return error::EnhancedStatus(code, s.message());
}

inline Status ToLegacy(const error::EnhancedStatus& es) {
    if (es.ok()) return Status::OK();
    // Use packed int32 code for round-trip; fall back to DB_UNEXPECTED_ERROR for non-internal
    int32_t packed = es.code().toInt32();
    int32_t legacy_code = (es.code().category == error::ErrorCategory::INTERNAL)
                              ? es.code().specific_code
                              : DB_UNEXPECTED_ERROR;
    (void)packed; // reserved for future mapping table
    return Status(static_cast<StatusCode>(legacy_code), es.message());
}

} // namespace status_adapter

} // namespace vectordb
