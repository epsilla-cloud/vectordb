#pragma once

#include <type_traits>
#include <utility>
#include <string>

#include "utils/status.hpp"
#include "utils/status_adapters.hpp"

namespace vectordb {

inline Status OkStatus() { return Status::OK(); }
inline Status MakeStatus(StatusCode code, const std::string& msg) { return Status(code, msg); }

// Convert common exceptions to Status; customize mappings as needed
inline Status ExceptionToStatus(const std::exception& e, const std::string& operation = "") {
    std::string msg = operation.empty() ? e.what() : (operation + ": " + e.what());
    return Status(DB_UNEXPECTED_ERROR, msg);
}

} // namespace vectordb

// RETURN_IF_ERROR(status_expr);
#define RETURN_IF_ERROR(expr)                     \
    do {                                          \
        auto _status = (expr);                    \
        if (!_status.ok()) {                      \
            return _status;                       \
        }                                         \
    } while (0)

// ASSIGN_OR_RETURN(lhs, status_or_expr);
#define ASSIGN_OR_RETURN(lhs, rexpr)                              \
    auto _status_or_##lhs = (rexpr);                              \
    if (!_status_or_##lhs.ok()) {                                 \
        return _status_or_##lhs.status();                         \
    }                                                             \
    lhs = std::move(_status_or_##lhs.value())
