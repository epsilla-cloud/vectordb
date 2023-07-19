#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <oatpp/core/data/mapping/type/Object.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/web/server/api/ApiController.hpp>

#include "utils/json.hpp"
#include "utils/status.hpp"

namespace vectordb {
namespace server {
namespace web {

#define RETURN_STATUS_DTO(STATUS_CODE, MESSAGE)      \
    do {                                             \
        auto status_dto = StatusDto::createShared(); \
        status_dto->code = (STATUS_CODE);            \
        status_dto->message = (MESSAGE);             \
        return status_dto;                           \
    } while (false);

#define ASSIGN_RETURN_STATUS_DTO(STATUS)                    \
    do {                                                    \
        int code;                                           \
        if (0 != (STATUS).code()) {                         \
            code = WebErrorMap((STATUS).code());            \
        } else {                                            \
            code = 0;                                       \
        }                                                   \
        RETURN_STATUS_DTO(code, (STATUS).message().c_str()) \
    } while (false);

StatusCode
WebErrorMap(ErrorCode code);

class WebRequestHandler {
 private:
    // std::shared_ptr<Context>
    // GenContextPtr(const std::string& context_str) {
    //     auto context_ptr = std::make_shared<Context>("dummy_request_id");
    //     context_ptr->SetTraceContext(trace_context);

    //     return context_ptr;
    // }

 private:
    // void
    // AddStatusToJson(vectordb::Json& json, int64_t code, const std::string& msg);

    // Status
    // IsBinaryCollection(const std::string& collection_name, bool& bin);

    // Status
    // CopyRecordsFromJson(const vectordb::Json& json, engine::VectorsData& vectors, bool bin);

 protected:
    // Status
    // CommandLine(const std::string& cmd, std::string& reply);

    // Status
    // Cmd(const std::string& cmd, std::string& result_str);

    // Status
    // PreLoadCollection(const vectordb::Json& json, std::string& result_str);

    // Status
    // ReleaseCollection(const vectordb::Json& json, std::string& result_str);

    // Status
    // Flush(const vectordb::Json& json, std::string& result_str);

    // Status
    // Compact(const vectordb::Json& json, std::string& result_str);

    // Status
    // GetConfig(std::string& result_str);

    // Status
    // SetConfig(const vectordb::Json& json, std::string& result_str);

    // Status
    // Search(const std::string& collection_name, const vectordb::Json& json, std::string& result_str);

    // Status
    // DeleteByIDs(const std::string& collection_name, const vectordb::Json& json, std::string& result_str);

    // Status
    // GetVectorsByIDs(const std::string& collection_name, const std::string& partition_tag,
    //                 const std::vector<int64_t>& ids, vectordb::Json& json_out);

 public:
    // WebRequestHandler() {
    //     context_ptr_ = GenContextPtr("Web Handler");
    //     request_handler_ = RequestHandler();
    // }

 public:

 public:
    // void
    // RegisterRequestHandler(const RequestHandler& handler) {
    //     request_handler_ = handler;
    // }

 private:
    // std::shared_ptr<Context> context_ptr_;
    // RequestHandler request_handler_;
    // std::unordered_map<std::string, engine::meta::hybrid::DataType> field_type_;
};

}
}
}