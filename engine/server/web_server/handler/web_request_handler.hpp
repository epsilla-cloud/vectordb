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
    std::shared_ptr<Context>
    GenContextPtr(const std::string& context_str) {
        auto context_ptr = std::make_shared<Context>("dummy_request_id");
        context_ptr->SetTraceContext(trace_context);

        return context_ptr;
    }

 private:
    void
    AddStatusToJson(nlohmann::json& json, int64_t code, const std::string& msg);

    Status
    IsBinaryCollection(const std::string& collection_name, bool& bin);

    Status
    CopyRecordsFromJson(const nlohmann::json& json, engine::VectorsData& vectors, bool bin);

 protected:
    Status
    GetCollectionMetaInfo(const std::string& collection_name, nlohmann::json& json_out);

    Status
    GetCollectionStat(const std::string& collection_name, nlohmann::json& json_out);

    Status
    GetSegmentVectors(const std::string& collection_name, const std::string& segment_name, int64_t page_size,
                      int64_t offset, nlohmann::json& json_out);

    Status
    GetSegmentIds(const std::string& collection_name, const std::string& segment_name, int64_t page_size,
                  int64_t offset, nlohmann::json& json_out);

    Status
    CommandLine(const std::string& cmd, std::string& reply);

    Status
    Cmd(const std::string& cmd, std::string& result_str);

    Status
    PreLoadCollection(const nlohmann::json& json, std::string& result_str);

    Status
    ReleaseCollection(const nlohmann::json& json, std::string& result_str);

    Status
    Flush(const nlohmann::json& json, std::string& result_str);

    Status
    Compact(const nlohmann::json& json, std::string& result_str);

    Status
    GetConfig(std::string& result_str);

    Status
    SetConfig(const nlohmann::json& json, std::string& result_str);

    Status
    Search(const std::string& collection_name, const nlohmann::json& json, std::string& result_str);

    Status
    DeleteByIDs(const std::string& collection_name, const nlohmann::json& json, std::string& result_str);

    Status
    GetVectorsByIDs(const std::string& collection_name, const std::string& partition_tag,
                    const std::vector<int64_t>& ids, nlohmann::json& json_out);

 public:
    WebRequestHandler() {
        context_ptr_ = GenContextPtr("Web Handler");
        request_handler_ = RequestHandler();
    }

 public:
    StatusDto::ObjectWrapper
    GetDevices(DevicesDto::ObjectWrapper& devices);

    StatusDto::ObjectWrapper
    GetAdvancedConfig(AdvancedConfigDto::ObjectWrapper& config);

    StatusDto::ObjectWrapper
    SetAdvancedConfig(const AdvancedConfigDto::ObjectWrapper& config);

    StatusDto::ObjectWrapper
    CreateCollection(const CollectionRequestDto::ObjectWrapper& table_schema);
    StatusDto::ObjectWrapper
    ShowCollections(const OQueryParams& query_params, OString& result);

    StatusDto::ObjectWrapper
    GetCollection(const OString& collection_name, const OQueryParams& query_params, OString& result);

    StatusDto::ObjectWrapper
    DropCollection(const OString& collection_name);

    StatusDto::ObjectWrapper
    CreateIndex(const OString& collection_name, const OString& body);

    StatusDto::ObjectWrapper
    GetIndex(const OString& collection_name, OString& result);

    StatusDto::ObjectWrapper
    DropIndex(const OString& collection_name);

    StatusDto::ObjectWrapper
    CreatePartition(const OString& collection_name, const PartitionRequestDto::ObjectWrapper& param);

    StatusDto::ObjectWrapper
    ShowPartitions(const OString& collection_name, const OQueryParams& query_params,
                   PartitionListDto::ObjectWrapper& partition_list_dto);

    StatusDto::ObjectWrapper
    DropPartition(const OString& collection_name, const OString& body);

    /***********
     *
     * Segment
     */
    StatusDto::ObjectWrapper
    ShowSegments(const OString& collection_name, const OQueryParams& query_params, OString& response);

    StatusDto::ObjectWrapper
    GetSegmentInfo(const OString& collection_name, const OString& segment_name, const OString& info,
                   const OQueryParams& query_params, OString& result);

    /**
     *
     * Vector
     */
    StatusDto::ObjectWrapper
    Insert(const OString& collection_name, const OString& body, VectorIdsDto::ObjectWrapper& ids_dto);

    StatusDto::ObjectWrapper
    GetVector(const OString& collection_name, const OQueryParams& query_params, OString& response);

    StatusDto::ObjectWrapper
    VectorsOp(const OString& collection_name, const OString& payload, OString& response);

    /**
     *
     * System
     */
    StatusDto::ObjectWrapper
    SystemInfo(const OString& cmd, const OQueryParams& query_params, OString& response_str);

    StatusDto::ObjectWrapper
    SystemOp(const OString& op, const OString& body_str, OString& response_str);

 public:
    void
    RegisterRequestHandler(const RequestHandler& handler) {
        request_handler_ = handler;
    }

 private:
    std::shared_ptr<Context> context_ptr_;
    RequestHandler request_handler_;
    std::unordered_map<std::string, engine::meta::hybrid::DataType> field_type_;
};

}
}
}