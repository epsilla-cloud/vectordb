#pragma once

#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/data/mapping/type/Any.hpp>

#include "db/catalog/meta.hpp"
#include "utils/json.hpp"

namespace vectordb {
namespace server {
namespace web {

#include OATPP_CODEGEN_BEGIN(DTO)

class StatusDto: public oatpp::DTO {

    DTO_INIT(StatusDto, DTO)

    DTO_FIELD(Int32, statusCode);
    DTO_FIELD(String, message);
};

class SchemaInfoDto: public oatpp::DTO {
    DTO_INIT(SchemaInfoDto, DTO)

    DTO_FIELD(Int32, statusCode);
    DTO_FIELD(String, message);
    // DTO_FIELD(Object<vectordb::engine::meta::TableSchema>, result);
};

class TableListDto : public oatpp::DTO {
    DTO_INIT(TableListDto, DTO)

    DTO_FIELD(Int32, statusCode);
    DTO_FIELD(String, message);
    DTO_FIELD(List<String>, result);
};

class DeleteRecordsReqDto : public oatpp::DTO {
    DTO_INIT(DeleteRecordsReqDto, DTO)

    DTO_FIELD_INFO(table) {
        info->required = true;
    }

    DTO_FIELD(String, table);
    DTO_FIELD(oatpp::List<oatpp::String>, ids);
};

class SearchRespDto : public oatpp::DTO {
    DTO_INIT(SearchRespDto, DTO);

    DTO_FIELD(Int32, statusCode);
    DTO_FIELD(String, message);
    DTO_FIELD(Any::ObjectWrapper, result);
};

#include OATPP_CODEGEN_END(DTO)

}
}
}
