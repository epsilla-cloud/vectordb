#pragma once

#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>

// #include "db/catalog/meta.hpp"

namespace vectordb {
namespace server {
namespace web {

#include OATPP_CODEGEN_BEGIN(DTO)

class StatusDto: public oatpp::DTO {

    DTO_INIT(StatusDto, DTO)

    DTO_FIELD(Int32, statusCode);
    DTO_FIELD(String, message);
};

// class SchemaInfoDto: public oatpp::DTO {
//     DTO_INIT(SchemaInfoDto, DTO)

//     DTO_FIELD(Int32, statusCode);
//     DTO_FIELD(String, message);
//     DTO_FIELD(Object<vectordb::engine::meta::TableSchema>, schema);
// };

#include OATPP_CODEGEN_END(DTO)

}
}
}
