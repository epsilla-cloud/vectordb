#pragma once

#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>

namespace vectordb {
namespace server {
namespace web {

#include OATPP_CODEGEN_BEGIN(DTO)

class EDto: public oatpp::DTO {

    DTO_INIT(EDto, DTO)

    DTO_FIELD(Int32, statusCode);
    DTO_FIELD(String, message);
};

#include OATPP_CODEGEN_END(DTO)

}
}
}