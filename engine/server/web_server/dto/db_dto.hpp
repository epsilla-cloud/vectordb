#pragma once

#include <oatpp/core/Types.hpp>
#include <oatpp/core/macro/codegen.hpp>

namespace verctordb {
namespace server {
namespace web {

#include OATPP_CODEGEN_BEGIN(DTO)

class LoadDBDto : public oatpp::DTO {
    DTO_INIT(LoadDBDto, DTO)

    DTO_FIELD(Int32, statusCode);
    DTO_FIELD(String, message);
    // DTO_FIELD(String, path);
    // DTO_FIELD(String, db_name);
};

// class CollectionFieldsDto : public oatpp::data::mapping::type::Object {
//     DTO_INIT(CollectionFieldsDto, Object)

//     DTO_FIELD(String, collection_name);
//     DTO_FIELD(Int64, dimension);
//     DTO_FIELD(Int64, index_file_size);
//     DTO_FIELD(String, metric_type);
//     DTO_FIELD(Int64, count);
//     DTO_FIELD(String, index);
//     DTO_FIELD(String, index_params);
// };

// class CollectionListDto : public OObject {
//     DTO_INIT(CollectionListDto, Object)

//     DTO_FIELD(List<String>::ObjectWrapper, collection_names);
// };

// class CollectionListFieldsDto : public OObject {
//     DTO_INIT(CollectionListFieldsDto, Object)

//     DTO_FIELD(List<CollectionFieldsDto::ObjectWrapper>::ObjectWrapper, collections);
//     DTO_FIELD(Int64, count) = 0L;
// };

#include OATPP_CODEGEN_END(DTO)

}
}
}