#include "db/table_segment.hpp"

namespace vectordb {
namespace engine {
  // The number of bytes of a field value is stored in a continuous memory block.
  constexpr size_t FieldTypeSize(meta::FieldType type) {
  switch (type) {
    case meta::FieldType::INT1:
      return 1;
    case meta::FieldType::INT2:
      return 2;
    case meta::FieldType::INT4:
      return 4;
    case meta::FieldType::INT8:
      return 8;
    case meta::FieldType::FLOAT:
      return 4;
    case meta::FieldType::DOUBLE:
      return 8;
    case meta::FieldType::BOOL:
      return 1;
    case meta::FieldType::STRING:
    case meta::FieldType::JSON:
      // String or json attribute requires a 8-byte pointer to the string table.
      return 8;
    case meta::FieldType::VECTOR_FLOAT:
    case meta::FieldType::VECTOR_DOUBLE:
      // For these types, we can't determine the size without additional information
      // like the length of the string or the dimension of the vector. You might want
      // to handle these cases differently.
      return 0;
    case meta::FieldType::UNKNOWN:
    default:
      // Unknown type
      return 0;
  }
}
}
}