#include "db/index/index.hpp"

#include "db/index/space_cosine.hpp"
#include "db/index/space_ip.hpp"
#include "db/index/space_l2.hpp"
#include "db/sparse_vector.hpp"

namespace vectordb {

DistFunc GetDistFunc(engine::meta::FieldType fType, engine::meta::MetricType mType) {
  if (fType == engine::meta::FieldType::VECTOR_FLOAT || fType == engine::meta::FieldType::VECTOR_DOUBLE) {
    switch (mType) {
      case engine::meta::MetricType::EUCLIDEAN:
        return L2Sqr;
      case engine::meta::MetricType::COSINE:
        return CosineDistance;
      case engine::meta::MetricType::DOT_PRODUCT:
        return InnerProduct;
      default:
        return L2Sqr;
    }
  } else {
    // sparse vector
    switch (mType) {
      case engine::meta::MetricType::EUCLIDEAN:
        return engine::GetL2DistSqr;
        break;
      case engine::meta::MetricType::COSINE:
        return engine::GetCosineDist;
        break;
      case engine::meta::MetricType::DOT_PRODUCT:
        return engine::GetCosineDist;
        break;
      default:
        return engine::GetL2DistSqr;
    }
  }
}

}  // namespace vectordb