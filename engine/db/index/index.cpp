#include "db/index/index.hpp"

#include "db/index/space_cosine.hpp"
#include "db/index/space_ip.hpp"
#include "db/index/space_l2.hpp"
#include "db/vector.hpp"

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
      case engine::meta::MetricType::COSINE:
        return engine::GetCosineDist;
      case engine::meta::MetricType::DOT_PRODUCT:
        return engine::GetInnerProductDist;
      default:
        return engine::GetL2DistSqr;
    }
  }
}

}  // namespace vectordb