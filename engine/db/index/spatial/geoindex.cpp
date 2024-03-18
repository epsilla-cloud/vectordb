// GeospatialIndex.cpp
#include "geoindex.hpp"

namespace vectordb {
namespace engine {
namespace index {

GeospatialIndex::GeospatialIndex() {
    // Constructor implementation (if needed)
}

GeospatialIndex::~GeospatialIndex() {
    // Destructor implementation (if needed)
}

void GeospatialIndex::insertPoint(double lat, double lon, int64_t id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  rtree.insert(std::make_pair(point_t(lat, lon), id));
}

void GeospatialIndex::deletePoint(double lat, double lon, int64_t id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  // Construct the point-ID pair to be deleted
  value_t valueToDelete = std::make_pair(point_t(lat, lon), id);
  // Remove the point-ID pair from the R-tree
  rtree.remove(valueToDelete);
}

void GeospatialIndex::searchWithinRadius(double lat, double lon, double radius_km, std::vector<value_t>& results) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  rtree.query(bgi::satisfies([&](const value_t& v) {
    return distance(point_t(lat, lon), v.first) <= radius_km;
  }), std::back_inserter(results));
  // Optionally, process results here or in the caller function
}

double GeospatialIndex::degToRad(double deg) {
  return deg * (M_PI / 180.0);
}

double GeospatialIndex::distance(const point_t& p1, const point_t& p2) {
  // Earth's radius in kilometers
  double earth_radius = 6371.0;

  double lat1_rad = degToRad(bg::get<0>(p1));
  double lon1_rad = degToRad(bg::get<1>(p1));
  double lat2_rad = degToRad(bg::get<0>(p2));
  double lon2_rad = degToRad(bg::get<1>(p2));

  double dlat = lat2_rad - lat1_rad;
  double dlon = lon2_rad - lon1_rad;

  double a = std::sin(dlat/2) * std::sin(dlat/2) +
            std::cos(lat1_rad) * std::cos(lat2_rad) *
            std::sin(dlon/2) * std::sin(dlon/2);
  double c = 2 * std::atan2(std::sqrt(a), std::sqrt(1-a));

  return earth_radius * c;
}

}
}
}
