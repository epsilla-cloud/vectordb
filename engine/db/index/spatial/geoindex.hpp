#pragma once

#include <mutex>
#include <shared_mutex>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/index/rtree.hpp>
#include <cmath>
#include <vector>
#include <iostream>

namespace vectordb {
namespace engine {
namespace index {

namespace bg = boost::geometry;
namespace bgi = boost::geometry::index;

class GeospatialIndex {
public:
  typedef bg::model::point<double, 2, bg::cs::geographic<bg::degree>> point_t;
  typedef bg::model::box<point_t> box_t;
  typedef std::pair<point_t, int64_t> value_t;
  typedef bgi::rtree<value_t, bgi::quadratic<16>> rtree_t;

  GeospatialIndex(); // Constructor declaration
  ~GeospatialIndex(); // Destructor declaration

  void insertPoint(double lat, double lon, int64_t id);
  void deletePoint(double lat, double lon, int64_t id);
  void searchWithinRadius(double lat, double lon, double radius_km, std::vector<value_t>& results) const;

private:
  mutable std::shared_mutex mutex_;
  rtree_t rtree;
  static double degToRad(double deg);
  static double distance(const point_t& p1, const point_t& p2);
};

}
}
}

  // vectordb::engine::index::GeospatialIndex index;
  // Test case 1: search in California
  // std::vector<std::pair<double, double>> points = {
  //     {36.7783, -119.4179}, // California
  //     {34.0522, -118.2437}, // Los Angeles
  //     {37.7749, -122.4194}, // San Francisco
  //     {36.6002, -121.8947}, // Monterey
  //     {38.5816, -121.4944}, // Sacramento
  //     {32.7157, -117.1611}, // San Diego
  //     {33.9533, -117.3962}, // Riverside
  //     {35.3733, -119.0187}, // Bakersfield
  //     {36.1627, -115.1391}, // Near Las Vegas, but let's pretend it's California for the demo
  //     {40.5865, -122.3917}  // Redding
  // };
  // for (std::size_t i = 0; i < points.size(); ++i) {
  //     index.insertPoint(points[i].first, points[i].second, i);
  // }
  // // Query points within 500 km radius of Los Angeles
  // std::vector<vectordb::engine::index::GeospatialIndex::value_t> results;
  // index.searchWithinRadius(34.0522, -118.2437, 150, results);

  // std::cout << "Points within 500 km radius of Los Angeles: " << results.size() << std::endl;
  // for (const auto& result : results) {
  //     std::cout << "Point: (" << vectordb::engine::index::bg::get<0>(result.first) << ", " << vectordb::engine::index::bg::get<1>(result.first) << "), ID: " << result.second << std::endl;
  // }

  // // Delete 5 points
  // for (std::size_t i = 0; i < 5; ++i) {
  //     index.deletePoint(points[i].first, points[i].second, i);
  // }

  // // Query again
  // results.clear();
  // index.searchWithinRadius(34.0522, -118.2437, 150, results);

  // std::cout << "Points within 500 km radius of Los Angeles after deletion: " << results.size() << std::endl;
  // for (const auto& result : results) {
  //     std::cout << "Point: (" << vectordb::engine::index::bg::get<0>(result.first) << ", " << vectordb::engine::index::bg::get<1>(result.first) << "), ID: " << result.second << std::endl;
  // }

  // Test case 2:
  // Insert 2 points, one at Alaska and one at Russia Far East
  // Assumption coordinates: Alaska（63.3333°N, -179.9999°E），Russia Far East（66.0000°N, 179.9999°E）
  // index.insertPoint(63.3333, -179.9999, 1); // Alaska
  // index.insertPoint(66.0000, 179.9999, 2); // Russia Far East

  // // Query points within 500 km radius of the International Date Line
  // std::vector<vectordb::engine::index::GeospatialIndex::value_t> results;
  // index.searchWithinRadius(65.0, -180.0, 500, results);

  // std::cout << "Near the International Date Line, points found: " << results.size() << std::endl;
  // for (const auto& result : results) {
  //     std::cout << "Point: (" << vectordb::engine::index::bg::get<0>(result.first) << ", " << vectordb::engine::index::bg::get<1>(result.first) << "), ID: " << result.second << std::endl;
  // }
  // // Delete a point (e.g., the point in western Alaska)
  // index.deletePoint(63.3333, -179.9999, 1);

  // // Query again
  // results.clear();
  // index.searchWithinRadius(65.0, -180.0, 500, results);

  // std::cout << "After deletion, points found: " << results.size() << std::endl;
  // for (const auto& result : results) {
  //     std::cout << "Point: (" << vectordb::engine::index::bg::get<0>(result.first) << ", " << vectordb::engine::index::bg::get<1>(result.first) << "), ID: " << result.second << std::endl;
  // }

