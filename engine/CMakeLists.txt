cmake_minimum_required(VERSION 3.10)
project(vectordb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# enable symbol flag for every build type
# ref: https://stackoverflow.com/questions/55275743/cmake-build-types-release-debug-etc-compiler-flags
set(CMAKE_CXX_FLAGS_INIT "-g")

SET(CMAKE_COLOR_MAKEFILE ON)
SET(CMAKE_VERBOSE_MAKEFILE ON)

# get build time
MACRO(GET_CURRENT_TIME CURRENT_TIME)
    execute_process(COMMAND "date" +"%Y-%m-%d %H:%M.%S" OUTPUT_VARIABLE ${CURRENT_TIME})
ENDMACRO(GET_CURRENT_TIME)
GET_CURRENT_TIME(BUILD_TIME)
string(REGEX REPLACE "\n" "" BUILD_TIME ${BUILD_TIME})
message(STATUS "Build time = ${BUILD_TIME}")

# Now all includes are relative to the engine directory
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# Specify source files in the root directory
set(LIB_FILES)

# Add source files in the utils directory
file(GLOB UTILS_FILES "utils/*.cpp")
list(APPEND LIB_FILES ${UTILS_FILES})

# Add source files in the server directory
file(GLOB UTILS_FILES "server/*.cpp")
list(APPEND LIB_FILES ${UTILS_FILES})

# Add source files in the db directory
file(GLOB UTILS_FILES "db/*.cpp")
list(APPEND LIB_FILES ${UTILS_FILES})
file(GLOB UTILS_FILES "db/catalog/*.cpp")
list(APPEND LIB_FILES ${UTILS_FILES})
file(GLOB UTILS_FILES "db/execution/*.cpp")
list(APPEND LIB_FILES ${UTILS_FILES})
file(GLOB UTILS_FILES "db/index/*.cpp")
list(APPEND LIB_FILES ${UTILS_FILES})
file(GLOB UTILS_FILES "db/index/nsg/*.cpp")
list(APPEND LIB_FILES ${UTILS_FILES})
file(GLOB UTILS_FILES "db/wal/*.cpp")
list(APPEND LIB_FILES ${UTILS_FILES})

# Add source files in sub dir db_server
file(GLOB_RECURSE DB_SERVER_FILES "server/db_server/*")
list(APPEND LIB_FILES ${DB_SERVER_FILES})

# Add source files in sub dir web_server
file(GLOB_RECURSE WEB_SERVER_FILES "server/web_server/*")
list(APPEND LIB_FILES ${WEB_SERVER_FILES})

# Add source files in logger directory
file(GLOB LOGGER_FILES "logger/*.cpp")
list(APPEND LIB_FILES ${LOGGER_FILES})

# Add source files in the query directory
file(GLOB_RECURSE QUERY_FILES "query/*")
list(APPEND LIB_FILES ${QUERY_FILES})

# used by PyBinding
find_package(PythonLibs REQUIRED)

add_library(oatpp STATIC IMPORTED)
set_target_properties(oatpp PROPERTIES
  IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/build/dependencies/lib/oatpp-1.3.0/liboatpp.a"
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/build/dependencies/include/oatpp-1.3.0/oatpp/"
)

add_library(vectordb_lib ${LIB_FILES})
add_library(vectordb_dylib SHARED ${LIB_FILES})
find_package(Boost REQUIRED COMPONENTS filesystem)
target_include_directories(vectordb_lib PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(vectordb_lib PUBLIC oatpp)
target_link_libraries(vectordb_lib PUBLIC ${Boost_LIBRARIES})
target_include_directories(vectordb_dylib PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(vectordb_dylib PUBLIC ${Boost_LIBRARIES})
target_link_libraries(vectordb_dylib PUBLIC oatpp)

add_executable(vectordb cmd/server/main.cpp)
target_link_libraries(vectordb PUBLIC vectordb_lib)
target_link_libraries(vectordb PUBLIC oatpp)

add_library(pybinding SHARED bindings/python/interface.cpp)

target_include_directories(pybinding PUBLIC ${PYTHON_INCLUDE_PATH})
# the binding library name must match the module name
set_target_properties(pybinding PROPERTIES
    PREFIX ""
    SUFFIX ".so"
    OUTPUT_NAME "epsilla"
)
target_link_libraries(pybinding PUBLIC vectordb_dylib)
target_link_libraries(pybinding PUBLIC ${PYTHON_LIBRARIES})

if(APPLE)
    set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I/opt/homebrew/Cellar/libomp/16.0.6/include")
    set(OpenMP_CXX_LIB_NAMES "omp")
    set(OpenMP_omp_LIBRARY "/opt/homebrew/Cellar/libomp/16.0.6/lib/libomp.dylib")
endif()
find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(vectordb PUBLIC OpenMP::OpenMP_CXX)
    target_link_libraries(vectordb_lib PUBLIC OpenMP::OpenMP_CXX)
    target_link_libraries(vectordb_dylib PUBLIC OpenMP::OpenMP_CXX)
endif()



include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)

enable_testing()

set(DB_TEST_FILES)

# Add source files in the utils directory
file(GLOB ENGINE_DB_TEST_FILES "test/engine/db/*.cpp")
list(APPEND DB_TEST_FILES ${ENGINE_DB_TEST_FILES})

add_executable(
  vector_db_test
  ${DB_TEST_FILES}
)
target_link_libraries(
  vector_db_test
  GTest::gtest_main
  vectordb_lib
)

gtest_discover_tests(vector_db_test)
