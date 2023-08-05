cmake_minimum_required(VERSION 3.10)
project(vectordb)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g")


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

# used by PyBinding
find_package(PythonLibs REQUIRED)
# add third-party dependencies
# oatpp
find_package(oatpp 1.3.0 REQUIRED)


add_library(vectordb_lib ${LIB_FILES})
add_library(vectordb_dylib SHARED ${LIB_FILES})
find_package(Boost REQUIRED COMPONENTS filesystem)
target_include_directories(vectordb_lib PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(vectordb_lib PUBLIC oatpp::oatpp)
target_link_libraries(vectordb_lib PUBLIC ${Boost_LIBRARIES})
target_include_directories(vectordb_dylib PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(vectordb_dylib PUBLIC oatpp::oatpp)
target_link_libraries(vectordb_dylib PUBLIC ${Boost_LIBRARIES})

add_executable(vectordb cmd/main.cpp)
target_link_libraries(vectordb PUBLIC vectordb_lib)



add_library(pybinding SHARED bindings/python/interface.cpp)
target_include_directories(pybinding PUBLIC ${PYTHON_INCLUDE_PATH})
# the binding library name must match the module name
set_target_properties(pybinding PROPERTIES 
    PREFIX ""
    SUFFIX ".so"
    OUTPUT_NAME "epsilla"
)
target_link_libraries(pybinding PUBLIC vectordb_dylib)




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
