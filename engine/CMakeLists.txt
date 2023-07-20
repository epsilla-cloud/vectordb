cmake_minimum_required(VERSION 3.10)
project(vectordb)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g")

# Find Boost
find_package(Boost REQUIRED)

# Include Boost headers
include_directories(${Boost_INCLUDE_DIRS})

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
set(SOURCE_FILES main.cpp)

# Add source files in the utils directory
file(GLOB UTILS_FILES "utils/*.cpp")
list(APPEND SOURCE_FILES ${UTILS_FILES})

# Add source files in the server directory
file(GLOB UTILS_FILES "server/*.cpp")
list(APPEND SOURCE_FILES ${UTILS_FILES})

# Add source files in the db directory
file(GLOB UTILS_FILES "db/*.cpp")
list(APPEND SOURCE_FILES ${UTILS_FILES})
file(GLOB UTILS_FILES "db/catalog/*.cpp")
list(APPEND SOURCE_FILES ${UTILS_FILES})
file(GLOB UTILS_FILES "db/execution/*.cpp")
list(APPEND SOURCE_FILES ${UTILS_FILES})
file(GLOB UTILS_FILES "db/index/*.cpp")
list(APPEND SOURCE_FILES ${UTILS_FILES})
file(GLOB UTILS_FILES "db/index/nsg/*.cpp")
list(APPEND SOURCE_FILES ${UTILS_FILES})

# Add source files in sub dir db_server
file(GLOB_RECURSE DB_SERVER_FILES "server/db_server/*")
list(APPEND SOURCE_FILES ${DB_SERVER_FILES})

# Add source files in sub dir web_server
file(GLOB_RECURSE WEB_SERVER_FILES "server/web_server/*")
list(APPEND SOURCE_FILES ${WEB_SERVER_FILES})



add_executable(vectordb ${SOURCE_FILES})

# add third-party dependencies
# oatpp
find_package(oatpp 1.3.0 REQUIRED)
target_link_libraries(vectordb PUBLIC oatpp::oatpp)

# # Specify all your source files
# set(SOURCE_FILES
#     main.cpp
#     utils/status.cpp
# )

# add_executable(vectordb ${SOURCE_FILES})

# Link Boost libraries
target_link_libraries(vectordb ${Boost_LIBRARIES})

if(APPLE)
    set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I/opt/homebrew/Cellar/libomp/16.0.6/include")
    set(OpenMP_CXX_LIB_NAMES "omp")
    set(OpenMP_omp_LIBRARY "/opt/homebrew/Cellar/libomp/16.0.6/lib/libomp.dylib")
endif()
find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(vectordb PUBLIC OpenMP::OpenMP_CXX)
endif()
