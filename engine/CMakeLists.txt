cmake_minimum_required(VERSION 3.10)
project(vectordb)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")

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

add_executable(vectordb ${SOURCE_FILES})

# # Specify all your source files
# set(SOURCE_FILES
#     main.cpp
#     utils/status.cpp
# )

# add_executable(vectordb ${SOURCE_FILES})
