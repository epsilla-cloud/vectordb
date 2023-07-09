cmake_minimum_required(VERSION 3.10)
project(vectordb)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")

# add_subdirectory(module1)
# add_subdirectory(module2)

add_executable(vectordb main.cpp)
# target_link_libraries(vectordb module1 module2)
