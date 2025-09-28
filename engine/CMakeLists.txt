cmake_minimum_required(VERSION 3.10)
project(vectordb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# enable symbol flag for every build type
# ref: https://stackoverflow.com/questions/55275743/cmake-build-types-release-debug-etc-compiler-flags
set(CMAKE_CXX_FLAGS_INIT "-g")

# Configure debug logging based on build type
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_definitions(-DDEBUG)
  message(STATUS "Debug logging enabled (build type: Debug)")
else()
  message(STATUS "Debug logging disabled for better performance (build type: ${CMAKE_BUILD_TYPE})")
endif()

# Optional: Allow enabling debug logging in release builds via option
option(ENABLE_DEBUG_LOGGING "Enable debug logging in release builds (impacts performance)" OFF)
if(ENABLE_DEBUG_LOGGING)
  add_definitions(-DDEBUG_LOGGING_ENABLED)
  message(WARNING "Debug logging enabled in release build - this will impact performance!")
endif()

# Suppress Boost deprecated header warnings
add_compile_definitions(BOOST_ALLOW_DEPRECATED_HEADERS)
add_compile_definitions(BOOST_HEADER_DEPRECATED=)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations")

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

# Add source files in the services directory
file(GLOB UTILS_FILES "services/*.cpp")
list(APPEND LIB_FILES ${UTILS_FILES})

# Add source files in the config directory
file(GLOB UTILS_FILES "config/*.cpp")
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
file(GLOB UTILS_FILES "db/index/spatial/*.cpp")
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
# Use the newer FindPython module for better compatibility
find_package(Python3 COMPONENTS Development)

add_library(oatpp STATIC IMPORTED)
set_target_properties(oatpp PROPERTIES
  IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/build/dependencies/lib/oatpp-1.3.0/liboatpp.a"
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/build/dependencies/include/oatpp-1.3.0/oatpp/"
)

add_library(oatpp_curl STATIC IMPORTED)
set_target_properties(oatpp_curl PROPERTIES
  IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/build/dependencies/lib/oatpp-1.3.0/liboatpp-curl.a"
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/build/dependencies/include/oatpp-1.3.0/oatpp-curl/"
)

# Add this line right here
set_property(TARGET oatpp_curl APPEND PROPERTY INTERFACE_LINK_LIBRARIES oatpp)

add_library(oatpp_swagger STATIC IMPORTED)
set_target_properties(oatpp_swagger PROPERTIES
  IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/build/dependencies/lib/oatpp-1.3.0/liboatpp-swagger.a"
  INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/build/dependencies/include/oatpp-1.3.0/oatpp-swagger/"
)

set_property(TARGET oatpp_swagger APPEND PROPERTY INTERFACE_LINK_LIBRARIES oatpp)

add_library(vectordb_lib ${LIB_FILES})
add_library(vectordb_dylib SHARED ${LIB_FILES})
find_package(Boost REQUIRED COMPONENTS filesystem)
target_include_directories(vectordb_lib PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(vectordb_lib PUBLIC oatpp)
target_link_libraries(vectordb_lib PUBLIC oatpp_curl)
target_link_libraries(vectordb_lib PUBLIC oatpp_swagger)
target_link_libraries(vectordb_lib PUBLIC ${Boost_LIBRARIES})
target_include_directories(vectordb_dylib PUBLIC ${Boost_INCLUDE_DIRS})
target_link_libraries(vectordb_dylib PUBLIC ${Boost_LIBRARIES})
target_link_libraries(vectordb_dylib PUBLIC oatpp)
target_link_libraries(vectordb_dylib PUBLIC oatpp_curl)
target_link_libraries(vectordb_dylib PUBLIC oatpp_swagger)

add_executable(vectordb cmd/server/main.cpp)
target_link_libraries(vectordb PUBLIC vectordb_lib)
target_link_libraries(vectordb PUBLIC oatpp)
target_link_libraries(vectordb PUBLIC oatpp_curl)
target_link_libraries(vectordb PUBLIC oatpp_swagger)

find_package(CURL REQUIRED)
if(CURL_FOUND)
  target_include_directories(vectordb_lib PUBLIC ${CURL_INCLUDE_DIRS})
  target_link_libraries(vectordb_lib PUBLIC ${CURL_LIBRARIES})

  target_include_directories(vectordb_dylib PUBLIC ${CURL_INCLUDE_DIRS})
  target_link_libraries(vectordb_dylib PUBLIC ${CURL_LIBRARIES})

  target_include_directories(vectordb PUBLIC ${CURL_INCLUDE_DIRS})
  target_link_libraries(vectordb PUBLIC ${CURL_LIBRARIES})
endif()

# Only build Python binding if Python has shared libraries available
if(Python3_FOUND)
  # Check if Python was built with shared libraries
  execute_process(
    COMMAND ${Python3_EXECUTABLE} -c "import sysconfig; import sys; sys.exit(0 if sysconfig.get_config_var('Py_ENABLE_SHARED') else 1)"
    RESULT_VARIABLE PYTHON_HAS_SHARED_LIB
  )
  
  if(PYTHON_HAS_SHARED_LIB EQUAL 0)
    add_library(pybinding SHARED bindings/python/interface.cpp)
    
    target_include_directories(pybinding PUBLIC ${Python3_INCLUDE_DIRS})
    # the binding library name must match the module name
    set_target_properties(pybinding PROPERTIES
       PREFIX ""
       SUFFIX ".so"
       OUTPUT_NAME "epsilla"
    )
    target_link_libraries(pybinding PUBLIC vectordb_dylib)
    target_link_libraries(pybinding PUBLIC Python3::Python)
  else()
    message(WARNING "Python was built without shared libraries. Skipping Python binding. The core C++ libraries are still built successfully.")
  endif()
endif()

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
file(COPY test/engine/db/testdata DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/engine/db)

target_link_libraries(
  vector_db_test
  GTest::gtest_main
  vectordb_lib
)

gtest_discover_tests(vector_db_test)

# Coverage support
option(ENABLE_COVERAGE "Enable coverage reporting" OFF)
if(ENABLE_COVERAGE)
  if(CMAKE_BUILD_TYPE MATCHES "Debug")
    message(STATUS "Building with coverage support")
    
    # Add coverage flags
    set(COVERAGE_FLAGS "--coverage -g -O0 -fprofile-arcs -ftest-coverage")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COVERAGE_FLAGS}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COVERAGE_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage")
    
    # Find lcov and genhtml for coverage reporting
    find_program(LCOV_PATH lcov)
    find_program(GENHTML_PATH genhtml)
    
    if(LCOV_PATH AND GENHTML_PATH)
      message(STATUS "Found lcov and genhtml: ${LCOV_PATH}, ${GENHTML_PATH}")
      
      # Add custom target for coverage
      add_custom_target(coverage
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/coverage
        COMMAND ${LCOV_PATH} --directory . --zerocounters
        COMMAND ${CMAKE_CTEST_COMMAND} --verbose
        COMMAND ${LCOV_PATH} --directory . --capture --output-file ${CMAKE_BINARY_DIR}/coverage/coverage.info
        COMMAND ${LCOV_PATH} --remove ${CMAKE_BINARY_DIR}/coverage/coverage.info '/usr/*' '*/build/_deps/*' '*/test/*' --output-file ${CMAKE_BINARY_DIR}/coverage/coverage_filtered.info
        COMMAND ${GENHTML_PATH} ${CMAKE_BINARY_DIR}/coverage/coverage_filtered.info --output-directory ${CMAKE_BINARY_DIR}/coverage/html
        COMMAND ${CMAKE_COMMAND} -E echo "Coverage report generated in ${CMAKE_BINARY_DIR}/coverage/html/index.html"
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating code coverage report"
      )
      
      # Ensure tests are built before coverage
      add_dependencies(coverage vector_db_test)
      
    else()
      message(WARNING "lcov or genhtml not found, coverage target will not be available")
    endif()
    
  else()
    message(WARNING "Coverage requires Debug build type. Current: ${CMAKE_BUILD_TYPE}")
  endif()
endif()

# Code formatting and linting targets
find_program(CLANG_FORMAT_PATH clang-format)
if(CLANG_FORMAT_PATH)
  message(STATUS "Found clang-format: ${CLANG_FORMAT_PATH}")
  
  # Get all source files for formatting
  file(GLOB_RECURSE ALL_SOURCE_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/db/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/db/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/utils/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/utils/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/config/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/config/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/services/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/services/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/server/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/server/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logger/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logger/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/query/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/query/*.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/test/engine/**/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/test/engine/**/*.hpp
  )
  
  add_custom_target(format
    COMMAND ${CLANG_FORMAT_PATH} -i ${ALL_SOURCE_FILES}
    COMMENT "Running clang-format on source files"
  )
  
  add_custom_target(format-check
    COMMAND ${CLANG_FORMAT_PATH} --dry-run --Werror ${ALL_SOURCE_FILES}
    COMMENT "Checking code formatting"
  )
endif()

find_program(CLANG_TIDY_PATH clang-tidy)
if(CLANG_TIDY_PATH)
  message(STATUS "Found clang-tidy: ${CLANG_TIDY_PATH}")
  
  add_custom_target(tidy
    COMMAND ${CLANG_TIDY_PATH} ${ALL_SOURCE_FILES} -p=${CMAKE_BINARY_DIR}
    COMMENT "Running clang-tidy on source files"
    DEPENDS vector_db_test
  )
endif()
