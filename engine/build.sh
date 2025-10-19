#!/bin/bash
set -e

bash scripts/install_oatpp_modules.sh

# Create build directory
mkdir -p build
cd build

N_PROCESSOR=1

if [[ "$(uname -s)" == "Darwin" ]]; then
    # On macOS, explicitly use clang/clang++ to avoid g++ issues
    export CC=clang
    export CXX=clang++
    N_PROCESSOR="$(sysctl -n hw.ncpu)"
elif [[ "$(uname -s)" == "Linux" ]]; then
    N_PROCESSOR="$(nproc)"
fi

# Build
if [[ "$1" == "-d" ]]; then
    echo "building in debug mode"
    cmake -DCMAKE_BUILD_TYPE=Debug ..
else
    echo "building in release mode"
    cmake -DCMAKE_BUILD_TYPE=Release ..
fi

cmake --build . --parallel "${N_PROCESSOR}"

if [[ "$TEST" != "" ]]; then
    ctest
fi
