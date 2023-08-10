#!/bin/bash

# Create build directory
mkdir -p build
cd build

N_PROCESSOR=1

if [[ "$(uname -s)" == "Darwin" ]]; then
    export CC=gcc-13
    export CXX=g++-13
    N_PROCESSOR="$(sysctl -n hw.ncpu)"
elif [[ "$(uname -s)" == "Linux" ]]; then
    N_PROCESSOR="$(nproc)"
fi

# Run cmake and make
cmake ..
make -j "${N_PROCESSOR}"
