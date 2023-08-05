#!/bin/bash

# Create build directory
mkdir -p build
cd build

if [[ "$(uname -s)" == "Darwin" ]]; then 
    export CC=gcc-13
    export CXX=g++-13
fi

# Run cmake and make
cmake ..
make -j $(nproc)
