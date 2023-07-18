#!/bin/bash

# Create build directory
mkdir -p build
cd build

# Run cmake and make
export CC=gcc-13
export CXX=g++-13
cmake ..
make
