#!/bin/bash

# Create build directory
mkdir -p build
cd build

# Run cmake and make
cmake ..
make
