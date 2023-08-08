#!/bin/bash
##########################################################
## install oatpp

MODULE_NAME="oatpp"
echo $(realpath "$0")
INSTALL_PATH="$(realpath $(dirname "$(realpath "$0")")/../build/dependencies)"
echo "installing to: $INSTALL_PATH"
mkdir -p "${INSTALL_PATH}"

working_dir="$(mktemp -d)"
cd "${working_dir}"
git clone https://github.com/oatpp/$MODULE_NAME
cd $MODULE_NAME
git checkout tags/1.3.0
mkdir build
cd build

N_PROCESSOR=1
PLATFORM="$(uname -s)"
if [[ "$PLATFORM" == "Darwin" ]]; then
    export CC=gcc-13
    export CXX=g++-13
    N_PROCESSOR="$(sysctl -n hw.ncpu)"
elif [[ "$PLATFORM" == "Linux" ]]; then
    N_PROCESSOR="$(nproc)"
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip git cmake build-essential libboost-all-dev
else
    echo "Unknown platform: $PLATFORM"
fi

cmake \
    -DOATPP_BUILD_TESTS=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX:PATH="${INSTALL_PATH}" \
    ..

make -j "${N_PROCESSOR}"
echo "installing OATPP to path ${INSTALL_PATH}"
make install


##########################################################
