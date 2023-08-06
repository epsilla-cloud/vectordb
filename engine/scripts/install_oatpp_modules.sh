#!/bin/sh

sudo apt-get update
sudo apt-get install -y python3 python3-pip git cmake build-essential libboost-all-dev

working_dir="$(mktemp -d)"
cd "${working_dir}"

##########################################################
## install oatpp

MODULE_NAME="oatpp"
INSTALL_PATH="${OATPP_INSTALL_PATH:-'/usr/local/'}"

git clone --depth=1 https://github.com/oatpp/$MODULE_NAME

cd $MODULE_NAME
mkdir build
cd build

cmake \
    -DOATPP_BUILD_TESTS=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX:PATH=${INSTALL_PATH} \
    ..

echo "installing OATPP to path ${INSTALL_PATH}"
make install -j $(nproc)


##########################################################

cd "${working_dir}"/..
rm -rf "${working_dir}"