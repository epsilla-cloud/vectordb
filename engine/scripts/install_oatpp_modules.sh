#!/bin/bash
##########################################################
## install oatpp

install_module() {
    local ORG_NAME=$1
    local MODULE_NAME=$2
    local TAG=$3
    echo "Installing module: ${ORG_NAME}/${MODULE_NAME}"

    # Create a temporary working directory
    local working_dir="$(mktemp -d)"
    cd "${working_dir}"

    # Clone the repository
    git clone https://github.com/${ORG_NAME}/${MODULE_NAME}
    cd ${MODULE_NAME}

    # Checkout the desired tag
    git checkout tags/${TAG}

    # Build and install
    mkdir build
    cd build
    cmake \
        -DOATPP_BUILD_TESTS=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX:PATH="${INSTALL_PATH}" \
        ..
    make -j "${N_PROCESSOR}"
    echo "Installing ${ORG_NAME}/${MODULE_NAME} to path ${INSTALL_PATH}"
    make install

    # Clean up
    cd "${CURRENT_DIR}"
    rm -rf "${working_dir}"
}

MODULE_NAME="oatpp"
CURRENT_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
INSTALL_PATH="$CURRENT_DIR/../build/dependencies"
echo "Installing to: $INSTALL_PATH"
mkdir -p "${INSTALL_PATH}"

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

# Install oatpp
install_module "oatpp" "oatpp" "1.3.0"

# Install oatpp-curl
install_module "epsilla-cloud" "oatpp-curl" "epsilla"

# Install oatpp-swagger
install_module "oatpp" "oatpp-swagger" "1.3.0"

##########################################################
