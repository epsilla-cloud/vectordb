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
        -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
        -DCMAKE_INSTALL_PREFIX:PATH="${INSTALL_PATH}" \
        ${CMAKE_EXTRA_FLAGS} \
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

# Load version configuration
VERSION_CONFIG="${CURRENT_DIR}/versions.conf"
if [ -f "$VERSION_CONFIG" ]; then
    echo "Loading versions from: $VERSION_CONFIG"
    source "$VERSION_CONFIG"
else
    echo "Warning: Version config not found, using defaults"
    OATPP_VERSION="1.3.0"
fi

N_PROCESSOR=1
PLATFORM="$(uname -s)"
CMAKE_EXTRA_FLAGS=""

# On macOS, try to upgrade libomp to ensure compatibility with Apple Clang 17+
# libomp 21.1.3+ is required for ___kmpc_dispatch_deinit symbol
if [[ "$PLATFORM" == "Darwin" ]]; then
    echo "Checking libomp version on macOS..."
    if command -v brew &> /dev/null; then
        echo "Attempting to upgrade libomp (required for OpenMP compatibility)..."
        brew upgrade libomp 2>/dev/null || echo "libomp upgrade failed or not needed, continuing..."
    else
        echo "Warning: Homebrew not found, skipping libomp upgrade"
    fi
fi

if [[ "$PLATFORM" == "Darwin" ]]; then
    # Let CMake auto-detect the compiler on macOS
    # Use xcrun to find the actual SDK path
    MACOS_SDK="$(xcrun --show-sdk-path 2>/dev/null)"
    if [ -z "$MACOS_SDK" ] || [ ! -d "$MACOS_SDK" ]; then
        # Fallback to checking common SDK locations
        for sdk in /Library/Developer/CommandLineTools/SDKs/MacOSX*.sdk; do
            if [ -d "$sdk" ] && [ ! -L "$sdk" ]; then
                MACOS_SDK="$sdk"
                break
            fi
        done
    fi
    
    if [ -n "$MACOS_SDK" ] && [ -d "$MACOS_SDK" ]; then
        echo "Using macOS SDK: $MACOS_SDK"
        CMAKE_EXTRA_FLAGS="-DCMAKE_OSX_SYSROOT=${MACOS_SDK}"
        # Also ensure the compiler can find headers
        export CPLUS_INCLUDE_PATH="${MACOS_SDK}/usr/include/c++/v1:${CPLUS_INCLUDE_PATH}"
        export CPATH="${MACOS_SDK}/usr/include:${CPATH}"
    else
        echo "Warning: Could not find macOS SDK"
    fi
    N_PROCESSOR="$(sysctl -n hw.ncpu)"
elif [[ "$PLATFORM" == "Linux" ]]; then
    N_PROCESSOR="$(nproc)"
    sudo apt-get update
    sudo apt-get install -y python3 python3-pip git cmake build-essential libboost-all-dev
else
    echo "Unknown platform: $PLATFORM"
fi

# Install oatpp (using version from config)
install_module "oatpp" "oatpp" "${OATPP_VERSION}"

# Install oatpp-curl
install_module "epsilla-cloud" "oatpp-curl" "epsilla"

# Install oatpp-swagger (using version from config)
install_module "oatpp" "oatpp-swagger" "${OATPP_VERSION}"

##########################################################
