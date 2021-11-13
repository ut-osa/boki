#!/bin/bash
set -e

SCRIPT_PATH=$(readlink -f $0)
BASE_DIR=$(dirname ${SCRIPT_PATH})

if [[ -z "${DEPS_INSTALL_PATH}" ]]; then
  DEPS_INSTALL_PATH="${BASE_DIR}/deps/out"
fi

USE_GCC=1
USE_CLANG=0
DEBUG_BUILD="no"

while [ ! $# -eq 0 ]
do
  case "$1" in
    --debug)
      DEBUG_BUILD="yes"
      ;;
    --use-clang)
      USE_GCC=0
      USE_CLANG=1
  esac
  shift
done

if [[ ${USE_GCC} == 1 ]]; then
  export CC=gcc
  export CXX=g++
fi

if [[ ${USE_CLANG} == 1 ]]; then
  export CC=clang
  export CXX=clang++
fi

COMPILE_FLAGS="-fPIE -march=haswell -D_GNU_SOURCE"
if [ ${DEBUG_BUILD} == "yes" ]; then
  COMPILE_FLAGS="${COMPILE_FLAGS} -DDEBUG -g -Og"
  CMAKE_BUILD_TYPE="Debug"
else
  COMPILE_FLAGS="${COMPILE_FLAGS} -DNDEBUG -O3"
  COMPILE_FLAGS="${COMPILE_FLAGS} -fdata-sections -ffunction-sections"  # used for -Wl,--gc-sections
  CMAKE_BUILD_TYPE="Release"
fi

export CFLAGS="${CFLAGS} ${COMPILE_FLAGS}"
export CXXFLAGS="${CXXFLAGS} ${COMPILE_FLAGS} -std=c++17"

rm -rf ${DEPS_INSTALL_PATH}
mkdir -p ${DEPS_INSTALL_PATH}

export PKG_CONFIG_PATH="${DEPS_INSTALL_PATH}/lib/pkgconfig"

if [[ ! -z "${OVERLAY_PATH}" ]]; then
  BASE_DIR="${OVERLAY_PATH}"
fi

# Build abseil-cpp
cd "${BASE_DIR}/deps/abseil-cpp" && \
  rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_INSTALL_PREFIX="${DEPS_INSTALL_PATH}" -DCMAKE_INSTALL_LIBDIR=lib .. && \
  make -j$(nproc) install && \
  rm -rf "${BASE_DIR}/deps/abseil-cpp/build"

# Build jemalloc
cd "${BASE_DIR}/deps/jemalloc" && \
  ./autogen.sh && \
  ./configure --prefix="${DEPS_INSTALL_PATH}" --disable-shared \
              --enable-prof --enable-stats && \
  make clean && make -j$(nproc) dist && make install && make clean

# Build zstd
cd "${BASE_DIR}/deps/zstd" && \
  rm -rf builddir && mkdir -p builddir && cd builddir && \
  cmake -DCMAKE_BUILD_TYPE=Release -DZSTD_BUILD_TESTS=OFF \
        -DZSTD_BUILD_STATIC=ON -DZSTD_BUILD_SHARED=OFF \
        -DCMAKE_INSTALL_PREFIX="${DEPS_INSTALL_PATH}" -DCMAKE_INSTALL_LIBDIR=lib ../build/cmake && \
  make -j$(nproc) install && \
  rm -rf "${BASE_DIR}/deps/zstd/builddir"

# Build http-parser
cd "${BASE_DIR}/deps/http-parser" && \
  make clean && make package && \
  install -D "${BASE_DIR}/deps/http-parser/http_parser.h" "${DEPS_INSTALL_PATH}/include/http_parser.h" && \
  install -D "${BASE_DIR}/deps/http-parser/libhttp_parser.a" "${DEPS_INSTALL_PATH}/lib/libhttp_parser.a" && \
  make clean

# Build lmdb
cd "${BASE_DIR}/deps/lmdb/libraries/liblmdb" && \
  make clean && \
  make install prefix="${DEPS_INSTALL_PATH}" CC=${CC} CFLAGS="${CFLAGS} -pthread" && \
  make clean

# Build liburing
cd "${BASE_DIR}/deps/liburing" && \
  make clean && \
  ./configure --prefix="${DEPS_INSTALL_PATH}" && \
  make install && make clean

# Build protobuf
cd "${BASE_DIR}/deps/protobuf" && \
  ./autogen.sh && \
  ./configure --prefix="${DEPS_INSTALL_PATH}" && \
  make clean && make -j$(nproc) install && make clean

# Build libuv
cd "${BASE_DIR}/deps/libuv" && \
  rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DLIBUV_BUILD_TESTS=OFF \
        -DCMAKE_INSTALL_PREFIX="${DEPS_INSTALL_PATH}" -DCMAKE_INSTALL_LIBDIR=lib .. && \
  make -j$(nproc) install && \
  rm -rf "${BASE_DIR}/deps/libuv/build"

# Build nghttp2
cd "${BASE_DIR}/deps/nghttp2" && \
  rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DENABLE_LIB_ONLY=ON \
        -DENABLE_STATIC_LIB=ON -DENABLE_SHARED_LIB=OFF \
        -DENABLE_ASIO_LIB=OFF -DWITH_JEMALLOC=ON \
        -DCMAKE_INSTALL_PREFIX="${DEPS_INSTALL_PATH}" -DCMAKE_INSTALL_LIBDIR=lib .. && \
  make -j$(nproc) install && \
  rm -rf "${BASE_DIR}/deps/nghttp2/build"

# Build zookeeper-client-c
CFLAGS_BAK=${CFLAGS}
if [[ ${USE_GCC} == 1 ]]; then
  export CFLAGS="${CFLAGS} -Wno-unused-but-set-variable"  # Fix gcc compilation
fi
cd "${BASE_DIR}/deps/zookeeper-client-c" && \
  autoreconf -if && \
  ./configure --prefix="${DEPS_INSTALL_PATH}" --disable-shared \
              --enable-debug=${DEBUG_BUILD} \
              --without-syncapi --without-cppunit --without-openssl && \
  make -j$(nproc) install && make clean
export CFLAGS=${CFLAGS_BAK}

# Build tkrzw
CXXFLAGS_BAK=${CXXFLAGS}
export CXXFLAGS="${CXXFLAGS} -I${DEPS_INSTALL_PATH}/include -L${DEPS_INSTALL_PATH}/lib"
cd "${BASE_DIR}/deps/tkrzw" && \
  ./configure --prefix="${DEPS_INSTALL_PATH}" --disable-shared \
              --disable-zlib --enable-zstd --disable-lz4 --disable-lzma \
              --enable-debug=${DEBUG_BUILD} && \
  make -j$(nproc) && make install && make clean
export CXXFLAGS=${CXXFLAGS_BAK}

# Build rocksdb
cd "${BASE_DIR}/deps/rocksdb" && \
  rm -rf build && mkdir -p build && cd build && \
  cmake -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} -DCMAKE_CXX_STANDARD=17 \
        -DWITH_JEMALLOC=ON -DWITH_ZSTD=ON -DROCKSDB_BUILD_SHARED=OFF \
        -DWITH_GFLAGS=OFF -DWITH_TESTS=OFF -DWITH_BENCHMARK_TOOLS=OFF \
        -DWITH_CORE_TOOLS=OFF -DWITH_TOOLS=OFF -DWITH_FOLLY_DISTRIBUTED_MUTEX=OFF \
        -DCMAKE_INSTALL_PREFIX="${DEPS_INSTALL_PATH}" .. && \
  make -j$(nproc) install && \
  rm -rf "${BASE_DIR}/deps/rocksdb/build"
