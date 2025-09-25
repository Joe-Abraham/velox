# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Use the existing Velox Ubuntu development image as base
FROM ghcr.io/facebookincubator/velox-dev:ubuntu-22.04

# Install additional development tools for CLion integration
RUN apt-get update && apt-get install -y \
    # Development and debugging tools
    gdb \
    lldb \
    valgrind \
    strace \
    ltrace \
    # Performance analysis tools
    perf-tools-unstable \
    linux-tools-generic \
    # Code analysis tools
    cppcheck \
    clang-tools-15 \
    clang-format-15 \
    clang-tidy-15 \
    # Network tools for debugging
    netcat \
    telnet \
    # Text editors and utilities
    vim \
    nano \
    tree \
    htop \
    # Git tools
    git-lfs \
    # CMake and build tools
    ninja-build \
    ccache \
    # Additional libraries that might be useful
    pkg-config \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set up ccache for faster builds
ENV CCACHE_DIR=/velox/.ccache
ENV CCACHE_MAXSIZE=5G
ENV CCACHE_SLOPPINESS=time_macros
ENV PATH="/usr/lib/ccache:$PATH"

# Configure git to handle the workspace properly
RUN git config --global --add safe.directory /velox

# Create ccache directory with proper permissions
RUN mkdir -p /velox/.ccache && chmod 777 /velox/.ccache

# Set the working directory
WORKDIR /velox

# Set up environment for development
ENV CC=gcc-11
ENV CXX=g++-11
ENV CMAKE_BUILD_TYPE=Debug
ENV VELOX_BUILD_TESTING=ON
ENV VELOX_ENABLE_DUCKDB=ON
ENV VELOX_ENABLE_PARQUET=ON

# Default command
CMD ["/bin/bash"]