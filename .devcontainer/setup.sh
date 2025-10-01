#!/bin/bash
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

# Velox Dev Container Setup Script
# This script helps set up and manage the Velox development container

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🚀 Velox Dev Container Setup"
echo "============================="

# Function to build the container
build_container() {
    echo "📦 Building dev container..."
    cd "$SCRIPT_DIR"
    docker compose build
    echo "✅ Container built successfully!"
}

# Function to start the container
start_container() {
    echo "🏃 Starting dev container..."
    cd "$SCRIPT_DIR"
    docker compose up -d
    echo "✅ Container started successfully!"
    echo "🔗 You can now attach your IDE to the running container"
}

# Function to stop the container
stop_container() {
    echo "🛑 Stopping dev container..."
    cd "$SCRIPT_DIR"
    docker compose down
    echo "✅ Container stopped successfully!"
}

# Function to enter the container shell
shell() {
    echo "🐚 Opening shell in dev container..."
    cd "$SCRIPT_DIR"
    docker compose exec velox-dev bash
}

# Function to show container status
status() {
    echo "📊 Container Status:"
    cd "$SCRIPT_DIR"
    docker compose ps
}

# Function to show help
show_help() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  build    Build the dev container"
    echo "  start    Start the dev container"
    echo "  stop     Stop the dev container"
    echo "  shell    Open a shell in the running container"
    echo "  status   Show container status"
    echo "  help     Show this help message"
    echo ""
    echo "Example workflow:"
    echo "  $0 build    # Build the container (first time only)"
    echo "  $0 start    # Start the container"
    echo "  # Now open CLion and attach to the container"
    echo "  $0 stop     # Stop when done"
}

# Main script logic
case "${1:-help}" in
    build)
        build_container
        ;;
    start)
        start_container
        ;;
    stop)
        stop_container
        ;;
    shell)
        shell
        ;;
    status)
        status
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac