# Velox Dev Container for CLion

This dev container configuration provides a complete development environment for Velox that works seamlessly with JetBrains CLion and other IDEs that support dev containers.

## Features

- **Complete Velox build environment** - Based on Ubuntu 22.04 with all Velox dependencies pre-installed
- **Development tools** - Includes GDB, LLDB, Valgrind, and other debugging tools
- **Build optimization** - Configured with ccache for faster incremental builds
- **CLion integration** - Optimized for CLion with proper CMake configuration
- **Persistent storage** - Separate volumes for build cache and ccache to persist between container rebuilds

## Quick Start

### Using with CLion

1. **Install CLion** with the Dev Containers plugin
2. **Open the repository** in CLion
3. **Select "Reopen in Container"** when prompted, or use the command palette
4. **Wait for container build** - First time setup will take 10-15 minutes
5. **Start developing** - CLion will automatically configure CMake and indexing

### Using with VS Code

1. **Install VS Code** with the Dev Containers extension
2. **Open the repository** in VS Code
3. **Reopen in Container** when prompted
4. **Wait for setup** to complete

### Manual Docker Setup

```bash
# Build the container
cd .devcontainer
./setup.sh build

# Start the container
./setup.sh start

# Check status
./setup.sh status

# Open a shell (optional)
./setup.sh shell

# Stop when done
./setup.sh stop
```

Alternatively, you can use docker compose directly:

```bash
# Build and start
cd .devcontainer
docker compose up -d

# Stop
docker compose down
```

## Configuration

### Build Settings

The dev container is configured with the following CMake settings optimized for development:

- `CMAKE_BUILD_TYPE=Debug` - Debug builds with full symbols
- `VELOX_BUILD_TESTING=ON` - Enable unit tests
- `VELOX_ENABLE_DUCKDB=ON` - Enable DuckDB integration
- `VELOX_ENABLE_PARQUET=ON` - Enable Parquet support
- Build directory: `_build/debug`
- Build tool: Ninja (faster than Make)
- Parallel jobs: 8 (configurable via NUM_THREADS)

### Performance Optimizations

- **ccache** configured with 5GB cache for faster rebuilds
- **Persistent volumes** for build artifacts and ccache
- **Cached volume mounts** for better I/O performance

### Debugging

The container includes several debugging tools:

- **GDB** - GNU Debugger
- **LLDB** - LLVM Debugger  
- **Valgrind** - Memory error detector
- **Strace/Ltrace** - System/library call tracers
- **Performance tools** - For profiling applications

## Customization

### Environment Variables

You can customize the build by setting these environment variables in `docker-compose.yml`:

- `NUM_THREADS` - Number of parallel build jobs (default: 8)
- `CMAKE_BUILD_TYPE` - Debug, Release, RelWithDebInfo (default: Debug)
- `CCACHE_MAXSIZE` - Maximum ccache size (default: 5G)

### Additional Tools

To add more development tools, modify the `Dockerfile` and rebuild:

```bash
cd .devcontainer
docker compose build --no-cache
```

## Troubleshooting

### Docker Command Issues

If you get "unknown shorthand flag" errors:

1. **Use modern Docker Compose** - Use `docker compose` (with space) instead of `docker-compose` (with hyphen)
2. **Check flag syntax** - Docker flags come before the subcommand: `docker compose -f file.yml up`
3. **Verify Docker version** - Ensure you have Docker Compose v2 installed

### Container Build Issues

If the container fails to build:

1. **Check Docker resources** - Ensure you have at least 8GB RAM and 50GB disk space
2. **Clean build** - Run `docker compose build --no-cache`
3. **Check network** - Some dependencies are downloaded during build

### CMake Configuration Issues

If CMake configuration fails with network errors:

1. **Network access** - The container needs internet access to download dependencies
2. **Corporate firewall** - You may need to configure proxy settings
3. **Retry** - Some downloads can be flaky, try running the configuration again
4. **Pre-built dependencies** - Consider using `VELOX_DEPENDENCY_SOURCE=SYSTEM` for system packages

### First Build

The initial CMake configuration and build can take 30-60 minutes as it downloads and builds many dependencies. Subsequent builds will be much faster thanks to ccache.

### CLion Issues

If CLion doesn't detect the CMake configuration:

1. **Refresh CMake** - Go to Tools → CMake → Reset Cache and Reload Project
2. **Check toolchain** - Verify the container toolchain is selected
3. **Restart indexing** - File → Invalidate Caches and Restart

### Performance Issues

If builds are slow:

1. **Increase Docker resources** - Allocate more CPU and RAM to Docker
2. **Use volume mounts** - Ensure persistent volumes are being used
3. **Check ccache** - Verify ccache is working with `ccache -s`

## Support

For issues specific to the dev container setup, please check:

1. **Docker logs** - `docker compose logs velox-dev`
2. **Container shell** - `docker compose exec velox-dev bash`
3. **Build logs** - Check the CMake and build output in CLion

For Velox-specific issues, refer to the main [Velox documentation](../README.md).