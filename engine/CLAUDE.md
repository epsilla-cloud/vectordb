# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

- **Build (Release)**: `./build.sh`
- **Build (Debug)**: `./build.sh -d`
- **Run Tests (C++)**: `cd build && ctest`
- **Run Tests (Python)**: `./test.sh`
- **Run Single-thread Python Tests**: `./test.sh --single-thread`
- **Format Code**: `cd build && make format`
- **Check Formatting**: `cd build && make format-check`
- **Run Linter**: `cd build && make tidy`
- **Coverage Report**: `./scripts/run_coverage.sh`

## Testing

### Unit Tests
- **C++ Tests**: Google Test framework, located in `test/engine/`
- **Test Count**: 5 C++ test files covering core functionality
- **Coverage**: Comprehensive test coverage with lcov reports
- **Concurrency Tests**: Thread-safe operations and atomic configuration

### Integration Tests  
- **Python Bindings**: Located in `test/bindings/python/`
- **Concurrent Testing**: Multi-threaded Python test scenarios
- **Vector Operations**: Test different metric types (EUCLIDEAN, DOT_PRODUCT, COSINE)

### CI/CD Pipeline
- **GitHub Actions**: `.github/workflows/ci.yml`
- **Matrix Testing**: Debug/Release builds, Python 3.8-3.11
- **Security**: AddressSanitizer, memory leak detection, static analysis
- **Performance**: Benchmark tests on main/develop branches
- **Docker**: Container build verification

## Delete Modes

### Soft Delete (Default)
- **Mode**: `SOFT_DELETE=true` (default)
- **Behavior**: Records are marked as deleted using a bitset but data remains in memory
- **Performance**: Faster deletions, but uses more memory over time
- **Use Case**: Frequent deletions with occasional compaction

### Hard Delete (Concurrent Safe Version)
- **Mode**: `SOFT_DELETE=false` 
- **Behavior**: Records are physically removed from memory immediately with concurrent safety
- **Performance**: Slower deletions due to data shifting, but immediate memory reclaim
- **Concurrency**: Thread-safe batch deletion with exclusive locking
- **Use Case**: Memory-constrained environments or when immediate space reclaim is needed

#### Hard Delete Concurrent Safety Features
- **Batch Processing**: Batch deletion avoids race conditions on individual records
- **Exclusive Locking**: Uses exclusive locks to prevent concurrent access during deletion
- **Primary Key Index Rebuild**: Rebuilds primary key index after deletion to ensure consistency
- **Data Compaction**: One-time data compaction reduces memory fragmentation
- **Atomic Operations**: Atomic operations for record count and state updates

### Configuration
- Set via environment variable: `export SOFT_DELETE=false`
- Runtime configuration via JSON API: `{"SoftDelete": false}`
- Atomic configuration changes with thread-safe operations

## Environment Variables

- `TEST=""` - Set to run tests after build
- `PYTHONPATH=./build/` - For Python bindings
- `DB_PATH` - Database storage path (default: /tmp/db2)
- `SOFT_DELETE` - Enable/disable soft delete mode (default: true)
- `EPSILLA_INTRA_QUERY_THREADS` - Override number of query threads

## Architecture Overview

This is the Epsilla Vector Database engine, a C++17 application with Python bindings. The core architecture is modular:

### Core Components

- **db/**: Database engine core
  - `catalog/` - Database metadata and schema management
  - `execution/` - Query execution engine
  - `index/` - Vector indexing (NSG, spatial indexes)
  - `wal/` - Write-ahead logging
- **server/**: Server implementations
  - `db_server/` - Database server logic
  - `web_server/` - HTTP API server (using oatpp framework)
- **query/**: Query parsing and execution
- **utils/**: Shared utilities (status handling, concurrent data structures)
- **config/**: Configuration management
- **services/**: Business logic services
- **logger/**: Logging subsystem

### Key Technologies

- **Build System**: CMake with custom dependency management
- **Web Framework**: oatpp (statically linked from build/dependencies/)
- **Testing**: Google Test for C++, custom Python test suite
- **Vector Search**: Custom NSG (Navigable Small World Graph) implementation
- **Concurrency**: OpenMP, custom concurrent hashmaps and locks
- **Delete Modes**: Configurable soft/hard delete with environment variable control

### Python Bindings

- Built as `epsilla.so` module in `build/` directory
- Requires Python with shared libraries (checked at build time)
- Integration tests run both single-thread and concurrent scenarios

### Server Configuration

Default server runs on port 8888 with these options:
- `-p/--port`: Server port
- `-r/--rebuild`: Rebuild indexes on startup (default: true)
- `-l/--leader`: Run as leader node (default: true)
- `-e/--embedding_baseurl`: Embedding service URL (default: http://localhost:8889)

### Development Notes

- Uses C++17 standard with Boost filesystem
- Comprehensive coverage analysis available via `scripts/run_coverage.sh`
- All builds include debug symbols (`-g` flag)
- Docker support with multi-stage builds
- Code formatting enforced via clang-format