# bbolt++

A modern C++17 reimplementation of [etcd-io/bbolt](https://github.com/etcd-io/bbolt), a pure Go key/value store.

bbolt++ aims to be a faithful translation of bbolt's architecture — including its B+ tree page management, copy-on-write transactions, memory-mapped I/O, and freelist management — while leveraging C++17 idioms and libraries like abseil, fmt, and folly.

## Status

**Version: 1.4.0-alpha.0**

The core functionality has been implemented and verified. Basic database operations (Open, Put, Get, Commit, Close) work end-to-end. The project is under active development.

### Test Results

| Test Suite | Status | Tests |
|---|---|---|
| test_basic | PASS | 2/2 |
| test_simple | PASS | 2/2 |
| test_db_simple | PASS | 10/10 |
| test_bucket | PASS | 4/4 |
| test_bucket_simple | PASS | 7/7 |
| test_internal_common_page | PASS | 4/4 |
| test_utils | PASS | 12/12 |
| test_node | Partial | 5/6 |
| test_db | Partial | - |
| test_allocate | Partial | 1/3 |

All 30+ test targets compile successfully.

## Features

- [x] Database operations (Open, Close, Begin, Commit, Rollback)
- [x] Bucket operations (Create, Delete, Get, Put, ForEach, Sequence)
- [x] Read-only and read-write transactions
- [x] Memory-mapped file I/O (`mmap`/`munmap`)
- [x] Page management (meta, branch, leaf, freelist pages)
- [x] Freelist management (Array and HashMap implementations)
- [x] B+ tree node operations (split, rebalance, spill)
- [x] Cursor operations (First, Last, Next, Prev, Seek, Delete)
- [x] Inline buckets
- [x] Database compaction
- [x] Database consistency checks (`Tx::Check`)
- [x] FNV-1a checksum (binary-compatible with Go bbolt)
- [x] File locking (`flock`)
- [x] Memory locking (`mlock`)
- [x] Logging system
- [x] Command-line tool (`bbolt`)
- [x] 33 test files covering core and advanced scenarios
- [ ] Full test parity with Go bbolt
- [ ] Performance benchmarks
- [ ] Windows support

## Building

This project uses [xmake](https://xmake.io/) as the build system.

### Prerequisites

- C++17 compatible compiler (clang++ recommended)
- [xmake](https://xmake.io/) v3.0+
- macOS or Linux

### Build & Run

```bash
# Install xmake (if not already installed)
curl -fsSL https://xmake.io/shget.text | bash

# Configure and build the static library
xmake build bbolt_pp

# Build and run a specific test
xmake build test_basic
./build/macosx/arm64/release/test_basic

# Build and run the command-line tool
xmake build bbolt
./build/macosx/arm64/release/bbolt version
```

## Dependencies

Managed automatically by xmake:

- [abseil-cpp](https://github.com/abseil/abseil-cpp) — string formatting, hash utilities
- [fmt](https://github.com/fmtlib/fmt) — formatting library
- [folly](https://github.com/facebook/folly) — Facebook's C++ library
- [Google Test](https://github.com/google/googletest) — testing framework

## Project Structure

```
bblotpp/
├── db.cpp/h              # Core database: Open, Close, mmap, allocate, grow
├── tx.cpp/h              # Transactions: Begin, Commit, Rollback, Check
├── bucket.cpp/h          # Buckets: Create, Delete, Get, Put, ForEach, spill
├── cursor.cpp/h          # Cursor: First, Last, Next, Prev, Seek, Delete
├── node.cpp/h            # B+ tree nodes: put, del, read, write, split, rebalance
├── page.cpp/h            # Page layout: meta, leaf, branch, freelist pages
├── freelist.cpp/h        # Array-based freelist
├── freelist_hmap.cpp     # HashMap-based freelist
├── compact.cpp           # Database compaction
├── file.cpp/h            # File I/O: pread, pwrite, flock, fdatasync
├── errors.h              # ErrorCode enum and Error class
├── unsafe.h              # Unsafe pointer arithmetic helpers
├── logger.cpp/h          # Configurable logging
├── mlock.cpp/h           # Memory locking (mlock/munlock)
├── utils.cpp/h           # Utility functions
├── verify.cpp/h          # Verification utilities
├── version.h             # Version string (1.4.0-alpha.0)
├── boltsync.cpp/h        # Synchronization primitives
├── status.h              # Status definitions
├── tests/                # 33 test files
│   ├── test_common.h     # Shared test utilities
│   ├── test_basic.cpp    # Basic Open/Put/Get/Commit/Close
│   ├── db_test_simple.cpp # DB open/close, page sizes, freelist types
│   ├── bucket_test_simple.cpp # Bucket CRUD operations
│   ├── node_test.cpp     # Node put/del/split/rebalance
│   ├── ...               # 28 more test files
│   └── cmd_bbolt_command_test.cpp
├── cmd/bbolt/main.cpp    # Command-line tool
└── xmake.lua             # Build configuration
```

## Usage

```cpp
#include "db.h"

using namespace bboltpp;

int main() {
    // Open (or create) a database file
    auto [db, err] = DB::Open("my.db", 0644, nullptr);
    if (!err.OK()) return 1;

    // Read-write transaction
    auto updateErr = db->Update([](Tx* tx) -> Error {
        auto [bucket, ec] = tx->CreateBucketIfNotExists("users");
        if (ec != ErrorCode::OK) return Error{ec};

        bucket->Put("alice", "admin");
        bucket->Put("bob",   "viewer");
        return Error{};
    });

    // Read-only transaction
    auto viewErr = db->View([](Tx* tx) -> Error {
        auto bucket = tx->FindBucketByName("users");
        if (!bucket) return Error{ErrorCode::ErrBucketNotFound};

        auto val = bucket->Get("alice");
        if (val.has_value()) {
            fmt::print("alice = {}\n", *val);
        }
        return Error{};
    });

    db->Close();
    return 0;
}
```

## Go-to-C++ Translation Notes

| Go concept | C++ equivalent |
|---|---|
| `[]byte` | `std::string` / `std::string_view` |
| `*Page` (unsafe pointer) | `Page*` with `reinterpret_cast` |
| `sync.RWMutex` | `std::shared_mutex` |
| `sync.Mutex` | `std::mutex` |
| `error` | `ErrorCode` enum / `Error` class |
| `hash/fnv.New64a()` | Manual FNV-1a implementation |
| `unsafe.Pointer` arithmetic | `reinterpret_cast<char*>` byte arithmetic |
| GC-managed `*node` | `std::shared_ptr<Node>` |
| `*Bucket` with parent ref | `std::weak_ptr<Tx>` to avoid cycles |

## License

Same as the original bbolt project — [MIT License](https://github.com/etcd-io/bbolt/blob/main/LICENSE).
