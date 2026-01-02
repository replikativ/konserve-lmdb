# CLAUDE.md - konserve-lmdb

This file provides context for Claude Code when working on this repository.

## Project Overview

**konserve-lmdb** is a high-performance LMDB backend for konserve using Project Panama FFI (Java 22+). It provides two API levels:

1. **Konserve API** - Full konserve protocol compatibility with metadata tracking
2. **Direct API** - Maximum performance for hot paths (e.g., datahike index storage)

## Build Commands

```bash
# Run tests (auto-detects system liblmdb)
clojure -M:test

# Run benchmarks
clojure -M:bench

# Format code
clojure -M:ffix

# Build jar
clojure -T:build jar

# Install locally
clojure -T:build install
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         konserve.core API                           │
│              (k/get, k/assoc, k/multi-assoc, etc.)                  │
├─────────────────────────────────────────────────────────────────────┤
│                          LMDBStore                                  │
│     (implements PEDNKeyValueStore, PMultiKeyEDNValueStore,          │
│      PBinaryKeyValueStore, PKeyIterable, PLockFreeStore)            │
│                                                                     │
│   Storage format: {:meta {...} :value <data>}                       │
├─────────────────────────────────────────────────────────────────────┤
│                         Direct API                                  │
│              (store/put, store/get, store/multi-put)                │
│                                                                     │
│   Storage format: <data> (no wrapper, maximum performance)          │
├─────────────────────────────────────────────────────────────────────┤
│                        Buffer Encoder                               │
│     (buffer.clj - typed binary encoding, type handlers)             │
├─────────────────────────────────────────────────────────────────────┤
│                        Native Layer                                 │
│     (native.clj - coffi bindings to liblmdb via Panama FFI)         │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Files

| File | Purpose |
|------|---------|
| `src/konserve_lmdb/store.clj` | LMDBStore record, konserve protocols, Direct API |
| `src/konserve_lmdb/native.clj` | LMDB FFI bindings via coffi/Panama |
| `src/konserve_lmdb/buffer.clj` | Binary encoder/decoder, type handlers, buffer pool |
| `bench/konserve_lmdb/run_benchmark.clj` | Public benchmark suite |
| `test/konserve_lmdb/` | Tests including konserve compliance test |

## Performance (Current Benchmarks)

1000 entries, ~50 bytes per value:

| Operation   | Native | Direct | Konserve | Datalevin |
|-------------|--------|--------|----------|-----------|
| Single Put  | 557K   | 448K   | 232K     | 347K      |
| Single Get  | 1.43M  | 1.06M  | 699K     | 768K      |
| Batch Put   | 3.52M  | 1.31M  | 375K     | 893K      |

**Key finding**: Direct API is faster than datalevin for all operations.

## API Layers

### 1. Konserve API (Full Compatibility)

Uses `{:meta {...} :value <data>}` storage format:

```clojure
(require '[konserve-lmdb.store :as lmdb]
         '[konserve.core :as k])

(def store (lmdb/connect-store "/tmp/store"))
(k/assoc store :key {:data "value"} {:sync? true})
(k/get store :key nil {:sync? true})
```

### 2. Direct API (Maximum Performance)

Stores values directly without metadata wrapper:

```clojure
(lmdb/put store :key {:data "value"})
(lmdb/get store :key)
(lmdb/multi-put store {:k1 "v1" :k2 "v2"})
```

**Important**: Direct API and Konserve API are NOT interoperable. If you `store/put` then try `k/get`, you'll get an error explaining the mismatch.

## Key Design Decisions

### 1. PLockFreeStore Protocol

LMDB provides MVCC, so no application-level locking is needed. The store implements `PLockFreeStore` with `-lock-free?` returning `true`. This tells konserve.core to skip lock acquisition.

### 2. Metadata-Only Decoding for Keys

The `-keys` implementation uses `decode-meta-only` to skip decoding the `:value` blob. This is 25x faster than full decode - important for GC enumeration over large datasets.

### 3. Binary Encoding Format

Custom typed encoding (faster than Fressian/Nippy):

```
Type Tags (1 byte):
  0x00: nil
  0x01: boolean
  0x02: long (8 bytes)
  0x03: double (8 bytes)
  0x04: string (4-byte length + UTF-8)
  0x05: keyword
  0x06: symbol
  0x07: uuid (16 bytes)
  0x08: instant (8 bytes millis)
  0x09: bytes (4-byte length + data)
  0x0A: list
  0x0B: vector
  0x0C: map
  0x0D: set
  0x10-0xFF: Custom types (via ITypeHandler)
```

### 4. Extensible Type Handlers

For custom types (e.g., datahike's Datom), create per-store registries:

```clojure
(def registry (buf/create-handler-registry [my-handler] {:context ctx}))
(def store (lmdb/connect-store path :type-handlers registry))
```

Handlers close over context needed for deserialization.

## Testing

```bash
# Run all tests (12 tests, 154 assertions)
clojure -M:test

# Run specific test
clojure -M:test --focus konserve-lmdb.compliance-test
```

The compliance test runs konserve's standard test suite against LMDBStore.

## Common Issues

### "Data was written with Direct API" Error

If you see this error when calling `k/get`:
```
Data was written with Direct API (store/put), not konserve API (k/assoc).
Use store/get to read it, or re-write with k/assoc.
```

This means the data was stored with `store/put` (no metadata wrapper) but you're trying to read with `k/get` (expects metadata wrapper). Use consistent APIs.

### Native Access Warning

If you see warnings about native access, add to JVM opts:
```
--enable-native-access=ALL-UNNAMED
```

### LMDB Library Not Found

The library auto-detects common system paths. If detection fails, set:
```bash
export KONSERVE_LMDB_LIB=/path/to/liblmdb.so
```

## Related Projects

- **konserve** - Core key-value store abstraction
- **datahike-lmdb** - Datahike integration using this store
- **datalevin** - LMDB-based database (performance comparison reference)

## Code Style

- Use `clojure -M:ffix` to format code
- Prefer explicit type hints for performance-critical code
- Use `set! *warn-on-reflection* true` in all namespaces
