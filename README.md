# konserve-lmdb

[![Slack](https://img.shields.io/badge/slack-join_chat-brightgreen.svg)](https://clojurians.slack.com/archives/CB7GJAN0L)
[![Clojars](https://img.shields.io/clojars/v/io.replikativ/konserve-lmdb.svg)](https://clojars.org/io.replikativ/konserve-lmdb)
[![CircleCI](https://circleci.com/gh/replikativ/konserve-lmdb.svg?style=shield)](https://circleci.com/gh/replikativ/konserve-lmdb)
[![Last Commit](https://img.shields.io/github/last-commit/replikativ/konserve-lmdb/main.svg)](https://github.com/replikativ/konserve-lmdb/tree/main)

A high-performance [LMDB](https://www.symas.com/lmdb) backend for [konserve](https://github.com/replikativ/konserve) using Project Panama FFI (Java 22+).

## Features

- **Zero-copy reads** via LMDB's memory-mapped architecture
- **Custom binary encoding** optimized for speed (faster than Fressian/Nippy for common types)
- **Two API levels**: Full konserve compatibility or Direct API for maximum performance
- **Lock-free operations**: LMDB provides MVCC, no application-level locking needed
- **Extensible type handlers** for custom serialization

## Requirements

- **Java 22+** (for Project Panama FFI)
- **liblmdb** native library

### Installing LMDB

The library auto-detects common system paths, so usually just installing the package is enough.

#### Ubuntu/Debian

```bash
sudo apt install liblmdb0
```

#### macOS (Homebrew)

```bash
brew install lmdb
```

#### Arch Linux

```bash
sudo pacman -S lmdb
```

#### Building from Source

LMDB is a small, dependency-free C library that compiles in seconds:

```bash
# Clone the official LMDB repository
git clone https://git.openldap.org/openldap/openldap.git
cd openldap/libraries/liblmdb

# Build (produces liblmdb.so and liblmdb.a)
make

# Install system-wide (recommended)
sudo make install
```

#### Custom Library Path

If the library is in a non-standard location, set `KONSERVE_LMDB_LIB`:

```bash
export KONSERVE_LMDB_LIB=/path/to/liblmdb.so
```

## Usage

Add to your dependencies:

[![Clojars Project](http://clojars.org/io.replikativ/konserve-lmdb/latest-version.svg)](http://clojars.org/io.replikativ/konserve-lmdb)

### Konserve API (Full Compatibility)

```clojure
(require '[konserve-lmdb.store :as lmdb]
         '[konserve.core :as k])

;; Create a store
(def store (lmdb/connect-store "/tmp/my-store"))

;; Standard konserve operations
(k/assoc store :user {:name "Alice" :age 30} {:sync? true})
(k/get store :user nil {:sync? true})
;; => {:name "Alice", :age 30}

(k/update-in store [:user :age] inc {:sync? true})
(k/get-in store [:user :age] nil {:sync? true})
;; => 31

;; Multi-key atomic operations
(k/multi-assoc store {:user1 {:name "Bob"}
                      :user2 {:name "Carol"}}
               {:sync? true})

(k/multi-get store [:user1 :user2 :missing] {:sync? true})
;; => {:user1 {:name "Bob"}, :user2 {:name "Carol"}}

;; List all keys (metadata only - efficient for GC)
(k/keys store {:sync? true})
;; => [{:key :user, :type :edn, :last-write #inst "..."}
;;     {:key :user1, :type :edn, :last-write #inst "..."}
;;     ...]

;; Binary data
(k/bassoc store :image (byte-array [1 2 3 4]) {:sync? true})
(k/bget store :image
        (fn [{:keys [input-stream size]}]
          (slurp input-stream))
        {:sync? true})

;; Clean up
(lmdb/release-store store)
```

### Direct API (Maximum Performance)

For performance-critical code, use the Direct API which bypasses konserve's metadata tracking:

```clojure
(require '[konserve-lmdb.store :as lmdb])

(def store (lmdb/connect-store "/tmp/my-store"))

;; Direct put/get - no metadata wrapper, fastest possible
(lmdb/put store :key {:data "value"})
(lmdb/get store :key)
;; => {:data "value"}

;; Batch operations - single transaction
(lmdb/multi-put store {:k1 "v1" :k2 "v2" :k3 "v3"})
(lmdb/multi-get store [:k1 :k2 :k3])
;; => {:k1 "v1", :k2 "v2", :k3 "v3"}

(lmdb/del store :key)

(lmdb/release-store store)
```

**Important**: Direct API and Konserve API use different storage formats and are **not interoperable**. Data written with `lmdb/put` cannot be read with `k/get` and vice versa. Choose one API for each store.

### Configuration Options

```clojure
(require '[konserve-lmdb.native :as n])

(lmdb/connect-store "/tmp/my-store"
  :map-size (* 1024 1024 1024)   ; LMDB map size (default: 1GB)
  :flags n/MDB_NORDAHEAD         ; Environment flags (see below)
  :type-handlers registry)       ; Custom type handlers for serialization
```

**Environment Flags:**

- `n/MDB_NORDAHEAD` - Don't use read-ahead; reduces memory pressure for large datasets
- `n/MDB_RDONLY` - Open in read-only mode; allows concurrent reading while another process writes
- `n/MDB_NOSYNC` - Don't fsync after commit; faster but less durable (use for ephemeral data)
- `n/MDB_WRITEMAP` - Use writeable mmap; faster for RAM-fitting DBs but less crash-safe
- `n/MDB_MAPASYNC` - Async flushes when using WRITEMAP; requires explicit `sync` for durability
- `n/MDB_NOTLS` - Disable thread-local storage; needed for apps with many user threads on few OS threads

Flags can be combined with `bit-or`:

```clojure
(lmdb/connect-store path :flags (bit-or n/MDB_NORDAHEAD n/MDB_NOSYNC))
```

### LMDB Best Practices & Caveats

LMDB is a powerful but low-level storage engine. Here are important considerations:

#### Do NOT Use LMDB On Network Filesystems

LMDB uses memory-mapped files and POSIX locking. **Never** store LMDB databases on NFS, CIFS, or other network/remote filesystems - this will cause data corruption.

#### Database File Growth

LMDB's database file **never shrinks automatically**. Deleted data frees pages internally for reuse, but the file size remains. To reclaim space, copy the database with compaction:

```bash
mdb_copy -c /path/to/db /path/to/compacted-db
```

#### Map Size Configuration

Set `map-size` large enough for your expected data. LMDB pre-allocates virtual address space (not physical memory). On 64-bit systems, setting 100GB+ is safe and recommended for growing databases:

```clojure
(lmdb/connect-store path :map-size (* 100 1024 1024 1024)) ; 100GB
```

#### Long-Running Processes

For servers running continuously, be aware that:

1. **Stale readers** - If a read transaction is abandoned (e.g., thread dies), it prevents space reuse until detected. LMDB has `mdb_reader_check()` but it's not exposed here yet.

2. **Keep transactions short** - Long-lived read transactions prevent freed pages from being reclaimed, causing database growth. The konserve and Direct APIs handle this correctly with short-lived transactions.

#### MDB_WRITEMAP Warning

While `MDB_WRITEMAP` is faster, it has risks:
- Buggy code can corrupt the database by writing to mapped memory
- Filesystem errors may crash the process instead of returning errors
- Use only when performance is critical and you have good backups

#### Thread Safety

LMDB environments are thread-safe. You can share a single store across all threads. However:
- Write transactions are serialized (one at a time)
- Read transactions provide MVCC isolation
- Don't pass cursors between threads

### Custom Type Handlers

For custom types (e.g., datahike's Datom), register type handlers:

```clojure
(require '[konserve-lmdb.buffer :as buf])

;; Create a handler for your type
(def my-handler
  (reify buf/ITypeHandler
    (type-tag [_] 0x20)           ; Tags 0x10-0xFF for custom types
    (type-class [_] MyRecord)
    (encode-type [_ buf value encode-fn]
      ;; Write fields to buf
      (.putLong buf (:id value))
      (encode-fn buf (:data value)))  ; Recursive encoding
    (decode-type [_ buf decode-fn]
      ;; Read fields from buf
      (->MyRecord (.getLong buf)
                  (decode-fn buf)))))

;; Create registry and pass to store
(def registry (buf/create-handler-registry [my-handler] {}))
(def store (lmdb/connect-store "/tmp/store" :type-handlers registry))
```

## Performance

Benchmarks comparing konserve-lmdb against datalevin's raw KV API.

**Test setup**: 1000 entries, ~50 bytes per value (map with UUID, timestamp, counter)

| Operation   | Native | Direct | Konserve | Datalevin |
|-------------|--------|--------|----------|-----------|
| Single Put  | 557K   | 448K   | 232K     | 347K      |
| Single Get  | 1.43M  | 1.06M  | 699K     | 768K      |
| Batch Put   | 3.52M  | 1.31M  | 375K     | 893K      |

*Operations per second, measured with criterium*

**Key findings**:
- **Direct API is faster than datalevin** for all operations
- Konserve API adds ~1-2µs overhead per operation for metadata tracking
- Batch operations are 3-10x faster than sequential puts

Run benchmarks yourself:

```bash
clojure -M:bench
```

## API Reference

### Store Management

- `(connect-store path & opts)` - Create/open an LMDB store
- `(release-store store)` - Close the store
- `(delete-store path)` - Delete store and all data

### Direct API (High Performance)

- `(put store key value)` - Store value at key
- `(get store key)` - Get value by key
- `(del store key)` - Delete key
- `(multi-put store kvs)` - Store multiple key-value pairs atomically
- `(multi-get store keys)` - Get multiple values

### Konserve Protocols

The store implements all standard konserve protocols:
- `PEDNKeyValueStore` - get-in, assoc-in, update-in, dissoc
- `PBinaryKeyValueStore` - bassoc, bget
- `PKeyIterable` - keys enumeration
- `PMultiKeyEDNValueStore` - multi-get, multi-assoc, multi-dissoc
- `PLockFreeStore` - indicates MVCC-based concurrency

## Development

```bash
# Run tests
clojure -M:test

# Run benchmarks
clojure -M:bench

# Format code
clojure -M:ffix

# Build jar
clojure -T:build jar
```

## License

Copyright © 2025 Christian Weilbach

Licensed under Eclipse Public License 2.0 (see [LICENSE](LICENSE)).
