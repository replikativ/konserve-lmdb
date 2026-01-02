(ns konserve-lmdb.native
  "Native LMDB bindings via coffi (Project Panama FFI).

   Provides zero-copy access to LMDB's memory-mapped storage.

   Two API styles:
   1. Simple API (lmdb-get, lmdb-put) - copies data, easy to use
   2. Zero-copy API (with-read-txn, get-segment) - no copies, requires txn management"
  (:require [coffi.mem :as mem]
            [coffi.ffi :as ffi])
  (:import [java.lang.foreign MemorySegment ValueLayout Arena]
           [java.nio ByteBuffer ByteOrder]
           [java.util.concurrent ConcurrentLinkedDeque]))

(set! *warn-on-reflection* true)

;;; Library loading

(def ^:private common-lib-paths
  "Common paths where liblmdb might be installed."
  [;; Linux x86_64
   "/usr/lib/x86_64-linux-gnu/liblmdb.so.0"
   "/usr/lib/x86_64-linux-gnu/liblmdb.so"
   ;; Linux aarch64
   "/usr/lib/aarch64-linux-gnu/liblmdb.so.0"
   "/usr/lib/aarch64-linux-gnu/liblmdb.so"
   ;; Generic Linux
   "/usr/lib/liblmdb.so"
   "/usr/local/lib/liblmdb.so"
   ;; macOS Homebrew
   "/opt/homebrew/lib/liblmdb.dylib"
   "/usr/local/lib/liblmdb.dylib"])

(defn- find-lib-path
  "Find liblmdb library path. Tries in order:
   1. System property konserve.lmdb.lib
   2. Environment variable KONSERVE_LMDB_LIB
   3. Common system paths
   4. System library 'lmdb' (lets OS find it)"
  []
  (or (System/getProperty "konserve.lmdb.lib")
      (System/getenv "KONSERVE_LMDB_LIB")
      (first (filter #(.exists (java.io.File. ^String %)) common-lib-paths))
      "lmdb"))

(defonce ^:private load-lib!
  (try
    (ffi/load-library (find-lib-path))
    (catch UnsatisfiedLinkError e
      (throw (ex-info "Failed to load liblmdb. Install liblmdb-dev or set KONSERVE_LMDB_LIB environment variable."
                      {:tried (find-lib-path)} e)))))

;;; Constants

(def ^:const MDB_CREATE    0x40000)
(def ^:const MDB_RDONLY    0x20000)
(def ^:const MDB_NOSUBDIR  0x4000)
(def ^:const MDB_NOSYNC    0x10000)
(def ^:const MDB_WRITEMAP  0x80000)   ; Use writeable mmap - faster but less safe
(def ^:const MDB_MAPASYNC  0x100000)  ; Async flushes with WRITEMAP
(def ^:const MDB_NOTLS     0x200000)  ; Don't use thread-local storage
(def ^:const MDB_NORDAHEAD 0x800000)  ; Don't use read-ahead - reduces memory pressure
(def ^:const MDB_NOTFOUND  -30798)

;;; Struct definitions

(def MDB_val
  "MDB_val struct: { size_t mv_size; void *mv_data; }"
  [::mem/struct [[:mv_size ::mem/long]
                 [:mv_data ::mem/pointer]]])

;;; FFI function definitions

;; Environment
(ffi/defcfn mdb-env-create
  "Create an LMDB environment handle."
  mdb_env_create [::mem/pointer] ::mem/int)

(ffi/defcfn mdb-env-open
  "Open an environment handle."
  mdb_env_open [::mem/pointer ::mem/c-string ::mem/int ::mem/int] ::mem/int)

(ffi/defcfn mdb-env-close
  "Close the environment and release resources."
  mdb_env_close [::mem/pointer] ::mem/void)

(ffi/defcfn mdb-env-set-mapsize
  "Set the size of the memory map."
  mdb_env_set_mapsize [::mem/pointer ::mem/long] ::mem/int)

(ffi/defcfn mdb-env-set-maxdbs
  "Set the maximum number of named databases."
  mdb_env_set_maxdbs [::mem/pointer ::mem/int] ::mem/int)

(ffi/defcfn mdb-env-sync
  "Flush data buffers to disk."
  mdb_env_sync [::mem/pointer ::mem/int] ::mem/int)

;; Transactions
(ffi/defcfn mdb-txn-begin
  "Create a transaction for use with the environment."
  mdb_txn_begin [::mem/pointer ::mem/pointer ::mem/int ::mem/pointer] ::mem/int)

(ffi/defcfn mdb-txn-commit
  "Commit all operations and close the transaction."
  mdb_txn_commit [::mem/pointer] ::mem/int)

(ffi/defcfn mdb-txn-abort
  "Abandon all operations and close the transaction."
  mdb_txn_abort [::mem/pointer] ::mem/void)

;; Database
(ffi/defcfn mdb-dbi-open
  "Open a database in the environment."
  mdb_dbi_open [::mem/pointer ::mem/c-string ::mem/int ::mem/pointer] ::mem/int)

;; Data operations
(ffi/defcfn mdb-get
  "Get items from a database."
  mdb_get [::mem/pointer ::mem/int ::mem/pointer ::mem/pointer] ::mem/int)

(ffi/defcfn mdb-put
  "Store items into a database."
  mdb_put [::mem/pointer ::mem/int ::mem/pointer ::mem/pointer ::mem/int] ::mem/int)

(ffi/defcfn mdb-del
  "Delete items from a database."
  mdb_del [::mem/pointer ::mem/int ::mem/pointer ::mem/pointer] ::mem/int)

;; Cursor operations
(ffi/defcfn mdb-cursor-open
  "Create a cursor handle."
  mdb_cursor_open [::mem/pointer ::mem/int ::mem/pointer] ::mem/int)

(ffi/defcfn mdb-cursor-close
  "Close a cursor handle."
  mdb_cursor_close [::mem/pointer] ::mem/void)

(ffi/defcfn mdb-cursor-get
  "Retrieve by cursor."
  mdb_cursor_get [::mem/pointer ::mem/pointer ::mem/pointer ::mem/int] ::mem/int)

;; Cursor operations
(def ^:const MDB_FIRST  0)
(def ^:const MDB_NEXT   8)

;; Error handling
(ffi/defcfn mdb-strerror
  "Return a string describing a given error code."
  mdb_strerror [::mem/int] ::mem/c-string)

;;; Helper functions

(defn make-mdb-val
  "Create an MDB_val struct from bytes."
  [arena ^bytes data]
  (let [seg (mem/alloc-instance MDB_val arena)
        len (alength data)
        data-seg (mem/alloc len arena)]
    (mem/write-bytes data-seg len data)
    (mem/write-long seg 0 len)
    (mem/write-address seg 8 data-seg)
    seg))

(defn fill-mdb-val
  "Fill an existing MDB_val struct with data. Allocates data segment from arena."
  [arena mdb-val-seg ^bytes data]
  (let [len (alength data)
        data-seg (mem/alloc len arena)]
    (mem/write-bytes data-seg len data)
    (mem/write-long mdb-val-seg 0 len)
    (mem/write-address mdb-val-seg 8 data-seg)
    mdb-val-seg))

(defn read-mdb-val
  "Read bytes from an MDB_val struct. Returns byte array or nil.
   Note: This copies data. For zero-copy, use read-mdb-val-segment."
  [mdb-val-seg]
  (let [size (mem/read-long mdb-val-seg 0)]
    (when (pos? size)
      (let [addr (mem/read-long mdb-val-seg 8)
            ^MemorySegment seg (.reinterpret (MemorySegment/ofAddress addr) size)]
        (.toArray seg ValueLayout/JAVA_BYTE)))))

(defn read-mdb-val-segment
  "Read MemorySegment from MDB_val struct. Returns [size segment] or nil.
   ZERO-COPY: The segment points directly to LMDB's mmap'd memory.
   WARNING: Only valid while the read transaction is open!"
  [mdb-val-seg]
  (let [size (mem/read-long mdb-val-seg 0)]
    (when (pos? size)
      (let [addr (mem/read-long mdb-val-seg 8)
            ^MemorySegment seg (.reinterpret (MemorySegment/ofAddress addr) size)]
        [size seg]))))

(defn segment->byte-buffer
  "Wrap a MemorySegment as a read-only ByteBuffer. Zero-copy.
   The ByteBuffer shares memory with the segment."
  ^ByteBuffer [^MemorySegment seg ^long size]
  (-> (.asByteBuffer seg)
      (.order ByteOrder/BIG_ENDIAN)
      (.limit (int size))
      (.asReadOnlyBuffer)))

(defn check-rc
  "Check return code, throw on error."
  [rc context]
  (when-not (zero? rc)
    (throw (ex-info (str context ": " (mdb-strerror rc))
                    {:rc rc :context context})))
  rc)

;;; High-level API

;; LMDBEnv includes per-env pools to avoid cross-env use-after-free issues.
;; When an env is closed, its pools become unreachable and GC'd together.
(defrecord LMDBEnv [env-ptr dbi arena mdb-val-pool ptr-pool])

(def ^:const +pool-max-size+ 64)

(defn- get-pooled-mdb-val
  "Get an MDB_val struct from env's pool or allocate new."
  [{:keys [arena ^ConcurrentLinkedDeque mdb-val-pool]}]
  (or (.poll mdb-val-pool)
      (mem/alloc-instance MDB_val arena)))

(defn- return-pooled-mdb-val
  "Return MDB_val to env's pool if not full."
  [{:keys [^ConcurrentLinkedDeque mdb-val-pool]} mdb-val]
  (when (< (.size mdb-val-pool) +pool-max-size+)
    (.offer mdb-val-pool mdb-val)))

(defn- get-pooled-ptr
  "Get a pointer holder from env's pool or allocate new."
  [{:keys [arena ^ConcurrentLinkedDeque ptr-pool]}]
  (or (.poll ptr-pool)
      (mem/alloc-instance ::mem/pointer arena)))

(defn- return-pooled-ptr
  "Return pointer to env's pool if not full."
  [{:keys [^ConcurrentLinkedDeque ptr-pool]} ptr]
  (when (< (.size ptr-pool) +pool-max-size+)
    (.offer ptr-pool ptr)))

;;; Read Transaction for Zero-Copy Access

(defrecord ReadTxn [env txn-ptr])

(defn begin-read-txn
  "Begin a read-only transaction. Returns ReadTxn.
   Use with get-segment for zero-copy reads.
   MUST call end-read-txn when done!"
  ^ReadTxn [{:keys [env-ptr] :as env}]
  (let [txn-ptr-holder (get-pooled-ptr env)]
    (check-rc (mdb-txn-begin env-ptr nil MDB_RDONLY txn-ptr-holder) "mdb_txn_begin")
    (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
      (return-pooled-ptr env txn-ptr-holder)
      (->ReadTxn env txn-ptr))))

(defn end-read-txn
  "End a read transaction. Must be called after begin-read-txn."
  [{:keys [txn-ptr]}]
  (mdb-txn-abort txn-ptr))

(defmacro with-read-txn
  "Execute body with a read transaction. Zero-copy segments are valid within body.

   Usage:
     (with-read-txn [txn env]
       (when-let [[size seg] (get-segment txn key-bytes)]
         (let [buf (segment->byte-buffer seg size)]
           (decode-from-buffer buf))))"
  [[txn-sym env] & body]
  `(let [~txn-sym (begin-read-txn ~env)]
     (try
       ~@body
       (finally
         (end-read-txn ~txn-sym)))))

(defn get-segment
  "Get value as MemorySegment within a read transaction. Zero-copy.
   Returns [size segment] or nil if not found.
   WARNING: Segment only valid while transaction is open!"
  [{:keys [env txn-ptr]} ^bytes key-bytes]
  (let [{:keys [dbi]} env]
    ;; Use confined arena for key data - freed when scope exits
    (with-open [scratch (Arena/ofConfined)]
      (let [key-val (make-mdb-val scratch key-bytes)
            data-val (get-pooled-mdb-val env)
            rc (mdb-get txn-ptr dbi key-val data-val)
            ;; Read result BEFORE returning to pool to avoid race condition
            result (cond
                     (zero? rc) (read-mdb-val-segment data-val)
                     (= rc MDB_NOTFOUND) nil
                     :else (check-rc rc "mdb_get"))]
        (return-pooled-mdb-val env data-val)
        result))))

(defn get-buffer
  "Get value as ByteBuffer within a read transaction. Zero-copy.
   Returns ByteBuffer or nil if not found.
   WARNING: Buffer only valid while transaction is open!"
  ^ByteBuffer [txn ^bytes key-bytes]
  (when-let [[size seg] (get-segment txn key-bytes)]
    (segment->byte-buffer seg size)))

(defn open-env
  "Open an LMDB environment. Returns LMDBEnv record.

   Options:
     :map-size  - Size of memory map in bytes (default 1GB)
     :flags     - Environment flags (default 0)
                  Use MDB_RDONLY for read-only access to existing DB
                  (allows concurrent reading while another process writes)"
  [path & {:keys [map-size flags]
           :or {map-size (* 1024 1024 1024)
                flags 0}}]
  (let [arena (mem/shared-arena)
        env-ptr-holder (mem/alloc-instance ::mem/pointer arena)
        read-only? (pos? (bit-and flags MDB_RDONLY))]
    (check-rc (mdb-env-create env-ptr-holder) "mdb_env_create")
    (let [env-ptr (mem/read-address env-ptr-holder 0)]
      (check-rc (mdb-env-set-mapsize env-ptr map-size) "mdb_env_set_mapsize")
      (check-rc (mdb-env-set-maxdbs env-ptr 1) "mdb_env_set_maxdbs")
      (check-rc (mdb-env-open env-ptr path flags 0644) "mdb_env_open")
      ;; Open default database
      (let [txn-ptr-holder (mem/alloc-instance ::mem/pointer arena)
            ;; Use read txn if read-only mode
            txn-flags (if read-only? MDB_RDONLY 0)]
        (check-rc (mdb-txn-begin env-ptr nil txn-flags txn-ptr-holder) "mdb_txn_begin")
        (let [txn-ptr (mem/read-address txn-ptr-holder 0)
              dbi-holder (mem/alloc-instance ::mem/int arena)
              ;; Don't use MDB_CREATE for read-only (DB already exists)
              dbi-flags (if read-only? 0 MDB_CREATE)]
          (check-rc (mdb-dbi-open txn-ptr nil dbi-flags dbi-holder) "mdb_dbi_open")
          (let [dbi (mem/read-int dbi-holder 0)]
            (check-rc (mdb-txn-commit txn-ptr) "mdb_txn_commit")
            ;; Create per-env pools to avoid cross-env use-after-free
            (->LMDBEnv env-ptr dbi arena
                       (ConcurrentLinkedDeque.)
                       (ConcurrentLinkedDeque.))))))))

(defn close-env
  "Close an LMDB environment."
  [{:keys [env-ptr]}]
  (mdb-env-close env-ptr))

(defn env-sync
  "Force sync environment to disk."
  [{:keys [env-ptr]}]
  (check-rc (mdb-env-sync env-ptr 1) "mdb_env_sync"))

;;; Write Transaction for Atomic Read-Modify-Write

(defrecord WriteTxn [env txn-ptr])

(defn begin-write-txn
  "Begin a write transaction. Returns WriteTxn.
   LMDB only allows one write transaction at a time - concurrent calls block.
   MUST call commit-write-txn or abort-write-txn when done!"
  ^WriteTxn [{:keys [env-ptr] :as env}]
  (let [txn-ptr-holder (get-pooled-ptr env)]
    (check-rc (mdb-txn-begin env-ptr nil 0 txn-ptr-holder) "mdb_txn_begin")
    (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
      (return-pooled-ptr env txn-ptr-holder)
      (->WriteTxn env txn-ptr))))

(defn commit-write-txn
  "Commit and close a write transaction."
  [{:keys [txn-ptr]}]
  (check-rc (mdb-txn-commit txn-ptr) "mdb_txn_commit"))

(defn abort-write-txn
  "Abort and close a write transaction."
  [{:keys [txn-ptr]}]
  (mdb-txn-abort txn-ptr))

(defmacro with-write-txn
  "Execute body within a write transaction. Commits on success, aborts on exception.
   LMDB serializes write transactions, so this provides atomicity for read-modify-write.

   Usage:
     (with-write-txn [txn env]
       (let [old-val (get-in-txn txn key-bytes decode-fn)]
         (put-in-txn txn key-bytes (encode new-val))))"
  [[txn-sym env] & body]
  `(let [~txn-sym (begin-write-txn ~env)]
     (try
       (let [result# (do ~@body)]
         (commit-write-txn ~txn-sym)
         result#)
       (catch Throwable t#
         (abort-write-txn ~txn-sym)
         (throw t#)))))

(defn get-in-txn
  "Get value within a transaction (read or write). Returns byte array or nil."
  [{:keys [env txn-ptr]} ^bytes key-bytes]
  (let [{:keys [dbi]} env]
    ;; Use confined arena for data - freed when scope exits
    (with-open [scratch (Arena/ofConfined)]
      (let [key-val (get-pooled-mdb-val env)
            _ (fill-mdb-val scratch key-val key-bytes)
            data-val (get-pooled-mdb-val env)
            rc (mdb-get txn-ptr dbi key-val data-val)
            result (cond
                     (zero? rc) (read-mdb-val data-val)
                     (= rc MDB_NOTFOUND) nil
                     :else (check-rc rc "mdb_get"))]
        (return-pooled-mdb-val env key-val)
        (return-pooled-mdb-val env data-val)
        result))))

(defn get-decode-in-txn
  "Get and decode value within a transaction. Zero-copy decode.
   decode-fn takes a ByteBuffer and returns the decoded value."
  [{:keys [env txn-ptr]} ^bytes key-bytes decode-fn]
  (let [{:keys [dbi]} env]
    ;; Use confined arena for data - freed when scope exits
    (with-open [scratch (Arena/ofConfined)]
      (let [key-val (get-pooled-mdb-val env)
            _ (fill-mdb-val scratch key-val key-bytes)
            data-val (get-pooled-mdb-val env)
            rc (mdb-get txn-ptr dbi key-val data-val)
            result (cond
                     (zero? rc)
                     (when-let [[size seg] (read-mdb-val-segment data-val)]
                       (decode-fn (segment->byte-buffer seg size)))
                     (= rc MDB_NOTFOUND) nil
                     :else (check-rc rc "mdb_get"))]
        (return-pooled-mdb-val env key-val)
        (return-pooled-mdb-val env data-val)
        result))))

(defn put-in-txn
  "Put a key-value pair within a write transaction."
  [{:keys [env txn-ptr]} ^bytes key-bytes ^bytes val-bytes]
  (let [{:keys [dbi]} env]
    ;; Use confined arena for data - freed when scope exits
    (with-open [scratch (Arena/ofConfined)]
      (let [key-val (get-pooled-mdb-val env)
            _ (fill-mdb-val scratch key-val key-bytes)
            data-val (get-pooled-mdb-val env)
            _ (fill-mdb-val scratch data-val val-bytes)]
        (check-rc (mdb-put txn-ptr dbi key-val data-val 0) "mdb_put")
        (return-pooled-mdb-val env key-val)
        (return-pooled-mdb-val env data-val)))))

(defn del-in-txn
  "Delete a key within a write transaction. Returns true if key existed."
  [{:keys [env txn-ptr]} ^bytes key-bytes]
  (let [{:keys [dbi]} env]
    ;; Use confined arena for data - freed when scope exits
    (with-open [scratch (Arena/ofConfined)]
      (let [key-val (get-pooled-mdb-val env)
            _ (fill-mdb-val scratch key-val key-bytes)
            rc (mdb-del txn-ptr dbi key-val nil)]
        (return-pooled-mdb-val env key-val)
        (when (and (not (zero? rc)) (not= rc MDB_NOTFOUND))
          (check-rc rc "mdb_del"))
        (zero? rc)))))

(defn lmdb-get
  "Get a value by key. Returns byte array or nil if not found."
  [{:keys [env-ptr dbi] :as env} ^bytes key-bytes]
  (let [txn-ptr-holder (get-pooled-ptr env)]
    (check-rc (mdb-txn-begin env-ptr nil MDB_RDONLY txn-ptr-holder) "mdb_txn_begin")
    (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
      (return-pooled-ptr env txn-ptr-holder)
      ;; Use confined arena for data - freed when scope exits
      (with-open [scratch (Arena/ofConfined)]
        (try
          (let [key-val (get-pooled-mdb-val env)
                _ (fill-mdb-val scratch key-val key-bytes)
                data-val (get-pooled-mdb-val env)
                rc (mdb-get txn-ptr dbi key-val data-val)
                result (cond
                         (zero? rc) (read-mdb-val data-val)
                         (= rc MDB_NOTFOUND) nil
                         :else (check-rc rc "mdb_get"))]
            (return-pooled-mdb-val env key-val)
            (return-pooled-mdb-val env data-val)
            result)
          (finally
            (mdb-txn-abort txn-ptr)))))))

(defn lmdb-get-decode
  "Get and decode a value by key in a single transaction. Zero-copy decode.
   decode-fn takes a ByteBuffer and returns the decoded value.
   Returns nil if not found."
  [{:keys [env-ptr dbi] :as env} ^bytes key-bytes decode-fn]
  (let [txn-ptr-holder (get-pooled-ptr env)]
    (check-rc (mdb-txn-begin env-ptr nil MDB_RDONLY txn-ptr-holder) "mdb_txn_begin")
    (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
      (return-pooled-ptr env txn-ptr-holder)
      ;; Use confined arena for data - freed when scope exits
      (with-open [scratch (Arena/ofConfined)]
        (try
          (let [key-val (get-pooled-mdb-val env)
                _ (fill-mdb-val scratch key-val key-bytes)
                data-val (get-pooled-mdb-val env)
                rc (mdb-get txn-ptr dbi key-val data-val)
                result (cond
                         (zero? rc)
                         (when-let [[size seg] (read-mdb-val-segment data-val)]
                           (decode-fn (segment->byte-buffer seg size)))
                         (= rc MDB_NOTFOUND) nil
                         :else (check-rc rc "mdb_get"))]
            (return-pooled-mdb-val env key-val)
            (return-pooled-mdb-val env data-val)
            result)
          (finally
            (mdb-txn-abort txn-ptr)))))))

(defn lmdb-put
  "Put a key-value pair."
  [{:keys [env-ptr dbi] :as env} ^bytes key-bytes ^bytes val-bytes]
  (let [txn-ptr-holder (get-pooled-ptr env)]
    (check-rc (mdb-txn-begin env-ptr nil 0 txn-ptr-holder) "mdb_txn_begin")
    (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
      (return-pooled-ptr env txn-ptr-holder)
      ;; Use confined arena for data - freed when scope exits
      (with-open [scratch (Arena/ofConfined)]
        (try
          (let [key-val (get-pooled-mdb-val env)
                _ (fill-mdb-val scratch key-val key-bytes)
                data-val (get-pooled-mdb-val env)
                _ (fill-mdb-val scratch data-val val-bytes)]
            (check-rc (mdb-put txn-ptr dbi key-val data-val 0) "mdb_put")
            (return-pooled-mdb-val env key-val)
            (return-pooled-mdb-val env data-val)
            (check-rc (mdb-txn-commit txn-ptr) "mdb_txn_commit"))
          (catch Throwable t
            (mdb-txn-abort txn-ptr)
            (throw t)))))))

(defn lmdb-del
  "Delete a key."
  [{:keys [env-ptr dbi] :as env} ^bytes key-bytes]
  (let [txn-ptr-holder (get-pooled-ptr env)]
    (check-rc (mdb-txn-begin env-ptr nil 0 txn-ptr-holder) "mdb_txn_begin")
    (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
      (return-pooled-ptr env txn-ptr-holder)
      ;; Use confined arena for data - freed when scope exits
      (with-open [scratch (Arena/ofConfined)]
        (try
          (let [key-val (get-pooled-mdb-val env)
                _ (fill-mdb-val scratch key-val key-bytes)
                rc (mdb-del txn-ptr dbi key-val nil)]
            (return-pooled-mdb-val env key-val)
            (when (and (not (zero? rc)) (not= rc MDB_NOTFOUND))
              (check-rc rc "mdb_del"))
            (check-rc (mdb-txn-commit txn-ptr) "mdb_txn_commit")
            (zero? rc))
          (catch Throwable t
            (mdb-txn-abort txn-ptr)
            (throw t)))))))

(defn lmdb-keys
  "Return all keys in the database as a vector of byte arrays."
  [{:keys [env-ptr dbi arena]}]
  ;; lmdb-keys uses direct arena allocation (not pooled) because the MDB_vals
  ;; are reused across the cursor loop and need stable memory for the iteration.
  (let [txn-ptr-holder (mem/alloc-instance ::mem/pointer arena)]
    (check-rc (mdb-txn-begin env-ptr nil MDB_RDONLY txn-ptr-holder) "mdb_txn_begin")
    (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
      (try
        (let [cursor-holder (mem/alloc-instance ::mem/pointer arena)]
          (check-rc (mdb-cursor-open txn-ptr dbi cursor-holder) "mdb_cursor_open")
          (let [cursor-ptr (mem/read-address cursor-holder 0)
                key-val (mem/alloc-instance MDB_val arena)
                data-val (mem/alloc-instance MDB_val arena)]
            (try
              (loop [keys []
                     op MDB_FIRST]
                (let [rc (mdb-cursor-get cursor-ptr key-val data-val op)]
                  (if (zero? rc)
                    (recur (conj keys (read-mdb-val key-val)) MDB_NEXT)
                    keys)))
              (finally
                (mdb-cursor-close cursor-ptr)))))
        (finally
          (mdb-txn-abort txn-ptr))))))

(defn lmdb-multi-get
  "Get multiple values by keys. Returns map of key-bytes -> val-bytes.
   Only includes keys that were found."
  [{:keys [env-ptr dbi] :as env} keys-bytes]
  (if (empty? keys-bytes)
    {}
    (let [txn-ptr-holder (get-pooled-ptr env)]
      (check-rc (mdb-txn-begin env-ptr nil MDB_RDONLY txn-ptr-holder) "mdb_txn_begin")
      (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
        (return-pooled-ptr env txn-ptr-holder)
        ;; Use confined arena for all key data in this batch - freed when scope exits
        (with-open [scratch (Arena/ofConfined)]
          (try
            (let [key-val (get-pooled-mdb-val env)
                  data-val (get-pooled-mdb-val env)
                  result (reduce (fn [acc key-bytes]
                                   (fill-mdb-val scratch key-val key-bytes)
                                   (let [rc (mdb-get txn-ptr dbi key-val data-val)]
                                     (if (zero? rc)
                                       (assoc acc key-bytes (read-mdb-val data-val))
                                       acc)))
                                 {}
                                 keys-bytes)]
              (return-pooled-mdb-val env key-val)
              (return-pooled-mdb-val env data-val)
              result)
            (finally
              (mdb-txn-abort txn-ptr))))))))

(defn lmdb-multi-put
  "Put multiple key-value pairs in a single transaction.
   kv-pairs is a sequence of [key-bytes val-bytes] pairs."
  [{:keys [env-ptr dbi] :as env} kv-pairs]
  (when (seq kv-pairs)
    (let [txn-ptr-holder (get-pooled-ptr env)]
      (check-rc (mdb-txn-begin env-ptr nil 0 txn-ptr-holder) "mdb_txn_begin")
      (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
        (return-pooled-ptr env txn-ptr-holder)
        ;; Use confined arena for all key/value data in this batch - freed when scope exits
        (with-open [scratch (Arena/ofConfined)]
          (try
            (let [key-val (get-pooled-mdb-val env)
                  data-val (get-pooled-mdb-val env)]
              (doseq [[key-bytes val-bytes] kv-pairs]
                (fill-mdb-val scratch key-val key-bytes)
                (fill-mdb-val scratch data-val val-bytes)
                (check-rc (mdb-put txn-ptr dbi key-val data-val 0) "mdb_put"))
              (return-pooled-mdb-val env key-val)
              (return-pooled-mdb-val env data-val))
            (check-rc (mdb-txn-commit txn-ptr) "mdb_txn_commit")
            (catch Throwable t
              (mdb-txn-abort txn-ptr)
              (throw t))))))))

(defn lmdb-multi-del
  "Delete multiple keys in a single transaction.
   Returns map of key-bytes -> existed? (true if was deleted)."
  [{:keys [env-ptr dbi] :as env} keys-bytes]
  (if (empty? keys-bytes)
    {}
    (let [txn-ptr-holder (get-pooled-ptr env)]
      (check-rc (mdb-txn-begin env-ptr nil 0 txn-ptr-holder) "mdb_txn_begin")
      (let [txn-ptr (mem/read-address txn-ptr-holder 0)]
        (return-pooled-ptr env txn-ptr-holder)
        ;; Use confined arena for all key data in this batch - freed when scope exits
        (with-open [scratch (Arena/ofConfined)]
          (try
            (let [results (reduce (fn [acc key-bytes]
                                    (let [key-val (make-mdb-val scratch key-bytes)
                                          rc (mdb-del txn-ptr dbi key-val nil)]
                                      (assoc acc key-bytes (zero? rc))))
                                  {}
                                  keys-bytes)]
              (check-rc (mdb-txn-commit txn-ptr) "mdb_txn_commit")
              results)
            (catch Throwable t
              (mdb-txn-abort txn-ptr)
              (throw t))))))))

(comment
  ;; Usage example
  (def env (open-env "/tmp/test-lmdb"))
  (lmdb-put env (.getBytes "hello") (.getBytes "world"))
  (String. (lmdb-get env (.getBytes "hello")))
  ;; => "world"
  (map #(String. %) (lmdb-keys env))
  (close-env env))
