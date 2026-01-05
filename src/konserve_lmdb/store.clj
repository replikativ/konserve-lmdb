(ns konserve-lmdb.store
  "Direct LMDB store implementing konserve protocols.

   Bypasses DefaultStore and Fressian - uses buffer encoder directly.

   Storage format:
   - key (UTF-8 bytes) → {:meta {...} :value ...} (buffer encoded)"
  (:refer-clojure :exclude [get])
  (:require [konserve.protocols :as p :refer [PLockFreeStore]]
            [konserve.utils :refer [async+sync *default-sync-translation*]]
            [konserve.store :as store]
            [konserve-lmdb.native :as n]
            [konserve-lmdb.buffer :as buf]
            [superv.async :refer [go-try-]])
  (:import [java.io File]
           [java.nio.charset StandardCharsets]))

(set! *warn-on-reflection* true)

;;; Key encoding - use our fast buffer encoder

(defn- encode-key
  "Encode store key to bytes for LMDB."
  ^bytes [key]
  (buf/encode key))

(defn- decode-key
  "Decode bytes to store key."
  [^bytes key-bytes]
  (buf/decode key-bytes))

;;; Helper for registry-aware encode/decode

(defn- store-encode
  "Encode value using store's type handlers if available, else global."
  ^bytes [store value]
  (if-let [registry (:type-handlers store)]
    (buf/encode-with-registry registry value)
    (buf/encode value)))

(defn- store-decode
  "Decode value using store's type handlers if available, else global."
  [store ^bytes data-bytes]
  (if-let [registry (:type-handlers store)]
    (buf/decode-with-registry registry data-bytes)
    (buf/decode data-bytes)))

(defn- make-buf-decode-fn
  "Create a ByteBuffer decode function for use with get-decode-in-txn."
  [store]
  (if-let [registry (:type-handlers store)]
    (fn [buf] (buf/decode-from* registry buf))
    buf/decode-from))

;;; Store record

(defrecord LMDBStore [env ^String path locks write-hooks type-handlers]
  p/PEDNKeyValueStore
  (-exists? [_ key opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (some? (n/lmdb-get env (encode-key key))))))

  (-get-meta [this key opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (when-let [data-bytes (n/lmdb-get env (encode-key key))]
                   (:meta (store-decode this data-bytes))))))

  (-get-in [this key-vec not-found opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [key (first key-vec)
                       path (rest key-vec)]
                   (if-let [data-bytes (n/lmdb-get env (encode-key key))]
                     (let [data (store-decode this data-bytes)]
                       ;; Check for Direct API data (missing :meta key)
                       (when-not (contains? data :meta)
                         (throw (ex-info "Data was written with Direct API (store/put), not konserve API (k/assoc). Use store/get to read it, or re-write with k/assoc."
                                         {:key key :data-keys (keys data)})))
                       (let [value (:value data)]
                         (if (seq path)
                           (get-in value path not-found)
                           (if (nil? value) not-found value))))
                     not-found)))))

  (-assoc-in [this key-vec meta-up-fn val opts]
    ;; Uses write transaction for atomic read-modify-write
    ;; LMDB serializes write transactions, providing atomicity without locks
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [key (first key-vec)
                       path (rest key-vec)
                       key-bytes (encode-key key)
                       decode-fn (make-buf-decode-fn this)]
                   (n/with-write-txn [txn env]
                     (let [old-data (n/get-decode-in-txn txn key-bytes decode-fn)
                           old-meta (:meta old-data)
                           old-value (:value old-data)
                           ;; meta-up-fn is (partial meta-update key :edn) which takes [old]
                           new-meta (if meta-up-fn (meta-up-fn old-meta) old-meta)
                           new-value (if (seq path)
                                       (assoc-in old-value path val)
                                       val)
                           new-data {:meta new-meta
                                     :value new-value}]
                       (n/put-in-txn txn key-bytes (store-encode this new-data))
                       [old-value new-value]))))))

  (-update-in [this key-vec meta-up-fn up-fn opts]
    ;; Uses write transaction for atomic read-modify-write
    ;; LMDB serializes write transactions, providing atomicity without locks
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [key (first key-vec)
                       path (rest key-vec)
                       key-bytes (encode-key key)
                       decode-fn (make-buf-decode-fn this)]
                   (n/with-write-txn [txn env]
                     (let [old-data (n/get-decode-in-txn txn key-bytes decode-fn)
                           old-meta (:meta old-data)
                           old-value (:value old-data)
                           target-val (if (seq path)
                                        (get-in old-value path)
                                        old-value)
                           new-target (if up-fn (up-fn target-val) target-val)
                           ;; meta-up-fn is (partial meta-update key :edn) which takes [old]
                           new-meta (if meta-up-fn (meta-up-fn old-meta) old-meta)
                           new-value (if (seq path)
                                       (assoc-in old-value path new-target)
                                       new-target)
                           new-data {:meta new-meta
                                     :value new-value}]
                       (n/put-in-txn txn key-bytes (store-encode this new-data))
                       [old-value new-value]))))))

  (-dissoc [_ key opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (n/lmdb-del env (encode-key key)))))

  p/PMultiKeySupport
  (-supports-multi-key? [_] true)

  p/PMultiKeyEDNValueStore
  (-multi-get [this keys opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (reduce (fn [acc key]
                           (if-let [data-bytes (n/lmdb-get env (encode-key key))]
                             (assoc acc key (:value (store-decode this data-bytes)))
                             acc))
                         {}
                         keys))))

  (-multi-assoc [this kvs meta-up-fn opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [kv-pairs (mapv (fn [[key val]]
                                        (let [key-bytes (encode-key key)
                                              old-data (when-let [bs (n/lmdb-get env key-bytes)]
                                                         (store-decode this bs))
                                              old-meta (:meta old-data)
                                              ;; meta-up-fn is konserve.utils/meta-update which takes [key type old]
                                              ;; type should be :edn (keyword), not actual Class
                                              new-meta (if meta-up-fn
                                                         (meta-up-fn key :edn old-meta)
                                                         old-meta)
                                              new-data {:meta new-meta
                                                        :value val}]
                                          [key-bytes (store-encode this new-data)]))
                                      kvs)]
                   (n/lmdb-multi-put env kv-pairs)
                   (into {} (map (fn [[k _]] [k true]) kvs))))))

  (-multi-dissoc [_ keys opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [key-bytes-vec (mapv encode-key keys)
                       results (n/lmdb-multi-del env key-bytes-vec)]
                   ;; Map back to original keys
                   (into {} (map-indexed (fn [i key]
                                           [key (clojure.core/get results (nth key-bytes-vec i) false)])
                                         keys))))))

  p/PKeyIterable
  (-keys [_this opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [all-keys (n/lmdb-keys env)]
                   (->> all-keys
                        (map (fn [^bytes kb]
                               (let [key (decode-key kb)
                                     ;; Only decode metadata, skip the value blob for efficiency
                                     ;; This is much faster for GC enumeration with large values
                                     meta (when-let [bs (n/lmdb-get env kb)]
                                            (buf/decode-meta-only-from-bytes bs))]
                                 ;; Return key info map like konserve does
                                 {:key key
                                  :type (or (:type meta) :edn)
                                  :last-write (:last-write meta)})))
                        ;; Filter out internal append-log entry keys (UUIDs with :append-log type)
                        ;; These are internal to the append log implementation
                        (remove #(and (uuid? (:key %))
                                      (= :append-log (:type %))))
                        vec)))))

  p/PBinaryKeyValueStore
  (-bget [this key locked-cb opts]
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (when-let [data-bytes (n/lmdb-get env (encode-key key))]
                   (let [data (store-decode this data-bytes)
                         value (:value data)]
                     (when (bytes? value)
                       (locked-cb {:input-stream (java.io.ByteArrayInputStream. ^bytes value)
                                   :size (alength ^bytes value)})))))))

  (-bassoc [this key meta-up-fn val opts]
    ;; Uses write transaction for atomic read-modify-write (for metadata)
    (async+sync (:sync? opts) *default-sync-translation*
                (go-try-
                 (let [key-bytes (encode-key key)
                       ;; Convert various input types to byte array
                       ^bytes bytes-val (cond
                                          (bytes? val) val
                                          (string? val) (.getBytes ^String val StandardCharsets/UTF_8)
                                          (instance? java.io.InputStream val)
                                          (.readAllBytes ^java.io.InputStream val)
                                          (instance? java.io.File val)
                                          (java.nio.file.Files/readAllBytes (.toPath ^java.io.File val))
                                          :else (throw (ex-info "Unsupported binary type" {:type (type val)})))
                       decode-fn (make-buf-decode-fn this)]
                   (n/with-write-txn [txn env]
                     (let [old-data (n/get-decode-in-txn txn key-bytes decode-fn)
                           old-meta (:meta old-data)
                           old-value (:value old-data)
                           ;; meta-up-fn is (partial meta-update key :binary) which takes [old]
                           new-meta (if meta-up-fn (meta-up-fn old-meta) old-meta)
                           new-data {:meta new-meta
                                     :value bytes-val}]
                       (n/put-in-txn txn key-bytes (store-encode this new-data))
                       [old-value bytes-val]))))))

  p/PWriteHookStore
  (-get-write-hooks [_] write-hooks)
  (-set-write-hooks! [this hooks-atom]
    (assoc this :write-hooks hooks-atom))

  p/PLockFreeStore
  (-lock-free? [_] true))

;;; Store creation

(def ^:const +default-map-size+
  "Default LMDB map size: 1GB"
  (* 1024 1024 1024))

(defn connect-store
  "Connect to an LMDB store implementing konserve protocols directly.

   This bypasses DefaultStore and Fressian - uses buffer encoder directly.

   LMDB provides native transaction support, so this store is always lock-free:
   - Read operations use MVCC (multiple concurrent readers)
   - Write operations use LMDB write transactions for atomicity
   - No application-level locks needed

   Arguments:
     path - Directory path for LMDB data files

   Options:
     :map-size - LMDB map size in bytes (default 1GB)
     :flags - LMDB environment flags (default 0). Common flags:
              n/MDB_RDONLY - Read-only mode, concurrent reading while another process writes
              n/MDB_NORDAHEAD - Don't use read-ahead, reduces memory pressure
              n/MDB_NOSYNC - Don't fsync after commit (faster but less durable)
              n/MDB_WRITEMAP - Use writeable mmap (faster but less crash-safe)
              n/MDB_NOTLS - Disable thread-local storage for multi-threaded apps
     :type-handlers - TypeHandlerRegistry for custom type serialization.
                      Use buf/create-handler-registry to create one with
                      handlers that close over decode context (e.g., PSS settings).

   Example:
     (def store (connect-store \"/tmp/my-store\"))
     (k/assoc store :foo {:bar 42} {:sync? true})
     (k/get store :foo nil {:sync? true})

     ;; With custom type handlers:
     (def handlers (buf/create-handler-registry [my-handler] {:settings my-settings}))
     (def store (connect-store \"/tmp/my-store\" :type-handlers handlers))

     ;; With NORDAHEAD flag for large datasets:
     (def store (connect-store \"/tmp/big-store\" :flags n/MDB_NORDAHEAD))"
  [path & {:keys [map-size flags type-handlers]
           :or {map-size +default-map-size+
                flags 0}}]
  (let [dir (File. ^String path)]
    (when-not (.exists dir)
      (.mkdirs dir))
    (let [env (n/open-env path :map-size map-size :flags flags)]
      ;; locks is always nil - we use LMDB transactions for atomicity
      ;; konserve.core's maybe-go-locked will skip locking for lock-free stores
      (->LMDBStore env path nil (atom {}) type-handlers))))

(defn release-store
  "Release/close the LMDB store."
  [^LMDBStore store]
  (n/close-env (:env store)))

;;; Direct API - maximum performance, no konserve overhead
;;; Values stored directly without {:meta ... :value ...} wrapper.
;;; Use for datahike-style direct access where konserve metadata isn't needed.
;;;
;;; These functions:
;;; - Zero-copy decode from LMDB mmap
;;; - No async+sync wrapping, no go-try-
;;; - Single LMDB transaction for multi-* operations
;;; - Use store's type-handlers for custom types
;;; - NOT compatible with konserve.core API

(defn- make-decode-fn
  "Create decode function for direct API that uses store's handlers."
  [^LMDBStore store]
  (if-let [registry (:type-handlers store)]
    (fn [^java.nio.ByteBuffer buf]
      (buf/decode-from* registry buf))
    buf/decode-from))

(defn get
  "Get value by key. Zero-copy decode from LMDB mmap."
  [^LMDBStore store key]
  (let [env (:env store)
        key-bytes (encode-key key)
        decode-fn (make-decode-fn store)]
    (n/lmdb-get-decode env key-bytes decode-fn)))

(defn put
  "Store value at key. Returns the value."
  [^LMDBStore store key val]
  (let [env (:env store)
        key-bytes (encode-key key)
        encoded (if-let [registry (:type-handlers store)]
                  (buf/encode-with-registry registry val)
                  (buf/encode val))]
    (n/lmdb-put env key-bytes encoded)
    val))

(defn del
  "Delete key. Returns true if key existed."
  [^LMDBStore store key]
  (let [env (:env store)
        key-bytes (encode-key key)]
    (n/lmdb-del env key-bytes)))

(defn multi-get
  "Get multiple values in single transaction. Returns {key value} map."
  [^LMDBStore store keys]
  (let [env (:env store)
        registry (:type-handlers store)]
    (reduce (fn [acc key]
              (if-let [data-bytes (n/lmdb-get env (encode-key key))]
                (assoc acc key (if registry
                                 (buf/decode-with-registry registry data-bytes)
                                 (buf/decode data-bytes)))
                acc))
            {}
            keys)))

(defn multi-put
  "Store multiple key-value pairs in single transaction."
  [^LMDBStore store kvs]
  (let [env (:env store)
        registry (:type-handlers store)
        kv-pairs (mapv (fn [[key val]]
                         [(encode-key key)
                          (if registry
                            (buf/encode-with-registry registry val)
                            (buf/encode val))])
                       kvs)]
    (n/lmdb-multi-put env kv-pairs)
    kvs))

(defn delete-store
  "Delete an LMDB store and all its data."
  [^String path]
  (let [dir (File. path)]
    (when (.exists dir)
      (doseq [^File f (.listFiles dir)]
        (.delete f))
      (.delete dir))))

;; =============================================================================
;; Multimethod Registration for konserve.store dispatch
;; =============================================================================

(defmethod store/connect-store :lmdb
  [{:keys [path map-size flags type-handlers] :as config}]
  ;; Check if store exists
  (when-not (.exists (clojure.java.io/file path))
    (throw (ex-info (str "LMDB store does not exist at path: " path)
                    {:path path :config config})))
  ;; Build options for connect-store from config
  (let [opts (cond-> {}
               map-size (assoc :map-size map-size)
               flags (assoc :flags flags)
               type-handlers (assoc :type-handlers type-handlers))]
    (apply connect-store path (flatten (seq opts)))))

(defmethod store/create-store :lmdb
  [{:keys [path map-size flags type-handlers] :as config}]
  ;; Check if store already exists
  (when (.exists (clojure.java.io/file path))
    (throw (ex-info (str "LMDB store already exists at path: " path)
                    {:path path :config config})))
  ;; Build options and create store
  (let [opts (cond-> {}
               map-size (assoc :map-size map-size)
               flags (assoc :flags flags)
               type-handlers (assoc :type-handlers type-handlers))]
    (apply connect-store path (flatten (seq opts)))))

(defmethod store/store-exists? :lmdb
  [{:keys [path opts]}]
  ;; LMDB store exists if the directory exists
  (let [opts (or opts {:sync? true})
        exists (.exists (clojure.java.io/file path))]
    (if (:sync? opts) exists (clojure.core.async/go exists))))

(defmethod store/delete-store :lmdb
  [{:keys [path]}]
  (delete-store path))

(defmethod store/release-store :lmdb
  [_config store]
  (release-store store))
