(ns konserve-lmdb.run-benchmark
  "Public benchmark comparing konserve-lmdb performance across API layers.

   Compares:
   - Native LMDB (raw FFI calls)
   - Direct API (store/put, store/get) - for high-performance use cases
   - Konserve API (k/assoc, k/get) - full konserve compatibility
   - Datalevin raw KV - external reference

   Run with:
     KONSERVE_LMDB_LIB=/path/to/liblmdb.so clojure -M:bench -m konserve-lmdb.run-benchmark"
  (:require [konserve-lmdb.native :as n]
            [konserve-lmdb.buffer :as buf]
            [konserve-lmdb.store :as store]
            [konserve.core :as k]
            [datalevin.binding.cpp]
            [datalevin.lmdb :as dl]
            [criterium.core :as crit]
            [clojure.java.io :as io])
  (:import [java.io File]
           [java.util UUID]))

(set! *warn-on-reflection* true)

;;; Helpers

(defn delete-dir [^File dir]
  (when (.exists dir)
    (doseq [^File f (.listFiles dir)]
      (when (.isDirectory f) (delete-dir f))
      (.delete f))
    (.delete dir)))

(defn fresh-dir [^String path]
  (let [dir (io/file path)]
    (delete-dir dir)
    (.mkdirs dir)
    path))

;;; Test data generation

(defn gen-value
  "Generate a test value (~50 bytes): map with UUID, timestamp, counter."
  [i]
  {:id (UUID/randomUUID)
   :ts (System/currentTimeMillis)
   :n (long i)
   :tag :benchmark})

(defn gen-test-data
  "Generate n test key-value pairs."
  [n]
  (into {} (map (fn [i] [(keyword (str "k" i)) (gen-value i)]) (range n))))

;;; Benchmark runners

(defn bench-and-report
  "Run criterium quick-bench and return mean time in microseconds."
  [label f]
  (println (str "  Benchmarking " label "..."))
  (let [results (crit/quick-benchmark* f {})
        mean-ns (* 1e9 (first (:mean results)))
        mean-us (/ mean-ns 1000.0)]
    mean-us))

;;; Native LMDB benchmarks

(defn bench-native-put [path kvs]
  (let [env (n/open-env path :map-size (* 512 1024 1024))
        kv-encoded (mapv (fn [[k v]] [(buf/encode k) (buf/encode v)]) kvs)]
    (try
      (let [mean-us (bench-and-report "native put"
                      #(doseq [[kb vb] kv-encoded]
                         (n/lmdb-put env kb vb)))]
        (/ (count kvs) (/ mean-us 1e6)))  ; ops/sec
      (finally
        (n/close-env env)))))

(defn bench-native-get [path kvs]
  (let [env (n/open-env path :map-size (* 512 1024 1024))
        keys-encoded (mapv (fn [[k _]] (buf/encode k)) kvs)]
    (try
      ;; Populate first
      (doseq [[k v] kvs]
        (n/lmdb-put env (buf/encode k) (buf/encode v)))
      (let [mean-us (bench-and-report "native get"
                      #(doseq [kb keys-encoded]
                         (when-let [bs (n/lmdb-get env kb)]
                           (buf/decode bs))))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (n/close-env env)))))

(defn bench-native-batch-put [path kvs]
  (let [env (n/open-env path :map-size (* 512 1024 1024))
        kv-encoded (mapv (fn [[k v]] [(buf/encode k) (buf/encode v)]) kvs)]
    (try
      (let [mean-us (bench-and-report "native batch"
                      #(n/lmdb-multi-put env kv-encoded))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (n/close-env env)))))

;;; Direct API benchmarks

(defn bench-direct-put [path kvs]
  (let [s (store/connect-store path)]
    (try
      (let [mean-us (bench-and-report "direct put"
                      #(doseq [[k v] kvs]
                         (store/put s k v)))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (store/release-store s)))))

(defn bench-direct-get [path kvs]
  (let [s (store/connect-store path)]
    (try
      ;; Populate first
      (doseq [[k v] kvs] (store/put s k v))
      (let [ks (keys kvs)
            mean-us (bench-and-report "direct get"
                      #(doseq [k ks]
                         (store/get s k)))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (store/release-store s)))))

(defn bench-direct-multi-put [path kvs]
  (let [s (store/connect-store path)]
    (try
      (let [mean-us (bench-and-report "direct multi-put"
                      #(store/multi-put s kvs))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (store/release-store s)))))

;;; Konserve API benchmarks

(defn bench-konserve-put [path kvs]
  (let [s (store/connect-store path)]
    (try
      (let [mean-us (bench-and-report "konserve put"
                      #(doseq [[k v] kvs]
                         (k/assoc s k v {:sync? true})))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (store/release-store s)))))

(defn bench-konserve-get [path kvs]
  (let [s (store/connect-store path)]
    (try
      ;; Populate first
      (doseq [[k v] kvs] (k/assoc s k v {:sync? true}))
      (let [ks (keys kvs)
            mean-us (bench-and-report "konserve get"
                      #(doseq [k ks]
                         (k/get s k nil {:sync? true})))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (store/release-store s)))))

(defn bench-konserve-multi-assoc [path kvs]
  (let [s (store/connect-store path)]
    (try
      (let [mean-us (bench-and-report "konserve multi-assoc"
                      #(k/multi-assoc s kvs {:sync? true}))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (store/release-store s)))))

;;; Datalevin benchmarks

(defn bench-datalevin-put [path kvs]
  (let [lmdb (dl/open-kv path {:mapsize 512})]
    (try
      (dl/open-dbi lmdb "bench")
      (let [mean-us (bench-and-report "datalevin put"
                      #(doseq [[k v] kvs]
                         (dl/transact-kv lmdb [[:put "bench" (name k) v :string :nippy]])))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (dl/close-kv lmdb)))))

(defn bench-datalevin-get [path kvs]
  (let [lmdb (dl/open-kv path {:mapsize 512})]
    (try
      (dl/open-dbi lmdb "bench")
      ;; Populate first
      (doseq [[k v] kvs]
        (dl/transact-kv lmdb [[:put "bench" (name k) v :string :nippy]]))
      (let [ks (keys kvs)
            mean-us (bench-and-report "datalevin get"
                      #(doseq [k ks]
                         (dl/get-value lmdb "bench" (name k) :string :nippy)))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (dl/close-kv lmdb)))))

(defn bench-datalevin-batch [path kvs]
  (let [lmdb (dl/open-kv path {:mapsize 512})]
    (try
      (dl/open-dbi lmdb "bench")
      (let [txs (mapv (fn [[k v]] [:put "bench" (name k) v :string :nippy]) kvs)
            mean-us (bench-and-report "datalevin batch"
                      #(dl/transact-kv lmdb txs))]
        (/ (count kvs) (/ mean-us 1e6)))
      (finally
        (dl/close-kv lmdb)))))

;;; Main benchmark runner

(defn format-ops [ops-per-sec]
  (cond
    (>= ops-per-sec 1e6) (format "%.2fM" (/ ops-per-sec 1e6))
    (>= ops-per-sec 1e3) (format "%.1fK" (/ ops-per-sec 1e3))
    :else (format "%.0f" ops-per-sec)))

(defn print-table-row [op native direct konserve datalevin]
  (println (format "| %-20s | %10s | %10s | %10s | %10s |"
                   op
                   (format-ops native)
                   (format-ops direct)
                   (format-ops konserve)
                   (format-ops datalevin))))

(defn run-benchmark
  "Run the full benchmark suite."
  [& {:keys [n] :or {n 1000}}]
  (println "")
  (println "╔══════════════════════════════════════════════════════════════════════╗")
  (println "║                    konserve-lmdb Benchmark Suite                     ║")
  (println "╚══════════════════════════════════════════════════════════════════════╝")
  (println "")
  (println (format "Entries: %d, Value size: ~50 bytes (map with UUID, timestamp, counter)" n))
  (println "")

  (let [test-data (gen-test-data n)
        base-path "/tmp/konserve-lmdb-bench"]

    ;; Warmup
    (println "Warming up JIT...")
    (let [warmup-data (gen-test-data 500)
          wp (fresh-dir (str base-path "-warmup"))]
      (let [s (store/connect-store wp)]
        (dotimes [_ 10]
          (doseq [[k v] warmup-data]
            (store/put s k v)
            (store/get s k)))
        (store/release-store s))
      (delete-dir (io/file wp)))
    (println "")

    ;; Run benchmarks
    (println "Running benchmarks (this may take a few minutes)...")
    (println "")

    ;; Single puts
    (println "=== Single Put Operations ===")
    (let [native-put (bench-native-put (fresh-dir (str base-path "-native")) test-data)
          direct-put (bench-direct-put (fresh-dir (str base-path "-direct")) test-data)
          konserve-put (bench-konserve-put (fresh-dir (str base-path "-konserve")) test-data)
          datalevin-put (bench-datalevin-put (fresh-dir (str base-path "-datalevin")) test-data)]
      (println "")

      ;; Single gets
      (println "=== Single Get Operations ===")
      (let [native-get (bench-native-get (fresh-dir (str base-path "-native")) test-data)
            direct-get (bench-direct-get (fresh-dir (str base-path "-direct")) test-data)
            konserve-get (bench-konserve-get (fresh-dir (str base-path "-konserve")) test-data)
            datalevin-get (bench-datalevin-get (fresh-dir (str base-path "-datalevin")) test-data)]
        (println "")

        ;; Batch puts
        (println "=== Batch Put Operations (single transaction) ===")
        (let [native-batch (bench-native-batch-put (fresh-dir (str base-path "-native")) test-data)
              direct-batch (bench-direct-multi-put (fresh-dir (str base-path "-direct")) test-data)
              konserve-batch (bench-konserve-multi-assoc (fresh-dir (str base-path "-konserve")) test-data)
              datalevin-batch (bench-datalevin-batch (fresh-dir (str base-path "-datalevin")) test-data)]
          (println "")

          ;; Results table
          (println "")
          (println "╔══════════════════════════════════════════════════════════════════════╗")
          (println "║                           RESULTS (ops/sec)                          ║")
          (println "╠══════════════════════════════════════════════════════════════════════╣")
          (println "")
          (println "| Operation            |     Native |     Direct |   Konserve |  Datalevin |")
          (println "|----------------------|------------|------------|------------|------------|")
          (print-table-row "Single Put" native-put direct-put konserve-put datalevin-put)
          (print-table-row "Single Get" native-get direct-get konserve-get datalevin-get)
          (print-table-row "Batch Put" native-batch direct-batch konserve-batch datalevin-batch)
          (println "")
          (println "Legend:")
          (println "  Native    - Raw LMDB FFI calls (baseline)")
          (println "  Direct    - store/put, store/get (no metadata, fastest API)")
          (println "  Konserve  - k/assoc, k/get (full konserve compatibility)")
          (println "  Datalevin - Raw KV API for comparison")
          (println "")
          (println "Notes:")
          (println "  - Direct API is designed for high-performance use cases (e.g., datahike)")
          (println "  - Konserve API adds metadata tracking (~1-2µs overhead per operation)")
          (println "  - All times measured with criterium for statistical accuracy")
          (println "")

          ;; Cleanup
          (doseq [suffix ["-native" "-direct" "-konserve" "-datalevin"]]
            (delete-dir (io/file (str base-path suffix)))))))))

(defn -main [& args]
  (let [n (if (seq args) (parse-long (first args)) 1000)]
    (run-benchmark :n n)
    (shutdown-agents)))
