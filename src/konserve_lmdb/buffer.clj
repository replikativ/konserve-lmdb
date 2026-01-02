(ns konserve-lmdb.buffer
  "Buffer pool management and simple buffer-based encoding.

   Provides:
   1. Buffer pooling for efficient ByteBuffer reuse
   2. Simple type-tagged encoding for zero-copy reads
   3. Per-store type handler registries for custom types

   Uses a simple format optimized for speed over size:
   - 1 byte type tag
   - type-specific encoding (fixed-size where possible)

   Type tag ranges:
   - 0x00-0x3F: Built-in types (nil, bool, long, string, arrays, etc.)
   - 0x40-0xFF: User-defined types (via per-store TypeHandlerRegistry)"
  (:import [java.nio ByteBuffer ByteOrder]
           [java.util UUID Date]
           [java.util.concurrent ConcurrentLinkedDeque]
           [java.nio.charset StandardCharsets]
           [java.math BigDecimal BigInteger]
           [clojure.lang BigInt Ratio]))

(set! *warn-on-reflection* true)

(def ^:const +default-buffer-size+
  "Default buffer size in bytes (64KB)"
  65536)

(def ^:const +max-buffer-size+
  "Maximum buffer size for pooling (1MB). Larger buffers are not pooled."
  (* 1024 1024))

(def ^:const +max-value-size+
  "Maximum value size for encoding (256MB). Values larger than this throw."
  (* 256 1024 1024))

(def ^:const +buffer-grow-factor+
  "Growth factor when buffer overflows."
  10)

;; Pool of reusable direct ByteBuffers
(defonce ^ConcurrentLinkedDeque buffer-pool (ConcurrentLinkedDeque.))

(defn get-buffer
  "Get a direct ByteBuffer of at least min-size bytes.
   Tries to reuse from pool, otherwise allocates new.
   Caller must call return-buffer when done.

   Thread-safe: checks .remove() return value to avoid race conditions
   where two threads could get the same buffer."
  ^ByteBuffer [^long min-size]
  (let [size (max min-size +default-buffer-size+)]
    (or (some (fn [^ByteBuffer bf]
                (when (and (>= (.capacity bf) size)
                           (.remove buffer-pool bf))  ; Only use if we successfully removed it
                  (.clear bf)
                  bf))
              buffer-pool)
        (ByteBuffer/allocateDirect size))))

(defn return-buffer
  "Return a buffer to the pool for reuse.
   Only pools buffers up to +max-buffer-size+."
  [^ByteBuffer buf]
  (when (and buf (<= (.capacity buf) +max-buffer-size+))
    (.offer buffer-pool buf)))

(defn clear-pool!
  "Clear all pooled buffers. Useful for testing."
  []
  (.clear buffer-pool))

;;; Extensible Type Handler System

(defprotocol ITypeHandler
  "Protocol for custom type handlers. Implement this to add support for new types.

   Example usage from datahike:
   ```clojure
   (defrecord DatomHandler []
     ITypeHandler
     (type-tag [_] 0x10)
     (type-class [_] datahike.datom.Datom)
     (encode-type [_ buf datom encode-fn]
       ;; encode-fn can be used for nested values
       (.putLong buf (.-e datom))
       (.putInt buf (.-a datom))
       (encode-fn buf (.-v datom))
       (.putLong buf (.-tx datom))
       (.put buf (if (.-added datom) (byte 1) (byte 0))))
     (decode-type [_ buf decode-fn]
       ;; decode-fn can be used for nested values
       (->Datom (.getLong buf) (.getInt buf) (decode-fn buf) (.getLong buf) (= 1 (.get buf)))))

   ;; Create per-store registry
   (def registry (create-handler-registry [(->DatomHandler)] {}))
   ```"
  (type-tag [this] "Return the byte tag for this type (must be >= 0x10)")
  (type-class [this] "Return the Class this handler handles")
  (encode-type [this buf value encode-fn] "Encode value to ByteBuffer. encode-fn for nested values.")
  (decode-type [this buf decode-fn] "Decode value from ByteBuffer. decode-fn for nested values."))

;;; Per-store Type Handler Registry
;;; Allows each store to have its own handlers with closed-over context

(defrecord TypeHandlerRegistry [by-tag by-class context]
  ;; context is a map that can contain {:settings ... :storage ...}
  ;; for use during decode
  )

(defn create-handler-registry
  "Create a new per-store handler registry with optional decode context.

   Arguments:
     handlers - sequence of ITypeHandler implementations
     context - map with decode context (e.g., {:settings ... :storage ...})"
  [handlers context]
  (let [by-tag (java.util.HashMap.)
        by-class (java.util.HashMap.)]
    (doseq [h handlers]
      (.put by-tag (Integer/valueOf (int (type-tag h))) h)
      (.put by-class (type-class h) h))
    (->TypeHandlerRegistry by-tag by-class context)))

(defn registry-get-handler-for-class
  "Get handler from registry by class."
  [^TypeHandlerRegistry registry ^Class clazz]
  (when registry
    (.get ^java.util.HashMap (:by-class registry) clazz)))

(defn registry-get-handler-for-tag
  "Get handler from registry by tag."
  [^TypeHandlerRegistry registry ^long tag]
  (when registry
    (.get ^java.util.HashMap (:by-tag registry) (Integer/valueOf (int tag)))))

;;; Type Tags

;; Core types (0x00-0x0F)
(def ^:const TAG_NIL     (byte 0x00))
(def ^:const TAG_FALSE   (byte 0x01))
(def ^:const TAG_TRUE    (byte 0x02))
(def ^:const TAG_LONG    (byte 0x03))
(def ^:const TAG_DOUBLE  (byte 0x04))
(def ^:const TAG_STRING  (byte 0x05))
(def ^:const TAG_KEYWORD (byte 0x06))
(def ^:const TAG_SYMBOL  (byte 0x07))
(def ^:const TAG_UUID    (byte 0x08))
(def ^:const TAG_INSTANT (byte 0x09))
(def ^:const TAG_BYTES   (byte 0x0A))
(def ^:const TAG_VECTOR  (byte 0x0B))
(def ^:const TAG_MAP     (byte 0x0C))
(def ^:const TAG_SET     (byte 0x0D))
(def ^:const TAG_SHORT   (byte 0x0E))
(def ^:const TAG_BYTE    (byte 0x0F))

;; Extended types (0x10-0x1F)
(def ^:const TAG_FLOAT      (byte 0x10))
(def ^:const TAG_CHAR       (byte 0x11))
(def ^:const TAG_BIGINT     (byte 0x12))
(def ^:const TAG_BIGDEC     (byte 0x13))
(def ^:const TAG_RATIO      (byte 0x14))
(def ^:const TAG_SHORT_ARRAY   (byte 0x15))
(def ^:const TAG_INT_ARRAY     (byte 0x16))
(def ^:const TAG_LONG_ARRAY    (byte 0x17))
(def ^:const TAG_FLOAT_ARRAY   (byte 0x18))
(def ^:const TAG_DOUBLE_ARRAY  (byte 0x19))
(def ^:const TAG_BOOLEAN_ARRAY (byte 0x1A))
(def ^:const TAG_CHAR_ARRAY    (byte 0x1B))
(def ^:const TAG_INT       (byte 0x1C))  ; Preserve Integer type (not promote to Long)

;;; Encoding (write to ByteBuffer)

(declare encode-to)

(defn- encode-string-bytes
  "Encode string bytes with length prefix."
  [^ByteBuffer buf ^bytes bs]
  (let [len (alength bs)]
    (.putInt buf len)
    (.put buf bs)))

(defn- encode-keyword-fast
  "Encode keyword without intermediate string allocation where possible."
  [^ByteBuffer buf kw]
  (.put buf TAG_KEYWORD)
  (if-let [ns (namespace kw)]
    (let [ns-bytes (.getBytes ^String ns StandardCharsets/UTF_8)
          name-bytes (.getBytes ^String (name kw) StandardCharsets/UTF_8)
          total-len (+ (alength ns-bytes) 1 (alength name-bytes))]
      (.putInt buf total-len)
      (.put buf ns-bytes)
      (.put buf (byte 0x2F)) ;; /
      (.put buf name-bytes))
    (let [name-bytes (.getBytes ^String (name kw) StandardCharsets/UTF_8)]
      (.putInt buf (alength name-bytes))
      (.put buf name-bytes)))
  buf)

(defn encode-to
  "Encode a value to a ByteBuffer. Returns the buffer.
   Only handles built-in types. For custom types, use encode-to* with a registry."
  ^ByteBuffer [^ByteBuffer buf value]
  (if (nil? value)
    (doto buf (.put TAG_NIL))
    (let [t (type value)]
      (cond
        ;; Primitive numerics
        (identical? t Long)
        (doto buf (.put TAG_LONG) (.putLong (long value)))

        (identical? t Integer)
        (doto buf (.put TAG_INT) (.putInt (int value)))

        (identical? t Short)
        (doto buf (.put TAG_SHORT) (.putShort (short value)))

        (identical? t Byte)
        (doto buf (.put TAG_BYTE) (.put (byte value)))

        (identical? t Double)
        (doto buf (.put TAG_DOUBLE) (.putDouble (double value)))

        (identical? t Float)
        (doto buf (.put TAG_FLOAT) (.putFloat (float value)))

        (identical? t Character)
        (doto buf (.put TAG_CHAR) (.putChar (char value)))

        (identical? t Boolean)
        (doto buf (.put (if value TAG_TRUE TAG_FALSE)))

        ;; Big numerics
        (identical? t BigInt)
        (let [^BigInteger bi (.toBigInteger ^BigInt value)
              bs (.toByteArray bi)]
          (.put buf TAG_BIGINT)
          (.putInt buf (alength bs))
          (.put buf bs)
          buf)

        (identical? t BigInteger)
        (let [bs (.toByteArray ^BigInteger value)]
          (.put buf TAG_BIGINT)
          (.putInt buf (alength bs))
          (.put buf bs)
          buf)

        (identical? t BigDecimal)
        (let [^BigDecimal bd value
              scale (.scale bd)
              ^BigInteger unscaled (.unscaledValue bd)
              bs (.toByteArray unscaled)]
          (.put buf TAG_BIGDEC)
          (.putInt buf scale)
          (.putInt buf (alength bs))
          (.put buf bs)
          buf)

        (identical? t Ratio)
        (let [^Ratio r value
              num-bytes (.toByteArray (.numerator r))
              den-bytes (.toByteArray (.denominator r))]
          (.put buf TAG_RATIO)
          (.putInt buf (alength num-bytes))
          (.put buf num-bytes)
          (.putInt buf (alength den-bytes))
          (.put buf den-bytes)
          buf)

        ;; String-like types
        (identical? t String)
        (let [bs (.getBytes ^String value StandardCharsets/UTF_8)]
          (.put buf TAG_STRING)
          (encode-string-bytes buf bs)
          buf)

        (identical? t clojure.lang.Keyword)
        (encode-keyword-fast buf value)

        (identical? t clojure.lang.Symbol)
        (let [s (if-let [ns (namespace value)]
                  (str ns "/" (name value))
                  (name value))
              bs (.getBytes s StandardCharsets/UTF_8)]
          (.put buf TAG_SYMBOL)
          (encode-string-bytes buf bs)
          buf)

        (identical? t UUID)
        (let [^UUID u value]
          (doto buf
            (.put TAG_UUID)
            (.putLong (.getMostSignificantBits u))
            (.putLong (.getLeastSignificantBits u))))

        ;; Primitive arrays
        (bytes? value)
        (let [^bytes bs value]
          (.put buf TAG_BYTES)
          (.putInt buf (alength bs))
          (.put buf bs)
          buf)

        (.isArray ^Class t)
        (let [component (.getComponentType ^Class t)]
          (cond
            (identical? component Short/TYPE)
            (let [^shorts arr value
                  len (alength arr)]
              (.put buf TAG_SHORT_ARRAY)
              (.putInt buf len)
              (dotimes [i len] (.putShort buf (aget arr i)))
              buf)

            (identical? component Integer/TYPE)
            (let [^ints arr value
                  len (alength arr)]
              (.put buf TAG_INT_ARRAY)
              (.putInt buf len)
              (dotimes [i len] (.putInt buf (aget arr i)))
              buf)

            (identical? component Long/TYPE)
            (let [^longs arr value
                  len (alength arr)]
              (.put buf TAG_LONG_ARRAY)
              (.putInt buf len)
              (dotimes [i len] (.putLong buf (aget arr i)))
              buf)

            (identical? component Float/TYPE)
            (let [^floats arr value
                  len (alength arr)]
              (.put buf TAG_FLOAT_ARRAY)
              (.putInt buf len)
              (dotimes [i len] (.putFloat buf (aget arr i)))
              buf)

            (identical? component Double/TYPE)
            (let [^doubles arr value
                  len (alength arr)]
              (.put buf TAG_DOUBLE_ARRAY)
              (.putInt buf len)
              (dotimes [i len] (.putDouble buf (aget arr i)))
              buf)

            (identical? component Boolean/TYPE)
            (let [^booleans arr value
                  len (alength arr)]
              (.put buf TAG_BOOLEAN_ARRAY)
              (.putInt buf len)
              (dotimes [i len] (.put buf (if (aget arr i) (byte 1) (byte 0))))
              buf)

            (identical? component Character/TYPE)
            (let [^chars arr value
                  len (alength arr)]
              (.put buf TAG_CHAR_ARRAY)
              (.putInt buf len)
              (dotimes [i len] (.putChar buf (aget arr i)))
              buf)

            :else
            (throw (ex-info "Unsupported array type for buffer encoding"
                            {:type t :component component}))))

        ;; Collections
        (identical? t clojure.lang.PersistentVector)
        (do
          (.put buf TAG_VECTOR)
          (.putInt buf (count value))
          (doseq [v value]
            (encode-to buf v))
          buf)

        (identical? t clojure.lang.PersistentArrayMap)
        (do
          (.put buf TAG_MAP)
          (.putInt buf (count value))
          (doseq [[k v] value]
            (encode-to buf k)
            (encode-to buf v))
          buf)

        (identical? t clojure.lang.PersistentHashMap)
        (do
          (.put buf TAG_MAP)
          (.putInt buf (count value))
          (doseq [[k v] value]
            (encode-to buf k)
            (encode-to buf v))
          buf)

        ;; Date/time
        (inst? value)
        (doto buf
          (.put TAG_INSTANT)
          (.putLong (.getTime ^Date value)))

        ;; Fallback to interface checks
        (set? value)
        (do
          (.put buf TAG_SET)
          (.putInt buf (count value))
          (doseq [v value]
            (encode-to buf v))
          buf)

        (sequential? value)
        (do
          (.put buf TAG_VECTOR)
          (.putInt buf (count value))
          (doseq [v value]
            (encode-to buf v))
          buf)

        (map? value)
        (do
          (.put buf TAG_MAP)
          (.putInt buf (count value))
          (doseq [[k v] value]
            (encode-to buf k)
            (encode-to buf v))
          buf)

        :else
        (throw (ex-info "Unsupported type for buffer encoding"
                        {:type t :value value}))))))

(defn encode
  "Encode a value to a new byte array.
   Uses pooled buffer for encoding, copies result to correctly-sized array.
   Automatically retries with larger buffer on overflow."
  ^bytes [value]
  (loop [size +default-buffer-size+]
    (let [^ByteBuffer buf (get-buffer size)
          result (try
                   (.order buf ByteOrder/BIG_ENDIAN)
                   (encode-to buf value)
                   (.flip buf)
                   (let [arr (byte-array (.remaining buf))]
                     (.get buf arr)
                     arr)
                   (catch java.nio.BufferOverflowException _
                     ::overflow)
                   (finally
                     (.clear buf)
                     (return-buffer buf)))]
      (if (= result ::overflow)
        (let [new-size (* +buffer-grow-factor+ size)]
          (if (> new-size +max-value-size+)
            (throw (ex-info "Value too large for buffer encoding"
                            {:size size :max +max-value-size+}))
            (recur new-size)))
        result))))

;;; Decoding (read from ByteBuffer)

(declare decode-from)

(defn- decode-string
  "Decode a string from buffer."
  ^String [^ByteBuffer buf]
  (let [len (.getInt buf)
        bs (byte-array len)]
    (.get buf bs)
    (String. bs StandardCharsets/UTF_8)))

(defn decode-from
  "Decode a value from a ByteBuffer. Zero-copy where possible.
   Only handles built-in types. For custom types, use decode-from* with a registry."
  [^ByteBuffer buf]
  (let [tag (int (.get buf))]
    (case tag
      ;; Core types (0x00-0x0F)
      0 nil ;; TAG_NIL
      1 false ;; TAG_FALSE
      2 true ;; TAG_TRUE
      3 (.getLong buf) ;; TAG_LONG
      4 (.getDouble buf) ;; TAG_DOUBLE
      5 (decode-string buf) ;; TAG_STRING
      6 (keyword (decode-string buf)) ;; TAG_KEYWORD
      7 (symbol (decode-string buf)) ;; TAG_SYMBOL
      8 (UUID. (.getLong buf) (.getLong buf)) ;; TAG_UUID
      9 (Date. (.getLong buf)) ;; TAG_INSTANT
      10 (let [len (.getInt buf) ;; TAG_BYTES
               bs (byte-array len)]
           (.get buf bs)
           bs)
      11 (let [n (.getInt buf)] ;; TAG_VECTOR
           (loop [i 0 acc (transient [])]
             (if (< i n)
               (recur (inc i) (conj! acc (decode-from buf)))
               (persistent! acc))))
      12 (let [n (.getInt buf)] ;; TAG_MAP
           (loop [i 0 acc (transient {})]
             (if (< i n)
               (let [k (decode-from buf)
                     v (decode-from buf)]
                 (recur (inc i) (assoc! acc k v)))
               (persistent! acc))))
      13 (let [n (.getInt buf)] ;; TAG_SET
           (loop [i 0 acc (transient #{})]
             (if (< i n)
               (recur (inc i) (conj! acc (decode-from buf)))
               (persistent! acc))))
      14 (Short/valueOf (.getShort buf)) ;; TAG_SHORT
      15 (Byte/valueOf (.get buf)) ;; TAG_BYTE

      ;; Extended types (0x10-0x1F)
      16 (Float/valueOf (.getFloat buf)) ;; TAG_FLOAT
      17 (Character/valueOf (.getChar buf)) ;; TAG_CHAR
      18 (let [len (.getInt buf) ;; TAG_BIGINT
               bs (byte-array len)]
           (.get buf bs)
           (bigint (BigInteger. bs)))
      19 (let [scale (.getInt buf) ;; TAG_BIGDEC
               len (.getInt buf)
               bs (byte-array len)]
           (.get buf bs)
           (BigDecimal. (BigInteger. bs) scale))
      20 (let [num-len (.getInt buf) ;; TAG_RATIO
               num-bs (byte-array num-len)
               _ (.get buf num-bs)
               den-len (.getInt buf)
               den-bs (byte-array den-len)]
           (.get buf den-bs)
           (/ (bigint (BigInteger. num-bs))
              (bigint (BigInteger. den-bs))))
      21 (let [len (.getInt buf) ;; TAG_SHORT_ARRAY
               arr (short-array len)]
           (dotimes [i len] (aset arr i (.getShort buf)))
           arr)
      22 (let [len (.getInt buf) ;; TAG_INT_ARRAY
               arr (int-array len)]
           (dotimes [i len] (aset arr i (.getInt buf)))
           arr)
      23 (let [len (.getInt buf) ;; TAG_LONG_ARRAY
               arr (long-array len)]
           (dotimes [i len] (aset arr i (.getLong buf)))
           arr)
      24 (let [len (.getInt buf) ;; TAG_FLOAT_ARRAY
               arr (float-array len)]
           (dotimes [i len] (aset arr i (.getFloat buf)))
           arr)
      25 (let [len (.getInt buf) ;; TAG_DOUBLE_ARRAY
               arr (double-array len)]
           (dotimes [i len] (aset arr i (.getDouble buf)))
           arr)
      26 (let [len (.getInt buf) ;; TAG_BOOLEAN_ARRAY
               arr (boolean-array len)]
           (dotimes [i len] (aset arr i (= 1 (.get buf))))
           arr)
      27 (let [len (.getInt buf) ;; TAG_CHAR_ARRAY
               arr (char-array len)]
           (dotimes [i len] (aset arr i (.getChar buf)))
           arr)
      28 (Integer/valueOf (.getInt buf)) ;; TAG_INT

      ;; Unknown tag - for custom types use decode-from* with a registry
      (throw (ex-info "Unknown type tag. Use decode-from* with a registry for custom types."
                      {:tag tag})))))

(defn decode
  "Decode a value from a byte array."
  [^bytes arr]
  (decode-from (-> (ByteBuffer/wrap arr)
                   (.order ByteOrder/BIG_ENDIAN))))

(defn decode-meta-only
  "Decode only the :meta entry from a stored {:meta ... :value ...} map.
   Skips decoding the :value blob entirely - much faster for GC enumeration
   where we only need metadata to decide what to delete.

   Returns the metadata map, or nil if not found or invalid format."
  [^ByteBuffer buf]
  (let [tag (.get buf)]
    (when (= tag TAG_MAP)
      (let [n (.getInt buf)]
        (when (pos? n)
          ;; Decode entries until we find :meta
          ;; For our format {:meta m :value v}, :meta is always first (ArrayMap order)
          (loop [i 0]
            (when (< i n)
              (let [k (decode-from buf)
                    v (decode-from buf)]
                (if (= k :meta)
                  v
                  (recur (inc i)))))))))))

(defn decode-meta-only-from-bytes
  "Decode only the :meta entry from a byte array.
   See decode-meta-only for details."
  [^bytes arr]
  (decode-meta-only (-> (ByteBuffer/wrap arr)
                        (.order ByteOrder/BIG_ENDIAN))))

;;; Registry-aware encode/decode
;;; These use per-store handlers instead of global registry

(declare encode-to*)

(defn- make-encode-fn
  "Create an encode function that uses the given registry."
  [registry]
  (fn [buf value]
    (encode-to* registry buf value)))

(defn encode-to*
  "Encode a value to a ByteBuffer using per-store registry."
  ^ByteBuffer [registry ^ByteBuffer buf value]
  (let [encode-fn (make-encode-fn registry)]
    (if (nil? value)
      (doto buf (.put TAG_NIL))
      (let [^Class t (type value)]
        ;; Check per-store registry for custom type handler
        (if-let [handler (registry-get-handler-for-class registry t)]
          (do
            (.put buf (byte (type-tag handler)))
            (encode-type handler buf value encode-fn)
            buf)
          (cond
            ;; Primitive numerics
            (identical? t Long)
            (doto buf (.put TAG_LONG) (.putLong (long value)))

            (identical? t Integer)
            (doto buf (.put TAG_INT) (.putInt (int value)))

            (identical? t Short)
            (doto buf (.put TAG_SHORT) (.putShort (short value)))

            (identical? t Byte)
            (doto buf (.put TAG_BYTE) (.put (byte value)))

            (identical? t Double)
            (doto buf (.put TAG_DOUBLE) (.putDouble (double value)))

            (identical? t Float)
            (doto buf (.put TAG_FLOAT) (.putFloat (float value)))

            (identical? t Character)
            (doto buf (.put TAG_CHAR) (.putChar (char value)))

            (identical? t Boolean)
            (doto buf (.put (if value TAG_TRUE TAG_FALSE)))

            ;; Big numerics
            (identical? t BigInt)
            (let [^BigInteger bi (.toBigInteger ^BigInt value)
                  bs (.toByteArray bi)]
              (.put buf TAG_BIGINT)
              (.putInt buf (alength bs))
              (.put buf bs)
              buf)

            (identical? t BigInteger)
            (let [bs (.toByteArray ^BigInteger value)]
              (.put buf TAG_BIGINT)
              (.putInt buf (alength bs))
              (.put buf bs)
              buf)

            (identical? t BigDecimal)
            (let [^BigDecimal bd value
                  scale (.scale bd)
                  ^BigInteger unscaled (.unscaledValue bd)
                  bs (.toByteArray unscaled)]
              (.put buf TAG_BIGDEC)
              (.putInt buf scale)
              (.putInt buf (alength bs))
              (.put buf bs)
              buf)

            (identical? t Ratio)
            (let [^Ratio r value
                  num-bytes (.toByteArray (.numerator r))
                  den-bytes (.toByteArray (.denominator r))]
              (.put buf TAG_RATIO)
              (.putInt buf (alength num-bytes))
              (.put buf num-bytes)
              (.putInt buf (alength den-bytes))
              (.put buf den-bytes)
              buf)

            ;; String-like types
            (identical? t String)
            (let [bs (.getBytes ^String value StandardCharsets/UTF_8)]
              (.put buf TAG_STRING)
              (encode-string-bytes buf bs)
              buf)

            (identical? t clojure.lang.Keyword)
            (encode-keyword-fast buf value)

            (identical? t clojure.lang.Symbol)
            (let [s (if-let [ns (namespace value)]
                      (str ns "/" (name value))
                      (name value))
                  bs (.getBytes s StandardCharsets/UTF_8)]
              (.put buf TAG_SYMBOL)
              (encode-string-bytes buf bs)
              buf)

            (identical? t UUID)
            (let [^UUID u value]
              (doto buf
                (.put TAG_UUID)
                (.putLong (.getMostSignificantBits u))
                (.putLong (.getLeastSignificantBits u))))

            ;; Primitive arrays
            (bytes? value)
            (let [^bytes bs value]
              (.put buf TAG_BYTES)
              (.putInt buf (alength bs))
              (.put buf bs)
              buf)

            (.isArray t)
            (let [component (.getComponentType t)]
              (cond
                (identical? component Short/TYPE)
                (let [^shorts arr value
                      len (alength arr)]
                  (.put buf TAG_SHORT_ARRAY)
                  (.putInt buf len)
                  (dotimes [i len] (.putShort buf (aget arr i)))
                  buf)

                (identical? component Integer/TYPE)
                (let [^ints arr value
                      len (alength arr)]
                  (.put buf TAG_INT_ARRAY)
                  (.putInt buf len)
                  (dotimes [i len] (.putInt buf (aget arr i)))
                  buf)

                (identical? component Long/TYPE)
                (let [^longs arr value
                      len (alength arr)]
                  (.put buf TAG_LONG_ARRAY)
                  (.putInt buf len)
                  (dotimes [i len] (.putLong buf (aget arr i)))
                  buf)

                (identical? component Float/TYPE)
                (let [^floats arr value
                      len (alength arr)]
                  (.put buf TAG_FLOAT_ARRAY)
                  (.putInt buf len)
                  (dotimes [i len] (.putFloat buf (aget arr i)))
                  buf)

                (identical? component Double/TYPE)
                (let [^doubles arr value
                      len (alength arr)]
                  (.put buf TAG_DOUBLE_ARRAY)
                  (.putInt buf len)
                  (dotimes [i len] (.putDouble buf (aget arr i)))
                  buf)

                (identical? component Boolean/TYPE)
                (let [^booleans arr value
                      len (alength arr)]
                  (.put buf TAG_BOOLEAN_ARRAY)
                  (.putInt buf len)
                  (dotimes [i len] (.put buf (if (aget arr i) (byte 1) (byte 0))))
                  buf)

                (identical? component Character/TYPE)
                (let [^chars arr value
                      len (alength arr)]
                  (.put buf TAG_CHAR_ARRAY)
                  (.putInt buf len)
                  (dotimes [i len] (.putChar buf (aget arr i)))
                  buf)

                :else
                (throw (ex-info "Unsupported array type for buffer encoding"
                                {:type t :component component}))))

            ;; Collections
            (identical? t clojure.lang.PersistentVector)
            (do
              (.put buf TAG_VECTOR)
              (.putInt buf (count value))
              (doseq [v value]
                (encode-fn buf v))
              buf)

            (identical? t clojure.lang.PersistentArrayMap)
            (do
              (.put buf TAG_MAP)
              (.putInt buf (count value))
              (doseq [[k v] value]
                (encode-fn buf k)
                (encode-fn buf v))
              buf)

            (identical? t clojure.lang.PersistentHashMap)
            (do
              (.put buf TAG_MAP)
              (.putInt buf (count value))
              (doseq [[k v] value]
                (encode-fn buf k)
                (encode-fn buf v))
              buf)

            ;; Date/time
            (inst? value)
            (doto buf
              (.put TAG_INSTANT)
              (.putLong (.getTime ^Date value)))

            ;; Fallback to interface checks
            (set? value)
            (do
              (.put buf TAG_SET)
              (.putInt buf (count value))
              (doseq [v value]
                (encode-fn buf v))
              buf)

            (sequential? value)
            (do
              (.put buf TAG_VECTOR)
              (.putInt buf (count value))
              (doseq [v value]
                (encode-fn buf v))
              buf)

            (map? value)
            (do
              (.put buf TAG_MAP)
              (.putInt buf (count value))
              (doseq [[k v] value]
                (encode-fn buf k)
                (encode-fn buf v))
              buf)

            :else
            (throw (ex-info "Unsupported type for buffer encoding"
                            {:type t :value value}))))))))

(defn encode-with-registry
  "Encode a value to a new byte array using per-store registry.
   Automatically retries with larger buffer on overflow."
  ^bytes [registry value]
  (loop [size +default-buffer-size+]
    (let [^ByteBuffer buf (get-buffer size)
          result (try
                   (.order buf ByteOrder/BIG_ENDIAN)
                   (encode-to* registry buf value)
                   (.flip buf)
                   (let [arr (byte-array (.remaining buf))]
                     (.get buf arr)
                     arr)
                   (catch java.nio.BufferOverflowException _
                     ::overflow)
                   (finally
                     (.clear buf)
                     (return-buffer buf)))]
      (if (= result ::overflow)
        (let [new-size (* +buffer-grow-factor+ size)]
          (if (> new-size +max-value-size+)
            (throw (ex-info "Value too large for buffer encoding"
                            {:size size :max +max-value-size+}))
            (recur new-size)))
        result))))

(declare decode-from*)

(defn- make-decode-fn
  "Create a decode function that uses the given registry."
  [registry]
  (fn [buf]
    (decode-from* registry buf)))

(defn decode-from*
  "Decode a value from ByteBuffer using per-store registry."
  [registry ^ByteBuffer buf]
  (let [decode-fn (make-decode-fn registry)
        tag (int (.get buf))]
    (case tag
      ;; Core types (0x00-0x0F)
      0 nil ;; TAG_NIL
      1 false ;; TAG_FALSE
      2 true ;; TAG_TRUE
      3 (.getLong buf) ;; TAG_LONG
      4 (.getDouble buf) ;; TAG_DOUBLE
      5 (decode-string buf) ;; TAG_STRING
      6 (keyword (decode-string buf)) ;; TAG_KEYWORD
      7 (symbol (decode-string buf)) ;; TAG_SYMBOL
      8 (UUID. (.getLong buf) (.getLong buf)) ;; TAG_UUID
      9 (Date. (.getLong buf)) ;; TAG_INSTANT
      10 (let [len (.getInt buf) ;; TAG_BYTES
               bs (byte-array len)]
           (.get buf bs)
           bs)
      11 (let [n (.getInt buf)] ;; TAG_VECTOR
           (loop [i 0 acc (transient [])]
             (if (< i n)
               (recur (inc i) (conj! acc (decode-fn buf)))
               (persistent! acc))))
      12 (let [n (.getInt buf)] ;; TAG_MAP
           (loop [i 0 acc (transient {})]
             (if (< i n)
               (let [k (decode-fn buf)
                     v (decode-fn buf)]
                 (recur (inc i) (assoc! acc k v)))
               (persistent! acc))))
      13 (let [n (.getInt buf)] ;; TAG_SET
           (loop [i 0 acc (transient #{})]
             (if (< i n)
               (recur (inc i) (conj! acc (decode-fn buf)))
               (persistent! acc))))
      14 (Short/valueOf (.getShort buf)) ;; TAG_SHORT
      15 (Byte/valueOf (.get buf)) ;; TAG_BYTE

      ;; Extended types (0x10-0x1F)
      16 (Float/valueOf (.getFloat buf)) ;; TAG_FLOAT
      17 (Character/valueOf (.getChar buf)) ;; TAG_CHAR
      18 (let [len (.getInt buf) ;; TAG_BIGINT
               bs (byte-array len)]
           (.get buf bs)
           (bigint (BigInteger. bs)))
      19 (let [scale (.getInt buf) ;; TAG_BIGDEC
               len (.getInt buf)
               bs (byte-array len)]
           (.get buf bs)
           (BigDecimal. (BigInteger. bs) scale))
      20 (let [num-len (.getInt buf) ;; TAG_RATIO
               num-bs (byte-array num-len)
               _ (.get buf num-bs)
               den-len (.getInt buf)
               den-bs (byte-array den-len)]
           (.get buf den-bs)
           (/ (bigint (BigInteger. num-bs))
              (bigint (BigInteger. den-bs))))
      21 (let [len (.getInt buf) ;; TAG_SHORT_ARRAY
               arr (short-array len)]
           (dotimes [i len] (aset arr i (.getShort buf)))
           arr)
      22 (let [len (.getInt buf) ;; TAG_INT_ARRAY
               arr (int-array len)]
           (dotimes [i len] (aset arr i (.getInt buf)))
           arr)
      23 (let [len (.getInt buf) ;; TAG_LONG_ARRAY
               arr (long-array len)]
           (dotimes [i len] (aset arr i (.getLong buf)))
           arr)
      24 (let [len (.getInt buf) ;; TAG_FLOAT_ARRAY
               arr (float-array len)]
           (dotimes [i len] (aset arr i (.getFloat buf)))
           arr)
      25 (let [len (.getInt buf) ;; TAG_DOUBLE_ARRAY
               arr (double-array len)]
           (dotimes [i len] (aset arr i (.getDouble buf)))
           arr)
      26 (let [len (.getInt buf) ;; TAG_BOOLEAN_ARRAY
               arr (boolean-array len)]
           (dotimes [i len] (aset arr i (= 1 (.get buf))))
           arr)
      27 (let [len (.getInt buf) ;; TAG_CHAR_ARRAY
               arr (char-array len)]
           (dotimes [i len] (aset arr i (.getChar buf)))
           arr)
      28 (Integer/valueOf (.getInt buf)) ;; TAG_INT

      ;; Check per-store registry for custom type (0x40+)
      (if-let [handler (registry-get-handler-for-tag registry tag)]
        (decode-type handler buf decode-fn)
        (throw (ex-info "Unknown type tag" {:tag tag}))))))

(defn decode-with-registry
  "Decode a value from a byte array using per-store registry."
  [registry ^bytes arr]
  (decode-from* registry (-> (ByteBuffer/wrap arr)
                             (.order ByteOrder/BIG_ENDIAN))))

(comment
  ;; Test encoding/decoding
  (def test-data {:e 42 :a :name :v "hello" :tx 1000000})
  (def encoded (encode test-data))
  (count encoded) ;; bytes
  (decode encoded) ;; => {:e 42, :a :name, :v "hello", :tx 1000000}

  ;; Round-trip test
  (= test-data (decode (encode test-data))) ;; => true
  )
