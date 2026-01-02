(ns konserve-lmdb.store-test
  "Tests for direct LMDB store (bypasses DefaultStore)."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [konserve.core :as k]
            [konserve.utils :as utils]
            [konserve-lmdb.store :as s]
            [konserve-lmdb.buffer :as buf])
  (:import [java.io File]
           [java.nio ByteBuffer]
           [java.util UUID]))

(def ^:dynamic *test-store* nil)
(def ^:dynamic *test-path* nil)

(defn delete-dir [^File dir]
  (when (.exists dir)
    (doseq [f (.listFiles dir)]
      (.delete f))
    (.delete dir)))

(defn with-store [f]
  (let [path (str "/tmp/konserve-lmdb-store-test-" (UUID/randomUUID))]
    (try
      (let [store (s/connect-store path)]
        (binding [*test-store* store
                  *test-path* path]
          (f)))
      (finally
        (delete-dir (File. path))))))

(use-fixtures :each with-store)

(deftest test-basic-get-assoc
  (testing "Basic get and assoc"
    (is (nil? (k/get *test-store* :foo nil {:sync? true})))
    (k/assoc *test-store* :foo {:bar 42} {:sync? true})
    (is (= {:bar 42} (k/get *test-store* :foo nil {:sync? true})))))

(deftest test-get-in-assoc-in
  (testing "get-in and assoc-in"
    (k/assoc-in *test-store* [:config] {:db {:host "localhost" :port 5432}} {:sync? true})
    (is (= {:host "localhost" :port 5432}
           (k/get-in *test-store* [:config :db] nil {:sync? true})))
    (is (= "localhost"
           (k/get-in *test-store* [:config :db :host] nil {:sync? true})))))

(deftest test-update-in
  (testing "update-in"
    (k/assoc-in *test-store* [:counter] 0 {:sync? true})
    (k/update-in *test-store* [:counter] inc {:sync? true})
    (is (= 1 (k/get-in *test-store* [:counter] nil {:sync? true})))
    (k/update-in *test-store* [:counter] #(+ % 10) {:sync? true})
    (is (= 11 (k/get-in *test-store* [:counter] nil {:sync? true})))))

(deftest test-dissoc
  (testing "dissoc"
    (k/assoc *test-store* :to-delete "value" {:sync? true})
    (is (= "value" (k/get *test-store* :to-delete nil {:sync? true})))
    (k/dissoc *test-store* :to-delete {:sync? true})
    (is (nil? (k/get *test-store* :to-delete nil {:sync? true})))))

(deftest test-exists?
  (testing "exists?"
    (is (not (k/exists? *test-store* :new-key {:sync? true})))
    (k/assoc *test-store* :new-key "exists" {:sync? true})
    (is (k/exists? *test-store* :new-key {:sync? true}))))

(deftest test-keys
  (testing "keys returns all stored keys with metadata"
    (k/assoc *test-store* :a 1 {:sync? true})
    (k/assoc *test-store* :b 2 {:sync? true})
    (k/assoc *test-store* :c 3 {:sync? true})
    (let [ks (k/keys *test-store* {:sync? true})
          key-set (into #{} (map :key ks))]
      (is (= 3 (count ks)))
      (is (= #{:a :b :c} key-set))
      ;; Check metadata format
      (is (every? #(= :edn (:type %)) ks))
      (is (every? #(inst? (:last-write %)) ks)))))

(deftest test-various-value-types
  (testing "Store various value types"
    ;; String
    (k/assoc *test-store* :string "hello world" {:sync? true})
    (is (= "hello world" (k/get *test-store* :string nil {:sync? true})))

    ;; Long
    (k/assoc *test-store* :long 9223372036854775807 {:sync? true})
    (is (= 9223372036854775807 (k/get *test-store* :long nil {:sync? true})))

    ;; Double
    (k/assoc *test-store* :double 3.14159265359 {:sync? true})
    (is (= 3.14159265359 (k/get *test-store* :double nil {:sync? true})))

    ;; UUID
    (let [u (UUID/randomUUID)]
      (k/assoc *test-store* :uuid u {:sync? true})
      (is (= u (k/get *test-store* :uuid nil {:sync? true}))))

    ;; Nested map
    (k/assoc *test-store* :nested {:a {:b {:c [1 2 3]}}} {:sync? true})
    (is (= {:a {:b {:c [1 2 3]}}} (k/get *test-store* :nested nil {:sync? true})))

    ;; Set
    (k/assoc *test-store* :set #{:x :y :z} {:sync? true})
    (is (= #{:x :y :z} (k/get *test-store* :set nil {:sync? true})))))

(deftest test-persistence
  (testing "Data persists after reopening store"
    ;; Store some data
    (k/assoc *test-store* :persistent "I should persist" {:sync? true})
    (s/release-store *test-store*)

    ;; Reopen the store
    (let [store2 (s/connect-store *test-path*)]
      (try
        (is (= "I should persist" (k/get store2 :persistent nil {:sync? true})))
        (finally
          (s/release-store store2))))))

(deftest test-multi-key-operations
  (testing "multi-assoc and multi-get"
    (is (utils/multi-key-capable? *test-store*))

    ;; multi-assoc
    (k/multi-assoc *test-store* {:x 1 :y 2 :z 3} {:sync? true})
    (is (= 1 (k/get *test-store* :x nil {:sync? true})))
    (is (= 2 (k/get *test-store* :y nil {:sync? true})))
    (is (= 3 (k/get *test-store* :z nil {:sync? true})))

    ;; multi-get
    (let [result (k/multi-get *test-store* [:x :y :z :not-found] {:sync? true})]
      (is (= 1 (get result :x)))
      (is (= 2 (get result :y)))
      (is (= 3 (get result :z)))
      (is (not (contains? result :not-found))))

    ;; multi-dissoc
    (k/multi-dissoc *test-store* [:x :z] {:sync? true})
    (is (nil? (k/get *test-store* :x nil {:sync? true})))
    (is (= 2 (k/get *test-store* :y nil {:sync? true})))
    (is (nil? (k/get *test-store* :z nil {:sync? true})))))

(deftest test-get-meta
  (testing "get-meta returns metadata"
    (k/assoc *test-store* :with-meta {:data "value"} {:sync? true})
    (let [meta (k/get-meta *test-store* :with-meta nil {:sync? true})]
      (is (map? meta))
      (is (= :with-meta (:key meta)))
      (is (inst? (:last-write meta))))))

;;; Custom Type Handler Tests with Direct Store

(defrecord Point [x y])

(deftest test-custom-type-handler-with-store
  (testing "Custom type handlers work with direct store"
    (let [point-handler (reify buf/ITypeHandler
                          (type-tag [_] 0x20)
                          (type-class [_] Point)
                          (encode-type [_ buffer p encode-fn]
                            (let [^ByteBuffer b buffer]
                              (.putLong b (long (:x p)))
                              (.putLong b (long (:y p)))))
                          (decode-type [_ buffer decode-fn]
                            (let [^ByteBuffer b buffer]
                              (->Point (.getLong b) (.getLong b)))))
          registry (buf/create-handler-registry [point-handler] {})
          ;; Create a new store with the type handler registry
          path (str "/tmp/konserve-lmdb-custom-type-test-" (UUID/randomUUID))
          store (s/connect-store path :type-handlers registry)]
      (try
        ;; Store Point via konserve API - this works because buffer encoder handles it
        (k/assoc store :point (->Point 100 200) {:sync? true})
        (let [result (k/get store :point nil {:sync? true})]
          (is (= 100 (:x result)))
          (is (= 200 (:y result)))
          (is (instance? Point result)))

        ;; Nested in collections
        (k/assoc store :points [(->Point 1 2) (->Point 3 4)] {:sync? true})
        (let [result (k/get store :points nil {:sync? true})]
          (is (= 2 (count result)))
          (is (instance? Point (first result))))

        (finally
          (s/release-store store)
          (delete-dir (java.io.File. path)))))))
