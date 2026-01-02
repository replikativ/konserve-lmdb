(ns konserve-lmdb.compliance-test
  "Runs konserve's standard compliance test against LMDBStore."
  (:require [clojure.test :refer [deftest use-fixtures]]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve-lmdb.store :as s])
  (:import [java.io File]
           [java.util UUID]))

(def ^:dynamic *test-store* nil)

(defn delete-dir [^File dir]
  (when (.exists dir)
    (doseq [f (.listFiles dir)]
      (.delete f))
    (.delete dir)))

(defn with-store [f]
  (let [path (str "/tmp/konserve-lmdb-compliance-" (UUID/randomUUID))]
    (try
      (let [store (s/connect-store path)]
        (binding [*test-store* store]
          (f)))
      (finally
        (delete-dir (File. path))))))

(use-fixtures :each with-store)

(deftest lmdb-compliance-test
  (compliance-test *test-store*))
