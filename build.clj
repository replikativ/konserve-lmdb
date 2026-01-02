(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'io.replikativ/konserve-lmdb)
(def version (format "0.1.%s" (b/git-count-revs nil)))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean
  [_]
  (b/delete {:path "target"}))

(defn jar
  [_]
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis basis
                :src-dirs ["src"]
                :scm {:url "https://github.com/replikativ/konserve-lmdb"
                      :connection "scm:git:git://github.com/replikativ/konserve-lmdb.git"
                      :developerConnection "scm:git:ssh://git@github.com/replikativ/konserve-lmdb.git"}})
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn deploy
  "Don't forget to set CLOJARS_USERNAME and CLOJARS_PASSWORD env vars."
  [_]
  (dd/deploy {:installer :remote :artifact jar-file
              :pom-file (b/pom-path {:lib lib :class-dir class-dir})}))

(defn install
  [_]
  (clean nil)
  (jar nil)
  (b/install {:basis (b/create-basis {})
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))
