(ns common-crawl-utils.constants
  (:require [clj-http.core :as http-core]
            [clj-http.conn-mgr :as http-conn]))

(def index-collinfo "http://index.commoncrawl.org/collinfo.json")

(def cc-s3-base-url "https://data.commoncrawl.org/")

(def default-connection-manager (http-conn/make-reusable-conn-manager {:timeout 10}))

(def default-http-client (http-core/build-http-client {} false default-connection-manager))
