(ns common-crawl-utils.utils
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.instant :as instant]
            [common-crawl-utils.constants :as constants]
            [clj-http.client :as http]
            [slingshot.slingshot :refer [try+]]
            [warc-clojure.core :as warc])
  (:import (java.io InputStreamReader BufferedReader)
           (java.util.zip GZIPInputStream)
           (java.text SimpleDateFormat)
           (java.time Instant)
           (org.jwat.warc WarcReaderFactory)))

(def timestamp-formatter (SimpleDateFormat. "yyyyMMddHHmmss"))

(defn parse-timestamp [ts]
  (let [inst (.toInstant (.parse timestamp-formatter ts))]
    (instant/parse-timestamp vector (str inst))))

(defn gzip-line-seq [path]
  (-> path
      (io/input-stream)
      (GZIPInputStream.)
      (InputStreamReader.)
      (BufferedReader.)
      (line-seq)))

(defn warc-record-seq [path]
  (-> path
      (io/input-stream)
      (WarcReaderFactory/getReader)
      (warc/get-response-records-seq)))

(defn get-http-error [{:keys [body error status]}]
  {:status    status
   :timestamp (str (Instant/now))
   :message   (or (when (some? error) (.getMessage ^Throwable error))
                  (when (some? body) (cond-> body (bytes? body) (slurp))))})

(defn request-json [url]
  (try+
    (http/get url
              {:method             :get
               :as                 :json
               :connection-manager constants/default-connection-manager
               :http-client        constants/default-http-client})
    (catch [:type :clj-http.client/unexceptional-status] r
      {:url url :error (get-http-error r)})
    (catch Throwable t
      {:url url :error (get-http-error {:error t})})))

(defn read-jsonl [s]
  (map #(json/parse-string % true)
       (str/split-lines s)))

(defn get-crawls
  ([]
   (get-crawls constants/index-collinfo))
  ([index-collinfo]
   (let [{:keys [body error] :as response}
         (request-json index-collinfo)]
     (if (some? error)
       (vector response)
       body))))

(defn get-most-recent-crawl
  ([]
   (get-most-recent-crawl constants/index-collinfo))
  ([index-collinfo]
   (first (get-crawls index-collinfo))))
