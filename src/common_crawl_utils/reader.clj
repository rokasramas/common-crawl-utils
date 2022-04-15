(ns common-crawl-utils.reader
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [common-crawl-utils.constants :as constants]
            [common-crawl-utils.utils :as utils]))

(defn get-urls
  ([path] (get-urls path constants/cc-s3-base-url))
  ([path cc-s3-base-url]
   (map (partial str cc-s3-base-url)
        (utils/gzip-line-seq (str constants/cc-s3-base-url path)))))

(defn get-warc-urls
  ([id] (get-warc-urls id constants/cc-s3-base-url))
  ([id cc-s3-base-url]
   (get-urls (format "crawl-data/%s/warc.paths.gz" id) cc-s3-base-url)))

(defn read-warc
  "Given an ID of the Crawl returns a sequence of WARC records.
  ID example: \"CC-MAIN-2019-35\".
  By default reads WARCs from the latest Crawl.
  Returns the sequence of the WARC records."
  ([] (read-warc (:id (utils/get-most-recent-crawl))))
  ([id] (read-warc id constants/cc-s3-base-url))
  ([id cc-s3-base-url] (mapcat utils/warc-record-seq (get-warc-urls id cc-s3-base-url))))

(defn get-cdx-urls
  ([id] (get-cdx-urls id constants/cc-s3-base-url))
  ([id cc-s3-base-url]
   (get-urls (format "crawl-data/%s/cc-index.paths.gz" id) cc-s3-base-url)))

(defn- parse-coordinate [line]
  (let [[urlkey timestamp body] (str/split line #"\s" 3)]
    (-> body
        (json/parse-string true)
        (assoc :timestamp timestamp
               :urlkey urlkey))))

(defn read-coordinates
  "Given an ID of the Crawl returns a sequence of Common Crawl Coordinates records.
  ID example: \"CC-MAIN-2019-35\".
  By default reads coordinates from the latest Crawl.
  Returns the sequence of the coordinates."
  ([] (read-coordinates (:id (utils/get-most-recent-crawl))))
  ([id] (read-coordinates id constants/cc-s3-base-url))
  ([id cc-s3-base-url] (->> (get-cdx-urls id cc-s3-base-url) (mapcat utils/gzip-line-seq) (map parse-coordinate))))
