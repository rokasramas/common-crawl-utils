(ns common-crawl-utils.fetcher
  (:require [common-crawl-utils.constants :as constants]
            [common-crawl-utils.coordinates :as coordinates]
            [common-crawl-utils.s3 :as s3]
            [common-crawl-utils.utils :as utils]
            [clj-http.client :as http]
            [slingshot.slingshot :refer [try+]])
  (:import (java.io ByteArrayInputStream)
           (java.util Scanner)
           (java.util.zip GZIPInputStream)))

(defn- get-range-header [{:keys [offset length]}]
  (let [offset (Integer/parseInt offset)
        length (Integer/parseInt length)]
    (format "bytes=%s-%s" offset (dec (+ offset length)))))

(defn- read-arc-content [body]
  (with-open [rdr (-> body (ByteArrayInputStream.) (GZIPInputStream.) (Scanner.))]
    {:header (.next (.useDelimiter rdr "\r\n\r\n"))
     :html   (.next (.useDelimiter rdr "\r\n\r\n"))}))

(defn- read-content [body]
  (with-open [rdr (-> body (ByteArrayInputStream.) (GZIPInputStream.) (Scanner.))]
    {:warc   (.next (.useDelimiter rdr "\r\n\r\n"))
     :header (.next (.useDelimiter rdr "\r\n\r\n"))
     :html   (.next (.useDelimiter rdr "\\A"))}))

(defn fetch-single-coordinate-content
  ([coordinate] (fetch-single-coordinate-content coordinate constants/cc-s3-base-url))
  ([coordinate cc-s3-base-url] (fetch-single-coordinate-content coordinate cc-s3-base-url {}))
  ([coordinate cc-s3-base-url {:keys [s3-client connection-manager http-client]
                               :or   {connection-manager constants/default-connection-manager
                                      http-client        constants/default-http-client}}]
   (try+
     (let [req (-> (if (some? s3-client)
                     (s3/build-cc-s3-request s3-client coordinate)
                     {:url    (str cc-s3-base-url (get coordinate :filename))
                      :method :get})
                   (assoc :as :byte-array
                          :connection-manager connection-manager
                          :http-client http-client)
                   (assoc-in [:headers "range"] (get-range-header coordinate)))
           resp (http/request req)
           year (first (utils/parse-timestamp (:timestamp coordinate)))
           content (if (< 2012 year) (read-content (:body resp)) (read-arc-content (:body resp)))]
       (-> coordinate (dissoc :error) (assoc :content content)))
     (catch [:type :clj-http.client/unexceptional-status] r
       (assoc coordinate :error (utils/get-http-error r)))
     (catch Throwable t
       (assoc coordinate :error (utils/get-http-error {:error t}))))))

(defn fetch-coordinate-content
  "Fetches coordinate content from AWS

  Takes `coordinates` seq, produced by `common-crawl-utils.coordinates/fetch`

  ;; To fetch limited number of coordinates with content
  (take 10 (fetch-coordinate-content (common-crawl-utils.coordinates/fetch {:url \"http://www.cnn.com\"}))"
  ([coordinates] (fetch-coordinate-content coordinates constants/cc-s3-base-url))
  ([coordinates cc-s3-base-url] (fetch-coordinate-content coordinates cc-s3-base-url {}))
  ([coordinates cc-s3-base-url opts]
   (map (fn [{error :error :as coordinate}]
          (cond-> coordinate (nil? error) (fetch-single-coordinate-content cc-s3-base-url opts)))
        coordinates)))

(defn fetch-content
  "Fetches coordinates from Common Crawl Index Server along with their content from AWS

  Takes `query` map, described in https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference

  Additionally, `:cdx-api` query key can specify index server endpoint.
  If `:cdx-api` is not provided, endpoint from most recent crawl is used and
  can be accesed with `(common-crawl-utils.config/get-most-recent-cdx-api)`

  ;; To fetch all content for host from most recent crawl
  (fetch-content {:url \"http://www.cnn.com\" :matchType \"host\"})

  ;; To fetch limited number of coordinates with content
  (take 10 (fetch-content {:url \"http://www.cnn.com\" :matchType \"host\"}))"
  ([query] (fetch-content query constants/cc-s3-base-url))
  ([query cc-s3-base-url]
   (let [opts (select-keys query [:s3-client :connection-manager :http-client])]
     (-> query
         (coordinates/fetch)
         (fetch-coordinate-content cc-s3-base-url opts)))))
