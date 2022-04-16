(ns common-crawl-utils.fetcher
  (:require [common-crawl-utils.constants :as constants]
            [common-crawl-utils.coordinates :as coordinates]
            [common-crawl-utils.utils :as utils]
            [clj-http.client :as http]
            [slingshot.slingshot :refer [try+]]
            [clojure.core.async :refer [chan <! >! go-loop close!]]
            [slingshot.slingshot :refer [try+]])
  (:import (java.io ByteArrayInputStream)
           (java.util Scanner)
           (java.util.zip GZIPInputStream)))

(defn- get-range-header [{:keys [offset length]}]
  (let [offset (Integer/parseInt offset)
        length (Integer/parseInt length)]
    (format "bytes=%s-%s" offset (dec (+ offset length)))))

(defn- read-content [body]
  (with-open [rdr (-> body (ByteArrayInputStream.) (GZIPInputStream.) (Scanner.))]
    {:warc   (.next (.useDelimiter rdr "\r\n\r\n"))
     :header (.next (.useDelimiter rdr "\r\n\r\n"))
     :html   (.next (.useDelimiter rdr "\\A"))}))

(defn fetch-single-coordinate-content
  ([coordinate] (fetch-single-coordinate-content coordinate constants/cc-s3-base-url))
  ([coordinate cc-s3-base-url] (fetch-single-coordinate-content coordinate cc-s3-base-url {}))
  ([coordinate cc-s3-base-url {:keys [connection-manager http-client]}]
   (try+
     (let [{body :body} (http/request {:url                (str cc-s3-base-url (get coordinate :filename))
                                       :method             :get
                                       :headers            {"range" (get-range-header coordinate)}
                                       :as                 :byte-array
                                       :connection-manager connection-manager
                                       :http-client        http-client})]
       (-> coordinate (dissoc :error) (assoc :content (read-content body))))
     (catch [:type :clj-http.client/unexceptional-status] r
       (assoc coordinate :error (utils/get-http-error r)))
     (catch Throwable t
       (assoc coordinate :error (utils/get-http-error {:error t}))))))

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
  ([{:keys [timeout connection-manager http-client] :as query} cc-s3-base-url]
   (let [opts {:timeout            (or timeout constants/http-timeout)
               :connection-manager connection-manager
               :http-client        http-client}]
     (map (fn [{error :error :as coordinate}]
            (cond-> coordinate (nil? error) (fetch-single-coordinate-content cc-s3-base-url opts)))
          (coordinates/fetch query)))))

(defn fetch-content-async
  "Async version of `fetch-content` which returns a content channel"
  ([query] (fetch-content-async query constants/cc-s3-base-url))
  ([{:keys [timeout connection-manager http-client] :as query} cc-s3-base-url]
   (let [opts {:timeout            (or timeout constants/http-timeout)
               :connection-manager connection-manager
               :http-client        http-client}
         page-chan (coordinates/fetch-async query)
         coord-chan (chan 100)
         content-chan (chan 100)]
     (go-loop []
       (if-some [coord-page (<! page-chan)]
         (do
           (doseq [coord coord-page]
             (>! coord-chan coord))
           (recur))
         (close! coord-chan)))
     (go-loop []
       (when-some [{error :error :as coordinate} (<! coord-chan)]
         (>! content-chan (cond-> coordinate (nil? error) (fetch-single-coordinate-content cc-s3-base-url opts)))
         (recur)))
     content-chan)))
