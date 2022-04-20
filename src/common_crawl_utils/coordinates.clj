(ns common-crawl-utils.coordinates
  (:require [clojure.tools.logging :as log]
            [common-crawl-utils.constants :as constants]
            [common-crawl-utils.utils :as utils]
            [clj-http.client :as http]
            [slingshot.slingshot :refer [try+]]))

(def cdx-params [:url :from :to :matchType :limit :sort :filter
                 :fl :page :pageSize :showNumPages :showPagedIndex])

(defn get-most-recent-cdx-api
  [{:keys [index-collinfo]
    :as   query
    :or   {index-collinfo constants/index-collinfo}}]
  (let [{:keys [cdx-api error]} (utils/get-most-recent-crawl index-collinfo)]
    (cond-> query
            (some? cdx-api) (assoc :cdx-api cdx-api)
            (some? error) (assoc :error error))))

(defn call-cdx-api [{:keys [cdx-api connection-manager http-client] :as query
                     :or   {connection-manager constants/default-connection-manager
                            http-client        constants/default-http-client}}]
  (log/debugf "Calling `%s` with query `%s`" cdx-api (select-keys query cdx-params))
  (try+
    (-> {:url                cdx-api
         :method             :get
         :connection-manager connection-manager
         :http-client        http-client
         :query-params       (-> query (select-keys cdx-params) (assoc :output "json"))}
        (http/request)
        (get :body)
        (utils/read-jsonl))
    (catch [:status 404] _
      (log/debugf "No results found in crawl `%s` for query `%s`" cdx-api (select-keys query cdx-params))
      [])
    (catch [:type :clj-http.client/unexceptional-status] r
      (-> query (assoc :error (utils/get-http-error r)) (vector)))
    (catch Throwable t
      (-> query (assoc :error (utils/get-http-error {:error t})) (vector)))))

(defn get-total-pages [{:keys [cdx-api] :as query}]
  (let [[{:keys [pages error]}] (when cdx-api (-> query (assoc :showNumPages true) (call-cdx-api)))]
    (cond-> query
            (some? pages) (assoc :pages pages)
            (some? error) (assoc :error error))))

(defn- validate-query [{:keys [cdx-api error page pages showNumPages] :as query}]
  (cond-> query
          (some? error) (dissoc :error)
          (some? showNumPages) (dissoc :showNumPages)
          (nil? cdx-api) (get-most-recent-cdx-api)
          (nil? pages) (get-total-pages)
          (nil? page) (assoc :page 0)))

(defn fetch
  "Issues HTTP request to Common Crawl Index Server and returns a lazy sequence with content coordinates

  Takes `query` map, described in https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference

  Additionally, `:cdx-api` query key can specify index server endpoint.
  If `:cdx-api` is not provided, endpoint from most recent crawl is used and
  can be accesed with `(common-crawl-utils.config/get-most-recent-cdx-api)`

  ;; To fetch all coordinates for host from most recent crawl
  (fetch {:url \"http://www.cnn.com\" :matchType \"host\"})

  ;; To fetch limited number of coordinates
  (take 10 (fetch {:url \"http://www.cnn.com\" :matchType \"host\"}))"
  [query]
  (lazy-seq
    (let [{:keys [page pages error] :as validated-query} (validate-query query)]
      (cond
        (some? error) (vector validated-query)
        (zero? pages) (vector)
        (< page pages) (concat (call-cdx-api validated-query)
                               (fetch (update validated-query :page inc)))))))

(defn get-coordinate-count
  ([hosts]
   (get-coordinate-count hosts (:cdx-api (utils/get-most-recent-crawl))))
  ([hosts cdx-api]
   (map #(vector % (count (fetch {:url % :matchType "host" :cdx-api cdx-api}))) hosts)))
