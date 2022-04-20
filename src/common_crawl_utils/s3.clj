(ns common-crawl-utils.s3
  (:require [common-crawl-utils.constants :as constants]
            [cognitect.aws.client :as client]
            [cognitect.aws.client.api :as aws]
            [cognitect.aws.credentials :as credentials]
            [cognitect.aws.endpoint :as endpoint]
            [cognitect.aws.region :as region]))

(defn build-cc-s3-client
  ([] (build-cc-s3-client (credentials/default-credentials-provider constants/default-http-client)))
  ([credentials-provider]
   (aws/client {:api                  :s3
                :credentials-provider credentials-provider
                :region               "us-east-1"
                :endpoint-override    {:protocol :https
                                       :hostname "s3.us-east-1.amazonaws.com"}})))

(defn- with-endpoint [req {:keys [protocol hostname port path]}]
  (cond-> (-> req
              (assoc-in [:headers "host"] hostname)
              (assoc :server-name hostname))
          protocol (assoc :scheme protocol)
          port (assoc :server-port port)
          path (assoc :uri path)))

(defn build-cc-s3-request [s3-client coord]
  (let [{:keys [service region-provider credentials-provider endpoint-provider]} (client/-get-info s3-client)
        region (region/fetch region-provider)
        endpoint (endpoint/fetch endpoint-provider region)
        op-map {:op      :GetObject
                :request {:Bucket "commoncrawl"
                          :Key    (:filename coord)}}]
    (client/sign-http-request service
                              endpoint
                              (credentials/fetch credentials-provider)
                              (-> (client/build-http-request service op-map)
                                  (with-endpoint endpoint)))))
