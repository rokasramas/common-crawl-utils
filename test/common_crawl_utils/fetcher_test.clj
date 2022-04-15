(ns common-crawl-utils.fetcher-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [common-crawl-utils.fetcher :as fetcher]))

(deftest ^:integration fetcher-test
  (let [query-1 {:url     "tokenmill.lt"
                 :filter  ["digest:U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS"]
                 :cdx-api "http://index.commoncrawl.org/CC-MAIN-2019-09-index"}
        coordinate-1 {:offset        "272838009",
                      :digest        "U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS",
                      :mime          "text/html",
                      :encoding       "UTF-8",
                      :mime-detected "text/html",
                      :filename      "crawl-data/CC-MAIN-2019-09/segments/1550247481992.39/warc/CC-MAIN-20190217111746-20190217133746-00381.warc.gz",
                      :status        "200",
                      :urlkey        "lt,tokenmill)/",
                      :url           "http://tokenmill.lt/",
                      :length        "8548",
                      :languages     "eng",
                      :timestamp     "20190217125141"}]
    (testing "Fetching single coordinate content"
      (let [response (fetcher/fetch-content query-1)]
        (is (= [coordinate-1] (map #(dissoc % :content) response)))
        (is (every? #(not (str/blank? (-> % (get :content) (vals) (str/join "\r\n\r\n")))) response))))
    (testing "Fetching content"
      (is (pos-int? (count (fetcher/fetch-content query-1)))))))
