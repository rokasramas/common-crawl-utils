(ns common-crawl-utils.fetcher-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [common-crawl-utils.fetcher :as fetcher]))

(deftest ^:integration fetcher-test
  (let [query-1 {:url     "tokenmill.lt"
                 :filter  ["digest:U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS"]
                 :cdx-api "http://index.commoncrawl.org/CC-MAIN-2019-09-index"}
        query-2 {:url     "commoncrawl.org"
                 :filter  ["digest:YEIPJFLNQ4IJ3HP4YBDJBWGJL4MTEJ42"]
                 :cdx-api "https://index.commoncrawl.org/CC-MAIN-2008-2009-index"}
        coordinate-1 {:offset        "272838009",
                      :digest        "U3FWVBI7XZ2KVBD72MRR7TCHHXSX2FJS",
                      :mime          "text/html",
                      :encoding      "UTF-8",
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
      (let [[{content-1 :content}] (fetcher/fetch-content query-1)]
        (is (= 563 (count (:warc content-1))))
        (is (= 160 (count (:header content-1))))
        (is (= 33300 (count (:html content-1))))))
    (testing "Fetching ARC content"
      (let [[{content-2 :content}] (fetcher/fetch-content query-2)]
        (is (nil? (:warc content-2)))
        (is (= 509 (count (:header content-2))))
        (is (= 2087 (count (:html content-2))))))))
