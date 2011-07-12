(ns vespa-crabro.test.core
  (:use [vespa.crabro]
        [clojure.test]))

(deftest test-vespa-crabro
  (with-open [server (create-server)
              mb (message-bus)]
    (testing "normal send/recieve"
      (send-to mb "foo" {:x 1})
      (receive-from mb "foo" (fn [x] (is (= x {:x 1})))))
    (testing "send cookie recieve, re-send using new message bus"
      (send-to mb "foo" (cookie mb))
      (receive-from mb "foo"
                    (fn [c]
                      (with-open [mb2 (message-bus c)]
                        (send-to mb2 "foo" {:x 2}))))
      (receive-from mb "foo" (fn [x] (is (= x {:x 2})))))))
