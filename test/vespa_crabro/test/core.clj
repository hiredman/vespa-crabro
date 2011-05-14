(ns vespa-crabro.test.core
  (:use [vespa.crabro]
        [clojure.test]))

(deftest test-vespa-crabro
  (with-open [server (create-server)
              mb (message-bus)]
    (send-to mb "foo" {:x 1})
    (receive-from mb "foo"
                  (fn [x]
                    (is (= x {:x 1}))))))
