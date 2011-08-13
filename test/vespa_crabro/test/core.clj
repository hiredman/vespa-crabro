(ns vespa-crabro.test.core
  (:use [vespa.crabro]
        [vespa.protocols]
        [clojure.test]))

;;(add-watch vespa.logging/log :stdout (fn [k r os obj] (println (map str obj))))

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
      (receive-from mb "foo" (fn [x] (is (= x {:x 2})))))
    (testing "broadcasting"
      (with-open [mb1 (message-bus)
                  mb2 (message-bus)]
        (declare-broadcast mb1 "broadcast")
        (declare-broadcast mb2 "broadcast")
        (send-to mb1 "broadcast" {:x 3})
        (receive-from mb1 "broadcast"
                      (fn [x]
                        (is (= x {:x 3}))))
        (receive-from mb2 "broadcast"
                      (fn [x]
                        (is (= x {:x 3}))))))
    (testing "cloning"
      (with-open [mb2 (.clone mb)]
        (send-to mb2 "foo" {:x 1})
        (is (nil? (receive-from mb "foo" (fn [x] (is (= x {:x 1})) nil))))))))
