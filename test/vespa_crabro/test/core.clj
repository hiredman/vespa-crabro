(ns vespa-crabro.test.core
  (:use [vespa.crabro]
        [clojure.test])
  (:require [vespa.streams :as st]))

;;(add-watch vespa.logging/log :stdout (fn [k r os obj] (println (map str obj))))

(deftest test-vespa-crabro
  (binding [*in-vm-only* true]
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
          (is (nil? (receive-from mb "foo"
                                  (fn [x] (is (= x {:x 1})) nil)))))))))

(deftest t-reactor
  (binding [*in-vm-only* true]
    (with-open [server (create-server)
                mb (message-bus)]
      (with-open [r (react-to mb "q" :foo)]
        (is (= @r :foo))
        (let [state-tracker (atom nil)]
          (set-action r (fn [mb r state msg]
                          (when (= msg ::boom)
                            (throw (Exception. "boom")))
                          (reset! state-tracker [state msg])
                          :baz))
          (set-error-handler r
                             (fn [mb r state e client-message]
                               (.acknowledge client-message)
                               (reset! state-tracker e)))
          (react! r)
          (send-to mb "q" :bar)
          (Thread/sleep 100)
          (is (= @state-tracker [:foo :bar]))
          (is (= @r :baz))
          (send-to mb "q" ::boom)
          (is (= @r :baz))
          (Thread/sleep 100)
          (is (thrown?  Exception (throw @state-tracker))))))))

(deftest t-expiration
  (binding [*in-vm-only* true]
    (with-open [server (create-server)
                mb (message-bus)]
      (testing "expiration"
        (send-to mb "foo" {:x 5} :expiration 1000)
        (send-to mb "foo" {:x 6})
        (Thread/sleep 2000)
        (receive-from mb "foo"
                      (fn [x]
                        (is (= x {:x 6}))))))))

(deftest test-streams
  (with-open [server (create-server)
              mb (message-bus)]
    (with-open [op (st/output-stream mb "foo" 1024)]
      (dotimes [i 26]
        (.write op (+ 65 i))))
    (with-open [in (st/input-stream mb "foo" 500)]
      (is (= "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
             (slurp in))))))

(deftest t-repl
  (with-open [server (create-server)
              mb (message-bus)]
    (future
      (try
        (binding [*in* (java.io.PushbackReader.
                        (java.io.InputStreamReader.
                         (st/input-stream mb "in" 500)))
                  *out* (-> (st/output-stream mb "out" 1024)
                            java.io.OutputStreamWriter.
                            java.io.PrintWriter.)
                  *err* (-> (st/output-stream mb "err" 1024)
                            java.io.OutputStreamWriter.
                            java.io.PrintWriter.)]
          (clojure.main/repl
           :need-prompt (constantly false)
           :prompt (constantly false)
           :print #(send-to mb "result" %)))
        (catch Exception _)))
    (doto (st/output-stream mb "in" 1024)
      (.write (.getBytes "(+ 1 2)\n"))
      (.flush))
    (receive-from mb "result" #(is (= 3 %)))
    (doto (st/output-stream mb "in" 1024)
      (.write (.getBytes "(println 1)\n"))
      (.flush))
    (receive-from mb "result" #(is (= nil %)))
    (receive-from mb "out" #(is (= "1\n" (String. %))))))

(comment

  (do
    (import (javax.swing JFrame SwingUtilities))

    (defmacro EDT
      "runs body on the Event-Dispatch-Thread (Swing)"
      [& body]
      `(SwingUtilities/invokeLater (fn [] ~@body)))

    (defonce store (atom {}))

    (def event-q "foo")

    (defn new-JFrame [mb frame-name]
      (let [jf (JFrame.)]
        (declare-broadcast mb event-q)
        (swap! store assoc frame-name jf)
        (send-to mb event-q [:created (type jf) frame-name])))

    (defn f []
      (binding [*in-vm-only* true]
        (with-open [server (create-server)
                    mb1 (message-bus)
                    mb2 (message-bus)]
          (let [o *out*]
            (future
              (binding [*out* o]
                (try
                  (declare-broadcast mb1 event-q)
                  (receive-from mb2 event-q
                                (fn [[event type name]]
                                  (cond (and (= event :created)
                                             (= type JFrame))
                                        (let [jf (get @store name)]
                                          (prn jf)
                                          (EDT
                                           (doto jf
                                             (.setSize 800 600)
                                             (.setVisible true)))))))
                  (catch Exception e
                    (prn e))))))
          (new-JFrame mb2 ::frame)
          @(promise))))

    )



  )
