(ns vespa.logging
  (:use [clojure.java.io :only [file]])
  (:import (clojure.lang IDeref IRef Box)
           (java.util ArrayList Date)
           (java.util.concurrent ConcurrentHashMap)
           (org.hornetq.core.server HornetQComponent)
           (org.hornetq.spi.core.logging LogDelegate LogDelegateFactory)))

(defprotocol Buffer
  (append [buf obj]))

(deftype RingBuffer [^ArrayList buf watches validator size]
  IDeref
  (deref [rb]
    (into [] buf))
  IRef
  (setValidator [rb vf]
    (set! (.val validator) vf))
  (getValidator [_]
    (.val validator))
  (getWatches [_]
    (into {} watches))
  (addWatch [this key callback]
    (.put watches key callback)
    this)
  (removeWatch [this key]
    (.remove watches key)
    this)
  Buffer
  (append [this obj]
    (locking buf
      (when ((.val validator) obj)
        (let [old-state (when-not (empty? watches)
                          @this)]
          (.add buf obj)
          (when (> (long (count buf)) size)
            (dotimes [i (- (count buf) size)]
              (.remove buf (int 0))))
          (doseq [[k the-fn] watches]
            (the-fn k this old-state obj)))))
    nil))

(defn ring-buffer [n]
  (let [a (ArrayList. n)]
    (RingBuffer. a (ConcurrentHashMap.) (Box. (constantly true)) n)))

;; (def ^{:doc
;;        "hornetq log messages are redirected to this queue, put a watch on the agent if
;;   you want to do something else with them.
;;   format is [date level message class-or-namespace & [throwable]]"}
;;   log (agent PersistentQueue/EMPTY))

;; (defn- trim-log [log]
;;   (if (> (count log) 10)
;;     (-> log pop pop)
;;     log))

;; (defn- log-append [& stuff]
;;   (send-off log (fn [log] (conj (trim-log log) stuff))))

(def log (ring-buffer 10))

(defn log-append [& stuff]
  (append log stuff))

(deftype LDF []
  LogDelegateFactory
  (createDelegate [_ class]
    (reify
      LogDelegate
      (isInfoEnabled [_] true)
      (isDebugEnabled [_] true)
      (isTraceEnabled [_] false)
      (fatal [_ message]
        (log-append (Date.) :fatal message class))
      (fatal [_ message throwable]
        (log-append (Date.) :fatal message class throwable))
      (error [_ message]
        (log-append (Date.) :error message))
      (error [_ message throwable]
        (log-append (Date.) :error message class throwable))
      (warn [_ message]
        (log-append (Date.) :warn message))
      (warn [_ message throwable]
        (log-append (Date.) :warn message class throwable))
      (info [_ message]
        (log-append (Date.) :info message class))
      (info [_ message throwable]
        (log-append (Date.) :info message class throwable))
      (debug [_ message]
        (log-append (Date.) :debug message class))
      (debug [_ message throwable]
        (log-append (Date.) :debug message class throwable))
      (trace [_ message]
        (log-append (Date.) :trace message class))
      (trace [_ message throwable]
        (log-append (Date.) :trace message class throwable)))))

(defn log-delegate-factory []
  (LDF.))

(def log-delegate-factory-classname "vespa.logging.LDF")
