(ns vespa.streams
  (:require [vespa.protocols :as vp]
            [vespa.crabro :as vc])
  (:import (java.io OutputStream
                    ByteArrayInputStream
                    SequenceInputStream
                    BufferedOutputStream)
           (java.util UUID Enumeration)
           (java.nio ByteBuffer)))

(defn int->bytes [n]
  (-> (ByteBuffer/allocate 4)
      (.putInt n)
      .array))

(defn output-stream [mb queue buffer-size]
  (BufferedOutputStream.
   (proxy [OutputStream] []
     (write
       ([byte-or-bytes]
          (if (number? byte-or-bytes)
            (vc/send-to mb queue (int->bytes byte-or-bytes))
            (vc/send-to mb queue byte-or-bytes)))
       ([bytes i len]
          (let [b (ByteBuffer/allocate len)]
            (.put b bytes i len)
            (vc/send-to mb queue (.array b)))))
     (close []
       (vc/send-to mb queue :eof)))
   buffer-size))

(defn seq->enumeration [s]
  (let [state (ref s)]
    (reify
      Enumeration
      (hasMoreElements [_]
        (boolean (seq @state)))
      (nextElement [_]
        (dosync
         (let [elem (first @state)]
           (alter state rest)
           elem))))))

(defn input-stream [mb queue]
  (SequenceInputStream.
   (seq->enumeration
    ((fn x []
       (lazy-seq
        (loop [r (vp/receive-from mb queue identity)]
          (cond
           (= :vespa.crabro/timeout r)
           (recur (vp/receive-from mb queue identity))
           (not= :eof r)
           (cons (ByteArrayInputStream. r) (x))
           :else
           nil))))))))
