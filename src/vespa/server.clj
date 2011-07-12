(ns vespa.server
  (:use [vespa.crabro :only [create-server]]
        [vespa.logging :only [log]])
  (:gen-class))

(defn- -main [& args]
  (try
    (add-watch log :stdout (fn [k r os obj] (println (map #(.toString %) obj))))
    (apply create-server (map read-string args))
    (catch Exception e
      (.printStackTrace e))))
