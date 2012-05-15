(ns vespa.server
  (:gen-class))

(defn- -main [& args]
  (use '[vespa.crabro :only [create-server]]
       '[vespa.logging :only [log]])
  (try
    (add-watch (resolve 'log)
               :stdout
               (fn [k r os obj] (locking #'*out* (println (map str obj)))))
    (apply (resolve 'create-server) (map read-string args))
    (catch Exception e
      (.printStackTrace e))))
