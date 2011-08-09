(ns vespa.protocols)

(defprotocol MessageBus
  (create-queue-fn [mb name opts])
  (create-tmp-queue-fn [mb name opts])
  (send-to-fn [mb name msg opts])
  (receive-from [mb name fun])
  (get-consumer-cache [mb])
  (get-producer [mb])
  (declare-broadcast [mb queue]))

(defprotocol IHaveACookie
  (cookie [obj]))

(defprotocol IHaveASession
  (get-session [obj]))
