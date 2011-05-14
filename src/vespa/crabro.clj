(ns vespa.crabro
  (:use [clojure.java.io :only [file]])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream Closeable File
                    ObjectInputStream ObjectOutputStream)
           (java.net InetAddress)
           (java.util UUID)
           (org.apache.commons.codec.binary Base64)
           (org.hornetq.api.core TransportConfiguration)
           (org.hornetq.api.core.client HornetQClient)
           (org.hornetq.core.config.impl ConfigurationImpl)
           (org.hornetq.core.remoting.impl.netty NettyAcceptorFactory NettyConnectorFactory)
           (org.hornetq.core.server HornetQComponent)
           (org.hornetq.core.server.embedded EmbeddedHornetQ)
           (org.hornetq.spi.core.security HornetQSecurityManager)))

(defn serialize [object]
  (with-open [baos (ByteArrayOutputStream.)
              oos (ObjectOutputStream. baos)]
    (.writeObject oos object)
    (.toByteArray baos)))

(defn deserialize [bytes]
  (with-open [bais (ByteArrayInputStream. bytes)
              ois (ObjectInputStream. bais)]
    (.readObject ois)))

(defn hostname []
  (.getHostName (InetAddress/getLocalHost)))

(defprotocol IHaveACookie
  (cookie [obj]))

(defn security-manager [username password]
  (reify
    HornetQSecurityManager
    (validateUser [sm user pw]
      (boolean
       (and (= user username)
            (= pw password))))
    (validateUserAndRole [sm user pw roles check-type]
      (boolean
       (and (= user username)
            (= pw password))))
    (addUser [sm _ _])
    (removeUser [sm _])
    (addRole [sm _ _])
    (removeRole [sm _ _])
    (setDefaultUser [sm _])
    (start [sm])
    (stop [sm])
    (isStarted [sm] true)))

(defn create-server
  "starts an embedded HornetQ server"
  ([] (create-server (+ 2000 (rand-int 100))))
  ([port]
     (create-server (hostname) port))
  ([host port]
     (let [username (str (UUID/randomUUID))
           password (str (UUID/randomUUID))
           tmp-dir (System/getProperty "java.io.tmpdir")
           config (ConfigurationImpl.)
           journal-dir (.getAbsolutePath
                        (file tmp-dir
                              username
                              (.getJournalDirectory config)))
           bindings-dir (.getAbsolutePath
                         (file tmp-dir
                               username
                               (.getBindingsDirectory config)))
           large-messages-dir (.getAbsolutePath
                               (file tmp-dir
                                     username
                                     (.getLargeMessagesDirectory config)))
           paging-dir (.getAbsolutePath
                       (file tmp-dir
                             username
                             (.getPagingDirectory config)))
           acceptor-configs (doto (.getAcceptorConfigurations config)
                              (.add (-> NettyAcceptorFactory .getName
                                        (TransportConfiguration.
                                         {"port" port
                                          "host" host}))))
           server (doto (EmbeddedHornetQ.)
                    (.setConfiguration
                     (doto config
                       (.setJournalDirectory journal-dir)
                       (.setBindingsDirectory bindings-dir)
                       (.setLargeMessagesDirectory large-messages-dir)
                       (.setPagingDirectory paging-dir)
                       (.setPersistenceEnabled false)
                       (.setSecurityEnabled true)
                       (.setSharedStore false)
                       (.setClusterUser username)
                       (.setClusterPassword password)))
                    (.setSecurityManager (security-manager username password))
                    (.start))]
       (reify
         Closeable
         (close [_]
           (.stop server)
           (doseq [f [journal-dir bindings-dir large-messages-dir paging-dir]]
             (.delete (file f)))
           (.delete (.getParentFile (file journal-dir)))
           (.delete (.getParentFile (.getParentFile (file journal-dir)))))
         IHaveACookie
         (cookie [_]
           (Base64/encodeBase64String
            (serialize {"port" port
                        "host" host
                        "user" username
                        "password" password})))))))

(defn create-session-factory [host port]
  (let [loc (doto (HornetQClient/createServerLocatorWithoutHA
                   (into-array
                    TransportConfiguration
                    [(TransportConfiguration.
                      (.getName NettyConnectorFactory)
                      {"host" host "port" port})]))
              (.setReconnectAttempts -1))]
    (.createSessionFactory loc)))

(defprotocol MessageBus
  (create-queue [mb name])
  (create-tmp-queue [mb name])
  (send-to [mb name msg])
  (receive-from [mb name fun]))

(defn message-bus
  ([cookie]
     (let [{host "host" port "port" user "user" password "password"}
           (deserialize (Base64/decodeBase64 cookie))
           sf (create-session-factory host port)
           s (.createSession sf user password false true true false 1)]
       (message-bus s sf)))
  ([session session-factory]
     (message-bus session session-factory (atom {}) (.createProducer session)))
  ([session session-factory consumer-cache producer]
     (reify
       MessageBus
       (create-queue [mb name]
         (.createQueue session name name))
       (create-tmp-queue [mb name]
         (.createTemporaryQueue session name name))
       (send-to [mb name msg]
         (try
           (create-queue mb name)
           (catch Exception _))
         (let [m (doto (.createMessage session false)
                   (-> .getBodyBuffer (.writeBytes (serialize msg))))]
           (.send producer name m)))
       (receive-from [mb name fun]
         (.start session)
         (swap! consumer-cache
                (fn [cache]
                  (if (contains? cache name)
                    cache
                    (assoc cache name (.createConsumer session name)))))
         (let [c (get @consumer-cache name)
               m (.receive c)
               bb (.getBodyBuffer m)
               buf (byte-array (.readableBytes bb))]
           (.readBytes bb buf)
           (let [result (fun (deserialize buf))]
             (.acknowledge m)
             result)))
       Object
       (clone [mb]
         (message-bus session session-factory))
       Closeable
       (close [mb]
         (.stop session)))))
