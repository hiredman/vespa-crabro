(ns vespa.crabro
  (:use [clojure.java.io :only [file]]
        [vespa.logging :only [log-append log-delegate-factory log-delegate-factory-classname]])
  (:require [vespa.logging])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream Closeable File
                    ObjectInputStream ObjectOutputStream)
           (java.net InetAddress)
           (java.util Date UUID)
           (org.apache.commons.codec.binary Base64)
           (org.hornetq.api.core TransportConfiguration)
           (org.hornetq.api.core.client HornetQClient)
           (org.hornetq.core.config.impl ConfigurationImpl)
           (org.hornetq.core.logging Logger)
           (org.hornetq.core.remoting.impl.netty NettyAcceptorFactory NettyConnectorFactory)
           (org.hornetq.core.server HornetQComponent)
           (org.hornetq.core.server.embedded EmbeddedHornetQ)
           (org.hornetq.spi.core.security HornetQSecurityManager)))

(defn- serialize [object]
  (with-open [baos (ByteArrayOutputStream.)
              oos (ObjectOutputStream. baos)]
    (.writeObject oos object)
    (.toByteArray baos)))

(defn- deserialize [bytes]
  (with-open [bais (ByteArrayInputStream. bytes)
              ois (ObjectInputStream. bais)]
    (.readObject ois)))

(defn- hostname []
  (.getHostName (InetAddress/getLocalHost)))

(defprotocol IHaveACookie
  (cookie [obj]))

(defprotocol IHaveASession
  (get-session [obj]))

(defn- security-manager [username password]
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

(defn safe-compiler-loader []
  (try
    @clojure.lang.Compiler/LOADER
    (catch Exception _
      (.getContextClassLoader (Thread/currentThread)))))

(defmacro with-loader [& body]
  `(let [cxt-loader# (.getContextClassLoader (Thread/currentThread))]
     (.setContextClassLoader (Thread/currentThread) (safe-compiler-loader))
     (try
       ~@body
       (finally
        (.setContextClassLoader (Thread/currentThread) cxt-loader#)))))

(defn configure-sever [& {:keys [config journal-dir bindings-dir
                                 large-messages-dir paging-dir persistence?
                                 security? shared-store? username password
                                 logging-delegate-classname]}]
  (doto (EmbeddedHornetQ.)
    (.setConfiguration
     (doto config
       (.setJournalDirectory journal-dir)
       (.setBindingsDirectory bindings-dir)
       (.setLargeMessagesDirectory large-messages-dir)
       (.setPagingDirectory paging-dir)
       (.setPersistenceEnabled persistence?)
       (.setSecurityEnabled security?)
       (.setSharedStore shared-store?)
       (.setClusterUser username)
       (.setClusterPassword password)
       (.setLogDelegateFactoryClassName logging-delegate-classname)))
    (.setSecurityManager (security-manager username password))))

(defn add-netty-acceptor-factory [list opts]
  (.add list (TransportConfiguration. (.getName NettyAcceptorFactory) opts)))

(defn create-server
  "starts an embedded HornetQ server"
  [& {:as opts}]
  (let [cookie (file (System/getProperty "user.dir") ".vespa-cookie")
        random-port (+ 2000 (rand-int 500))
        random-username (str (UUID/randomUUID))
        random-password (str (UUID/randomUUID))
        defaults {:username random-username
                 :password random-password
                 :host (hostname)
                 :port random-port}
        {:keys [username password host port] :as opts} (merge
                                                        defaults
                                                        (when (.exists cookie)
                                                          (deserialize
                                                           (Base64/decodeBase64
                                                            (slurp cookie))))
                                                        opts)
        cookie-string (Base64/encodeBase64String (serialize opts))
        tmp-dir (file (System/getProperty "java.io.tmpdir") username)
        config (ConfigurationImpl.)
        {:keys [bindingsDirectory journalDirectory largeMessagesDirectory pagingDirectory]}
        (bean config)
        journal-dir (.getAbsolutePath (file tmp-dir journalDirectory))
        bindings-dir (.getAbsolutePath (file tmp-dir bindingsDirectory))
        large-messages-dir (.getAbsolutePath (file tmp-dir largeMessagesDirectory))
        paging-dir (.getAbsolutePath (file tmp-dir pagingDirectory))
        acceptor-configs (doto (.getAcceptorConfigurations config)
                           (add-netty-acceptor-factory {"host" host "port" port}))
        server (configure-sever
                :config config
                :journal-dir journal-dir
                :bindings-dir bindings-dir
                :large-messages-dir large-messages-dir
                :paging-dir paging-dir
                :persistence? false
                :security? true
                :shared-store? false
                :username username
                :password password
                :logging-delegate-classname log-delegate-factory-classname)]
    ;; hornetq starts logging before applying my logging settings
    ;; this gets around it
    (Logger/setDelegateFactory (log-delegate-factory))
    ;; with-loader set's the compiler's classloader in place of the
    ;; thread's current context loader so when the server starts it
    ;; loads the logging class via the compiler's loader
    (with-loader
      (.start server))
    (spit cookie cookie-string)
    (reify
      Closeable
      (close [_]
        (.stop server)
        (doseq [f (reverse (file-seq tmp-dir))]
          (.delete f)))
      IHaveACookie
      (cookie [_] cookie-string))))

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
  (receive-from [mb name fun])
  (get-consumer-cache [mb])
  (get-producer [mb]))

(defn send-to-fn [mb name msg]
  (try
    (create-queue mb name)
    (catch Exception e
      (log-append (Date.) :trace "failed to create-queue" *ns* e)))
  (let [m (doto (.createMessage (get-session mb) false)
            (-> .getBodyBuffer (.writeBytes (serialize msg))))]
    (.send (get-producer mb) name m)))

(defn receive-from-fn [mb name fun]
  (try
    (create-queue mb name)
    (catch Exception e
      (log-append (Date.) :trace "failed to create-queue" *ns* e)))
  (.start (get-session mb))
  (swap! (get-consumer-cache mb)
         (fn [cache]
           (if (contains? cache name)
             cache
             (assoc cache name (.createConsumer (get-session mb) name)))))
  (let [c (get @(get-consumer-cache mb) name)
        m (.receive c)
        bb (.getBodyBuffer m)
        buf (byte-array (.readableBytes bb))]
    (.readBytes bb buf)
    (let [result (fun (deserialize buf))]
      (.acknowledge m)
      result)))

(declare message-bus)

(deftype AMessageBus [session session-factory producer consumer-cache cookie]
  MessageBus
  (create-queue [mb name]
    (.createQueue session name name))
  (create-tmp-queue [mb name]
    (.createTemporaryQueue session name name))
  (send-to [mb name msg]
    (send-to-fn mb name msg))
  (receive-from [mb name fun]
    (receive-from-fn mb name fun))
  (get-consumer-cache [mb] consumer-cache)
  (get-producer [mb] producer)
  IHaveASession
  (get-session [mb] session)
  Object
  (clone [mb]
    (message-bus session session-factory))
  Closeable
  (close [mb]
    (.stop session)
    (.close session))
  IHaveACookie
  (cookie [_] cookie))

(defn message-bus
  ([]
     (message-bus
      (slurp (file (System/getProperty "user.dir") ".vespa-cookie"))))
  ([cookie-or-map]
     (if (map? cookie-or-map)
       (let [{:keys [host port username password]} cookie-or-map
             sf (create-session-factory host port)
             s (.createSession sf username password false true true false 1)]
         (message-bus
          s sf (Base64/encodeBase64String (serialize cookie-or-map))))
       (message-bus (deserialize (Base64/decodeBase64 cookie-or-map)))))
  ([session session-factory cookie]
     (message-bus session session-factory (.createProducer session)
                  (atom {}) cookie))
  ([session session-factory producer consumer-cache cookie]
     (AMessageBus. session session-factory producer consumer-cache cookie)))
