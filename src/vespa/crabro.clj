(ns vespa.crabro
  (:use [clojure.java.io :only [file]])
  (:import (clojure.lang PersistentQueue)
           (java.io ByteArrayInputStream ByteArrayOutputStream Closeable File
                    ObjectInputStream ObjectOutputStream)
           (java.net InetAddress)
           (java.util Date UUID)
           (org.apache.commons.codec.binary Base64)
           (org.hornetq.api.core TransportConfiguration)
           (org.hornetq.api.core.client HornetQClient)
           (org.hornetq.core.config.impl ConfigurationImpl)
           (org.hornetq.core.remoting.impl.netty NettyAcceptorFactory NettyConnectorFactory)
           (org.hornetq.core.server HornetQComponent)
           (org.hornetq.core.server.embedded EmbeddedHornetQ)
           (org.hornetq.spi.core.logging LogDelegate LogDelegateFactory)
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

(def ^{:doc
       "hornetq log messages are redirected to this queue, put a watch on the agent if
  you want to do something else with them.
  format is [date level message class-or-namespace & [throwable]]"}
  log (agent PersistentQueue/EMPTY))

(defn- trim-log [log]
  (if (> (count log) 10)
    (-> log pop pop)
    log))

(defn- log-append [& stuff]
  (send-off log (fn [log] (conj (trim-log log) stuff))))

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

(defmacro with-loader [& body]
  `(let [cxt-loader# (.getContextClassLoader (Thread/currentThread))]
     (.setContextClassLoader (Thread/currentThread) @clojure.lang.Compiler/LOADER)
     (try
       ~@body
       (finally
        (.setContextClassLoader (Thread/currentThread) cxt-loader#)))))

(defn configure-sever [& {:keys [config journal-dir bindings-dir large-messages-dir
                                 paging-dir persistence? security? shared-store?
                                 username password logging-delegate-classname]}]
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

(defn create-server
  "starts an embedded HornetQ server"
  [& {:as opts}]
  (let [cookie (file (System/getProperty "user.home") ".vespa-cookie")
        {:keys [username password host port] :as opts} (merge
                                                        {:username (str (UUID/randomUUID))
                                                         :password (str (UUID/randomUUID))
                                                         :host (hostname)
                                                         :port (+ 2000 (rand-int 500))}
                                                        (when (.exists cookie)
                                                          (deserialize
                                                           (Base64/decodeBase64 (slurp cookie))))
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
                           (.add (-> NettyAcceptorFactory .getName
                                     (TransportConfiguration.
                                      {"port" port
                                       "host" host}))))
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
                :logging-delegate-classname "vespa.crabro.LDF")]
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
  (receive-from [mb name fun]))

(defn message-bus
  ([]
     (message-bus
      (slurp (file (System/getProperty "user.home") ".vespa-cookie"))))
  ([cookie-or-map]
     (if (map? cookie-or-map)
       (let [{:keys [host port username password]} cookie-or-map
             sf (create-session-factory host port)
             s (.createSession sf username password false true true false 1)]
         (message-bus s sf))
       (message-bus (deserialize (Base64/decodeBase64 cookie-or-map)))))
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
           (catch Exception e
             (log-append (Date.) :trace "failed to create-queue" *ns* e)))
         (let [m (doto (.createMessage session false)
                   (-> .getBodyBuffer (.writeBytes (serialize msg))))]
           (.send producer name m)))
       (receive-from [mb name fun]
         (try
           (create-queue mb name)
           (catch Exception e
             (log-append (Date.) :trace "failed to create-queue" *ns* e)))
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
