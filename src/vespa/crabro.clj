(ns vespa.crabro
  (:use [clojure.java.io :only [file]]
        [vespa.logging :only [log-append log-delegate-factory
                              log-delegate-factory-classname]]
        [vespa.rest :only [rest-server]])
  (:require [vespa.logging])
  (:import (java.io ByteArrayInputStream ByteArrayOutputStream Closeable File
                    ObjectInputStream ObjectOutputStream)
           (java.net InetAddress)
           (clojure.lang IRef)
           (java.util Date UUID)
           (org.apache.commons.codec.binary Base64)
           (org.hornetq.api.core TransportConfiguration SimpleString)
           (org.hornetq.api.core.client HornetQClient MessageHandler)
           (org.hornetq.core.config.impl ConfigurationImpl)
           (org.hornetq.core.logging Logger)
           (org.hornetq.core.remoting.impl.netty NettyAcceptorFactory
                                                 NettyConnectorFactory)
           (org.hornetq.core.remoting.impl.invm InVMAcceptorFactory
                                                InVMConnectorFactory)
           (org.hornetq.api.core.client ClientMessage
                                        ClientSession
                                        ClientSessionFactory
                                        ClientConsumer
                                        ClientProducer)
           (org.hornetq.core.server HornetQComponent)
           (org.hornetq.core.server.embedded EmbeddedHornetQ)
           (org.hornetq.spi.core.security HornetQSecurityManager)))

(defprotocol Reactor
  (set-action [reactor fun]
    "set the action to run on a message. action args are [messagebus this-reactor current-state new-state] actions may be run more than once for a message")
  (set-error-handler [reactor fun]
    "fun's args are [message-bus this-reactor state the-exception]")
  (react! [reactor])
  (get-queue [reactor])
  (get-action [reactor])
  (get-error-handler [reactor]))

(defprotocol MessageBus
  (create-queue-fn [mb name opts])
  (create-tmp-queue-fn [mb name opts])
  (send-to-fn [mb name msg opts])
  (receive-from [mb name fun])
  (get-consumer-cache [mb])
  (^ClientProducer get-producer [mb])
  (declare-broadcast [mb queue])
  (-react-to [mb queue init]))

(defprotocol IHaveACookie
  (cookie [obj]))

(defprotocol IHaveASession
  (^ClientSession get-session [obj]))

(def ^{:dynamic true
       :doc "bind to true to create in-vm servers and message-buses"}
  *in-vm-only* false)

(def ^{:dynamic true
       :doc "bind to true to launch hornetq's rest interface"}
  *rest-interface* false)

(defn read-coookie []
  (read-string (slurp (file (System/getProperty "user.dir") ".vespa-cookie"))))

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
    (let [c clojure.lang.Compiler/LOADER]
      (if (bound? c)
        @c
        (.getContextClassLoader (Thread/currentThread))))
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
                                 logging-delegate-classname
                                 server]}]
  (doto (or server (EmbeddedHornetQ.))
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

(defn add-vm-acceptor-factory [list opts]
  (.add list (TransportConfiguration. (.getName InVMAcceptorFactory) opts)))

(def ^{:dynamic true} *hornetq-server* nil)

(defn create-server
  "starts an embedded HornetQ server"
  [& {:as opts}]
  (let [cookie (file (System/getProperty "user.dir") ".vespa-cookie")
        ;; TODO: pull defaults out into function
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
                                                          (read-coookie))
                                                        opts)
        cookie-string (pr-str (dissoc opts :configurator))
        tmp-dir (file (System/getProperty "java.io.tmpdir") username)
        config (ConfigurationImpl.)
        {:keys [bindingsDirectory
                journalDirectory
                largeMessagesDirectory
                pagingDirectory]} (bean config)
        journal-dir (.getAbsolutePath (file tmp-dir journalDirectory))
        bindings-dir (.getAbsolutePath (file tmp-dir bindingsDirectory))
        large-messages-dir (.getAbsolutePath
                            (file tmp-dir largeMessagesDirectory))
        paging-dir (.getAbsolutePath (file tmp-dir pagingDirectory))
        acceptor-configs (if *in-vm-only*
                           (doto (.getAcceptorConfigurations config)
                             (add-vm-acceptor-factory {"server-id" port}))
                           (doto (.getAcceptorConfigurations config)
                             (add-netty-acceptor-factory
                              {"host" host "port" port})
                             (add-vm-acceptor-factory {"server-id" port})))
        server (configure-sever
                :server *hornetq-server*
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
      (when-let [configurator (:configurator opts)]
        (configurator server))
      (.start server))
    (spit cookie cookie-string)
    (let [rs (when *rest-interface*
               (rest-server server username password port))]
      (reify
        Closeable
        (close [_]
          (.stop server)
          (when *rest-interface*
            (.close rs))
          (doseq [f (reverse (file-seq tmp-dir))]
            (.delete f)))
        IHaveACookie
        (cookie [_] cookie-string)))))

(defn create-session-factory [host port & opts]
  (let [loc (doto (HornetQClient/createServerLocatorWithoutHA
                   (into-array
                    TransportConfiguration
                    (if ((set opts) :invm)
                      [(TransportConfiguration.
                        (.getName InVMConnectorFactory)
                        {"server-id" port})]
                      [(TransportConfiguration.
                        (.getName NettyConnectorFactory)
                        {"host" host "port" port})])))
              (.setReconnectAttempts -1)
              (.setMinLargeMessageSize 52428800))]
    (.createSessionFactory loc)))

(defn create-queue [mb name & {:as opts}]
  (create-queue-fn mb name opts))

(defn create-tmp-queue [mb name & {:as opts}]
  (create-tmp-queue-fn mb name opts))

(declare send-to)

(defn send-to-fn-fn [mb ^String name msg opts]
  (try
    ;; would rather not do this, but if I don't queues need to be
    ;; declared up front
    ;; possibly split out 2 sets of fns for point-to-point and pubsub messaging
    (when-not (get @(get-consumer-cache mb) name)
      (create-tmp-queue-fn mb name opts))
    (catch Exception e
      (log-append (Date.) :trace "failed to create-queue" *ns* e)))
  (let [m (doto (.createMessage (get-session mb)
                                ClientMessage/DEFAULT_TYPE
                                false
                                (:expiration opts 0)
                                (:timestamp opts 0)
                                0)
            (-> .getBodyBuffer (.writeBytes (serialize msg))))]
    (.send (get-producer mb) name m)))

(defn add-consumer [mb ^String name]
  (swap! (get-consumer-cache mb)
         (fn [cache]
           (if (contains? cache name)
             cache
             (do
               (try
                 (create-queue mb name)
                 (catch Exception e
                   (log-append (Date.) :trace "failed to create-queue" *ns* e)))
               (assoc cache name (.createConsumer (get-session mb) name)))))))

(defn message->bytes [^ClientMessage message]
  (let [bb (.getBodyBuffer message)
        buf (byte-array (.readableBytes bb))]
    (.readBytes bb buf)
    buf))

(defn receive-from-fn [mb name fun]
  (log-append (Date.) :trace (str @(get-consumer-cache mb)) *ns* nil)
  (.start (get-session mb))
  (add-consumer mb name)
  (let [^ClientConsumer c (get @(get-consumer-cache mb) name)]
    (if-let [^ClientMessage m (.receive c)]
      (let [result (fun (deserialize (message->bytes m)))]
        (.acknowledge m)
        result)
      ::timeout)))

(declare message-bus)

(defn ^MessageHandler message-handler [mb state ^IRef reactor]
  (reify
    MessageHandler
    (^void onMessage [_ ^ClientMessage client-message]
      (try
        (let [msg (-> client-message
                      message->bytes
                      deserialize)]
          (loop []
            (let [old-state @state
                  new-state ((get-action reactor)
                             mb
                             reactor
                             old-state
                             msg)]
              (when-let [v (.getValidator reactor)]
                (when (false? (v new-state))
                  (throw (IllegalStateException. "validator failed"))))
              (if (compare-and-set! state
                                    old-state
                                    new-state)
                (do
                  (doseq [[k fun] (.getWatches reactor)]
                    (fun k reactor old-state new-state))
                  (.acknowledge client-message))
                (recur)))))
        (catch Exception e
          (if-let [eh (get-error-handler reactor)]
            (eh mb reactor @state client-message e)
            (throw e)))))))

(deftype AReactor [mb
                   state
                   ^:volatile-mutable action
                   ^:volatile-mutable error-handler
                   ^:volatile-mutable running?
                   ^:volatile-mutable validator
                   ^java.util.Map watches
                   queue]
  IRef
  (setValidator [rb vf]
    (set! validator vf))
  (getValidator [_]
    validator)
  (getWatches [_]
    (into {} watches))
  (addWatch [this key callback]
    (.put watches key callback)
    this)
  (removeWatch [this key]
    (.remove watches key)
    this)
  clojure.lang.IDeref
  (deref [_] @state)
  Reactor
  (set-action [reactor fun]
    (set! action (bound-fn* fun))
    nil)
  (set-error-handler [reactor fun]
    (set! error-handler (bound-fn* fun))
    nil)
  (react! [reactor]
    (when (not running?)
      (when (not action)
        (throw (Exception. "no action")))
      (.start (get-session mb))
      (set! running? true)
      (add-consumer mb queue)
      (let [mh (message-handler mb state reactor)]
        (.setMessageHandler ^ClientConsumer (get @(get-consumer-cache mb) queue)
                            mh))
      nil))
  (get-queue [reactor]
    queue)
  (get-action [reactor]
    action)
  (get-error-handler [reactor]
    error-handler)
  Closeable
  (close [_]
    (.close ^Cloneable mb)))

(defn react-to
  "returns a Reactor built from the given messagebus
  Reactors are for listening attaching listeners to queues
  use set-action and set-error-handler on the reactor "
  ([mb queue]
     (react-to mb queue nil))
  ([mb queue & args]
     (let [options (if (and (> (count args) 1)
                            (keyword? (first args)))
                     (apply hash-map args)
                     {:init (first args)})
           r (-react-to mb queue (:init options))]
       (when (:action options)
         (set-action r (:action options)))
       (when (:error-handler options)
         (set-error-handler r (:error-handler options)))
       r)))

(deftype AMessageBus [^ClientSession session
                      session-factory
                      producer
                      consumer-cache
                      cookie]
  MessageBus
  (create-queue-fn [mb name opts]
    (.createQueue session ^String name ^String name))
  (create-tmp-queue-fn [mb name opts]
    (.createTemporaryQueue session ^String name ^String name))
  (send-to-fn [mb name msg opts]
    (send-to-fn-fn mb name msg opts))
  (receive-from [mb name fun]
    (receive-from-fn mb name fun))
  (get-consumer-cache [mb] consumer-cache)
  (get-producer [mb] producer)
  ;; needs a better name
  (declare-broadcast [mb queue]
    (let [^String queue-name (str queue "." (UUID/randomUUID))
          ^String address queue]
      (try
        (.createTemporaryQueue session address queue-name)
        (catch Exception e
          (log-append (Date.) :trace "failed to create-queue" *ns* e)))
      (.start (get-session mb))
      (swap! (get-consumer-cache mb)
             (fn [cache]
               (if (contains? cache address)
                 cache
                 (assoc cache
                   address (.createConsumer ^ClientSession (get-session mb)
                                            ^String queue-name))))))
    nil)
  (-react-to [mb queue init]
    (AReactor. (.clone mb)
               (atom init)
               nil
               nil
               false
               nil
               (java.util.concurrent.ConcurrentHashMap.)
               queue))
  IHaveASession
  (get-session [mb] session)
  Object
  (finalize [mb]
    (.close mb))
  (clone [mb]
    (message-bus cookie session-factory))
  Closeable
  (close [mb]
    (doseq [[_ ^ClientConsumer consumer] @consumer-cache]
      (.close consumer))
    (.stop session)
    (.close session))
  IHaveACookie
  (cookie [_] cookie))

(defn message-bus
  ([]
     (message-bus (read-coookie)))
  ([cookie-or-map]
     (if (map? cookie-or-map)
       (let [{:keys [host port]} cookie-or-map
             sf (create-session-factory host port (when *in-vm-only* :invm))]
         (message-bus cookie-or-map sf))
       (message-bus (read-string cookie-or-map))))
  ([m ^ClientSessionFactory session-factory]
     (let [{:keys [host port username password]} m
           s (.createSession
              session-factory username password false true true false 1)]
       (message-bus s session-factory m)))
  ([^ClientSession session session-factory cookie]
     (message-bus
      session session-factory (.createProducer session) (atom {}) cookie))
  ([session session-factory producer consumer-cache cookie]
     (AMessageBus. session session-factory producer consumer-cache cookie)))

(defn send-to [mb name msg & {:as opts}]
  (send-to-fn mb name msg opts))
