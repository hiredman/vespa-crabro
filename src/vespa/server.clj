(ns vespa.server
  (:use [vespa.crabro :only [create-server *hornetq-server* message-bus create-queue in-vm-locator
                             read-coookie]]
        [vespa.protocols]
        [vespa.logging :only [log]])
  (:import (org.hornetq.rest.integration EmbeddedRestHornetQ)
           (org.hornetq.rest.queue QueueDeployment DestinationSettings QueueServiceManager)
           (org.hornetq.rest MessageServiceManager MessageServiceConfiguration)
           (org.jboss.resteasy.plugins.server.tjws TJWSEmbeddedJaxrsServer)
           (org.hornetq.rest.util TimeoutTask
                                  LinkHeaderLinkStrategy)
           (java.util.concurrent Executors)
           (org.hornetq.api.core.client ClientSessionFactory ClientSession))
  (:gen-class))

(defn service-manager []
  (let [{:keys [username password]} (read-coookie) 
        config (MessageServiceConfiguration.)
        pool (Executors/newCachedThreadPool)
        tot (TimeoutTask. (.getTimeoutTaskInterval config))
        defaults (doto (DestinationSettings.)
                   (.setConsumerSessionTimeoutSeconds 60)
                   (.setDuplicatesAllowed (.isDupsOk config))
                   (.setDurableSend (.isDefaultDurableSend config)))
        loc (in-vm-locator {"server-id" "0"})]
    (when (not= -1 (.getConsumerWindowSize config))
      (.setConsumerWindowSize loc (.getConsumerWindowSize config)))
    (.execute pool tot)
    (let [session-factory (.createSessionFactory loc)
          fake-factory (reify
                         ClientSessionFactory
                         (^ClientSession createSession [_]
                           ^ClientSession (.createSession session-factory
                                                          username
                                                          password
                                                          false
                                                          false
                                                          false
                                                          false
                                                          1))
                         (^ClientSession createSession [_ ^boolean ac-sends ^boolean ac-acks]
                           ^ClientSession (.createSession session-factory
                                                          username
                                                          password
                                                          false
                                                          ac-sends
                                                          ac-acks
                                                          false
                                                          1))
                         (^ClientSession createSession [_ ^boolean _ ^boolean _ ^boolean _]
                           ^ClientSession (.createSession session-factory
                                                          username
                                                          password
                                                          false
                                                          false
                                                          false
                                                          false
                                                          1)))
          link-strart (LinkHeaderLinkStrategy.)]
      {:queue-manager (doto (QueueServiceManager.)
                        (.setServerLocator loc)
                        (.setSessionFactory fake-factory)
                        (.setTimeoutTask tot)
                        (.setConsumerServerLocator loc)
                        (.setConsumerSessionFactory fake-factory)
                        (.setDefaultSettings defaults)
                        (.setPushStoreFile (.getQueuePushStoreDirectory config))
                        (.setProducerPoolSize (.getProducerSessionPoolSize config))
                        (.setLinkStrategy link-strart))})))

(defn- -main [& args]
  (try
    (add-watch log :stdout (fn [k r os obj] (locking #'*out* (println (map str obj)))))
    (let [tjws (doto (TJWSEmbeddedJaxrsServer.)
                 (.setRootResourcePath "")
                 (.setSecurityDomain nil)
                 (.setPort 3000))
          hornetq (apply create-server (map read-string args))
          {:keys [queue-manager]} (service-manager)]
      (.start tjws)
      #_(.start manager)
      (.start queue-manager)
      (-> tjws .getDeployment .getRegistry (.addSingletonResource (.getDestination queue-manager)))
      (with-open [mb (message-bus)]
        (create-queue mb "stupid.queue")
        (while true
          (receive-from mb "stupid.queue" prn))))
    (catch Exception e
      (.printStackTrace e))))
