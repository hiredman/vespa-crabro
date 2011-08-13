(ns vespa.rest
  (:use [vespa.protocols]
        [vespa.logging :only [log]]
        [vespa.remoting :only [in-vm-locator]])
  (:import (org.hornetq.rest.integration EmbeddedRestHornetQ)
           (org.hornetq.rest.queue QueueDeployment DestinationSettings QueueServiceManager)
           (org.hornetq.rest MessageServiceManager MessageServiceConfiguration)
           (org.jboss.resteasy.plugins.server.tjws TJWSEmbeddedJaxrsServer)
           (org.hornetq.rest.util TimeoutTask
                                  LinkHeaderLinkStrategy)
           (java.util.concurrent Executors)
           (java.io Closeable)
           (org.hornetq.api.core.client ClientSessionFactory ClientSession)))

(defn service-manager [username password]
  (let [config (MessageServiceConfiguration.)
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

(defn rest-server [hornetq username password]
  (let [tjws (doto (TJWSEmbeddedJaxrsServer.)
               (.setRootResourcePath "")
               (.setSecurityDomain nil)
               (.setPort 3000))
        {:keys [queue-manager]} (service-manager username password)]
    (.start tjws)
    (.start queue-manager)
    (-> tjws .getDeployment .getRegistry (.addSingletonResource (.getDestination queue-manager)))
    (reify
      Closeable
      (close [_]
        (.stop tjws)
        (.stop queue-manager)))))
