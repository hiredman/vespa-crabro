(ns vespa.remoting
  (:import (org.hornetq.api.core TransportConfiguration SimpleString)
           (org.hornetq.api.core.client HornetQClient)
           (org.hornetq.core.remoting.impl.invm InVMAcceptorFactory
                                                InVMConnectorFactory)))

(defn in-vm-locator [opts]
  (doto (HornetQClient/createServerLocatorWithoutHA
         (into-array
          TransportConfiguration
          [(TransportConfiguration.
            (.getName InVMConnectorFactory)
            opts)]))
    (.setReconnectAttempts -1)))
