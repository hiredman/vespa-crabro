(defproject vespa-crabro "1.0.0-SNAPSHOT"
  :description "easy distribution for clojure"
  :repositories {"jboss" "https://repository.jboss.org/nexus/content/repositories/releases/"}
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.hornetq/hornetq-core "2.2.2.Final"]
                 [org.hornetq/hornetq-core-client "2.2.2.Final"]
                 [org.hornetq/hornetq-transports "2.1.0.r9031"]
                 [org.jboss.netty/netty "3.2.0.Final"]
                 [commons-codec "1.5"]]
  :main vespa.server)
