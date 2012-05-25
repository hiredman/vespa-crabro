(defproject vespa-crabro "1.0.0-SNAPSHOT"
  :description "easy distribution for clojure"
  :repositories {"jboss4" "http://repository.jboss.org/nexus/content/groups/public-jboss/"
                 "hornetq" "http://hornetq.s3.amazonaws.com/"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [org.hornetq/hornetq-core "2.3.0-SNAPSHOT"]
                 [org.hornetq/hornetq-core-client "2.3.0-SNAPSHOT"]
                 [org.hornetq/hornetq-transports "2.1.0.r9031"]
                 [org.hornetq.rest/hornetq-rest "2.3.0-SNAPSHOT"]
                 [org.jboss.netty/netty "3.2.0.Final"]
                 [org.jboss.resteasy/resteasy-jaxrs "1.2.1.GA"]
                 [javax.servlet/servlet-api "2.5"]
                 [tjws/webserver "1.3.3"]
                 [commons-codec "1.5"]]
  :main vespa.server)
