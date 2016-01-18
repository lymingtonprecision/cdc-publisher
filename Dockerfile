FROM develar/java:8u45
MAINTAINER Adam Harper <docker@adam-harper.com>

COPY target/cdc-publisher-standalone.jar /cdc-publisher.jar

ENV JVM_FLAGS "-server -d64 -XX:+UseParallelGC -Xmx512M -XX:MaxInlineSize=1024 -XX:FreqInlineSize=1024"

CMD ["-jar", "/cdc-publisher.jar"]
