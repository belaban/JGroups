#!/bin/sh

CP=../classes:../conf

for i in ../lib/*.jar
  do CP=$CP:$i
done

OPTS="-Dlog4j.configuration=file:$HOME/log4j.properties -Djava.net.preferIPv4Stack=true"
OPTS="$OPTS -Dcom.sun.management.jmxremote"

java $OPTS -classpath $CLASSPATH $JAVA_OPTS org.jgroups.stack.GossipRouter -port 12001 $*

