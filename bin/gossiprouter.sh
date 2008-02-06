#!/bin/sh

CP=../classes:../conf

for i in ../lib/*.jar
  do CP=$CP:$i
done


java -classpath $CLASSPATH $JAVA_OPTS org.jgroups.stack.GossipRouter -port 12001 $*

