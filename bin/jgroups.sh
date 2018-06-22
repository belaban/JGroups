# Author: Bela Ban

#!/bin/bash

CURR=`dirname $0`
CURRENT=$CURR/../

JG=${JG-$CURRENT}

LIB=$JG/lib

CP=$JG/classes:$JG/conf

# If this is a bin dist, JARs are in the $JG directory.
if [ ! -d $LIB ]; then
    LIB=$JG
fi;

CP=$CP:$LIB/*

if [ -f $HOME/log4j.properties ]; then
    LOG="-Dlog4j.configuration=file:$HOME/log4j.properties"
fi;

if [ -f $HOME/log4j2.xml ]; then
    LOG="$LOG -Dlog4j.configurationFile=$HOME/log4j2.xml"
fi;

if [ -f $HOME/logging.properties ]; then
    LOG="$LOG -Djava.util.logging.config.file=$HOME/logging.properties"
fi;

#JG_FLAGS="-Djgroups.bind_addr=match-address:192.168.1.*"
JG_FLAGS="$JG_FLAGS -Djava.net.preferIPv4Stack=true"
FLAGS="-server -Xmx1G -Xms500M"
GC="-XX:+UseG1GC"

JMX="-Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=localhost"

#java -Xrunhprof:cpu=samples,monitor=y,interval=5,lineno=y,thread=y -classpath $CP $LOG $JG_FLAGS $FLAGS $JMX  $*

#DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5000"

java -cp $CP $DEBUG $LOG $GC $JG_FLAGS $FLAGS $JMX $JMC  $*

