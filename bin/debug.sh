
# Author: Bela Ban
# version: $Id: debug.sh,v 1.1 2009/04/28 09:42:44 belaban Exp $

#!/bin/bash

JG=${JG-$HOME/JGroups}

LIB=$JG/lib

CP=$JG/classes:$JG/conf

# If this is a bin dist, JARs are in the $JG directory.
if [ ! -d $LIB ]; then
    LIB=$JG
fi;

for i in $LIB/*.jar
do
    CP=$CP:$i
done

if [ -f $HOME:/log4j.properties ]; then
    LOG="-Dlog4j.configuration=file:$HOME/log4j.properties"
fi;

JG_FLAGS="-Dresolve.dns=false -Djgroups.bind_addr=$IP_ADDR -Djboss.tcpping.initial_hosts=$IP_ADDR[7800]"
FLAGS="-server -Xmx600M -Xms600M -Xmn500M"
FLAGS="$FLAGS -XX:CompileThreshold=10000 -XX:+AggressiveHeap -XX:ThreadStackSize=64 -XX:SurvivorRatio=8"
FLAGS="$FLAGS -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=31"
FLAGS="$FLAGS -Djava.net.preferIPv4Stack=true -Djgroups.timer.num_threads=4"
FLAGS="$FLAGS -Xshare:off -XX:+UseBiasedLocking"
JMX="-Dcom.sun.management.jmxremote"
EXPERIMENTAL="-XX:+UseFastAccessorMethods -XX:+UseTLAB -XX:+DoEscapeAnalysis"
DEBUG_OPTS="$JAVA_OPTS -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=n"

java -classpath $CP $LOG $JG_FLAGS $FLAGS $EXPERIMENTAL $DEBUG $JMX  $*
