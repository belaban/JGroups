
# Author: Bela Ban
# version: $Id: jgroups.sh,v 1.8 2009/04/27 15:34:13 radoslavhusar Exp $

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

LOG="-Dlog4j.configuration=file:$HOME/log4j.properties"
JG_FLAGS="-Dresolve.dns=false -Djgroups.bind_addr=$IP_ADDR -Djboss.tcpping.initial_hosts=$IP_ADDR[7800]"
FLAGS="-server -Xmx600M -Xms600M -Xmn500M"
FLAGS="$FLAGS -XX:CompileThreshold=10000 -XX:+AggressiveHeap -XX:ThreadStackSize=64 -XX:SurvivorRatio=8"
FLAGS="$FLAGS -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=31"
FLAGS="$FLAGS -Djava.net.preferIPv4Stack=true -Djgroups.timer.num_threads=4"
FLAGS="$FLAGS -Xshare:off -XX:+UseBiasedLocking"
JMX="-Dcom.sun.management.jmxremote"
EXPERIMENTAL="-XX:+UseFastAccessorMethods -XX:+UseTLAB -XX:+DoEscapeAnalysis"

java -classpath $CP $LOG $JG_FLAGS $FLAGS $EXPERIMENTAL $JMX  $*
