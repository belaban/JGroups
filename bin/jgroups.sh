
# Author: Bela Ban
# version: $Id: jgroups.sh,v 1.5 2008/09/26 10:47:19 belaban Exp $

#!/bin/bash

JG=${JG-$HOME/JGroups}

LIB=$JG/lib

CP=$JG/classes:$JG/conf

for i in $LIB/*.jar
do
    CP=$CP:$i
done

LOG="-Dlog4j.configuration=file:$HOME/log4j.properties"
JG_FLAGS="-Dresolve.dns=false -Djgroups.bind_addr=$IP_ADDR -Djboss.tcpping.initial_hosts=$IP_ADDR[7800]"
FLAGS="-server -Xmx600M -Xms600M"
FLAGS="$FLAGS -XX:CompileThreshold=10000 -XX:+AggressiveHeap -XX:ThreadStackSize=64 -XX:SurvivorRatio=8"
FLAGS="$FLAGS -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=31"
FLAGS="$FLAGS -Djava.net.preferIPv4Stack=true -Djgroups.timer.num_threads=4"
FLAGS="$FLAGS -Xshare:off -XX:+UseBiasedLocking"
JMX="-Dcom.sun.management.jmxremote"
EXPERIMENTAL="-XX:+UseFastAccessorMethods -XX:+UseTLAB"

java -classpath $CP $LOG $JG_FLAGS $FLAGS $EXPERIMENTAL $JMX  $*
