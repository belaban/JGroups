#!/bin/bash

JG=.
LIB=$JG

CP=$JG/classes:$JG/conf:$LIB/jgroups-all.jar:$LIB/commons-logging.jar:$LIB/log4j.jar:$JG/keystore


FLAGS="$FLAGS -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=192.168.1.5 -Djgroups.tcpping.initial_hosts=192.168.1.5[7800]"

java -Ddisable_canonicalization=false -classpath $CP $LOG $FLAGS -Dcom.sun.management.jmxremote -Dresolve.dns=false org.jgroups.$*
