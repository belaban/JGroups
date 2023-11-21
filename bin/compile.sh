#!/bin/bash

## Creates a native image using the GraalVM compiler (needs to be on the path)

LIB=`dirname $0`/../lib
CLASSES=`dirname $0`/../classes
CONF=`dirname $0`/../conf
CLASSPATH="$CLASSES:$LIB/*"

OPTIONS="$OPTIONS --no-fallback"
OPTIONS="$OPTIONS -Dgraal.CompilationFailureAction=Diagnose"

#OPTIONS="$OPTIONS --debug-attach=*:8000"

OPTIONS="$OPTIONS --initialize-at-build-time="
OPTIONS="$OPTIONS -Dlog4j2.disable.jmx=true" ## Prevents log4j2 from creating an MBeanServer

#OPTIONS="$OPTIONS --initialize-at-run-time=com.sun.jmx.mbeanserver.JmxMBeanServer"

#OPTIONS="$OPTIONS --initialize-at-run-time=org.jgroups.protocols.FD_SOCK"

native-image -cp $CLASSPATH $OPTIONS $*