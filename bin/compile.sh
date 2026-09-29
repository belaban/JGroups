#!/bin/bash

## Creates a native image using the GraalVM compiler (needs to be on the path)

CLASSES=`dirname $0`/../classes
CLASSPATH="$CLASSES"

OPTIONS="$OPTIONS --no-fallback"
OPTIONS="$OPTIONS -Djdk.graal.CompilationFailureAction=Diagnose"

#OPTIONS="$OPTIONS --debug-attach=*:8000"

OPTIONS="$OPTIONS --initialize-at-build-time="
OPTIONS="$OPTIONS -Dlog4j2.disable.jmx=true" ## Prevents log4j2 from creating an MBeanServer

native-image -cp $CLASSPATH $OPTIONS $*