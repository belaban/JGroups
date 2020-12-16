#!/bin/bash

## Creates a native image using the GraalVM compiler (needs to be on the path)


OPTIONS="-H:+JNI --no-server -H:+ReportExceptionStackTraces --features=org.graalvm.home.HomeFinderFeature"

OPTIONS="$OPTIONS -H:+AllowVMInspection -H:TraceClassInitialization=true --no-fallback --allow-incomplete-classpath"

OPTIONS="$OPTIONS -H:ReflectionConfigurationFiles=conf/reflection.json"

# OPTIONS="$OPTIONS -H:+PrintAnalysisCallTree"


OPTIONS="$OPTIONS -Dgraal.CompilationFailureAction=Diagnose"

# OPTIONS="$OPTIONS -H:IncludeResources=/home/bela/logging.properties -Dfoo=bar -Dcom.sun.management.jmxremote"

#OPTIONS="$OPTIONS --debug-attach=*:8000"

#OPTIONS="$OPTIONS -J-server -J-XX:+UseG1GC -J-XX:+UseAdaptiveSizePolicy -J-XX:MinHeapFreeRatio=20 -J-XX:MaxHeapFreeRatio=20"

OPTIONS="$OPTIONS --initialize-at-build-time="

OPTIONS="$OPTIONS -Dlog4j2.disable.jmx=true" ## Prevents log4j2 from creating an MBeanServer

#OPTIONS="$OPTIONS -H:GenerateDebugInfo=1"

#OPTIONS="$OPTIONS --initialize-at-run-time=com.sun.jmx.mbeanserver.JmxMBeanServer"

#OPTIONS="$OPTIONS --initialize-at-run-time=org.jgroups.protocols.FD_SOCK"

native-image -cp $CLASSPATH $OPTIONS $*