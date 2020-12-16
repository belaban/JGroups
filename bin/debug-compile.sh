#!/bin/bash

## Creates a native image using the GraalVM compiler (needs to be on the path)


## --features=org.graalvm.home.HomeFinderFeature --no-server --debug-attach=*:5000 --no-fallback
# --allow-incomplete-classpath -cp ./classes/:./lib/* --initialize-at-build-time= -Dlog4j2.disable.jmx=true

CLASSPATH="./classes/:./lib/*"

OPTIONS="--features=org.graalvm.home.HomeFinderFeature --no-server --no-fallback --allow-incomplete-classpath"


OPTIONS="$OPTIONS --debug-attach=*:5000 --initialize-at-build-time= -Dlog4j2.disable.jmx=true"

#OPTIONS="$OPTIONS --trace-object-instantiation=java.net.Inet4Address"


native-image -cp $CLASSPATH $OPTIONS $*