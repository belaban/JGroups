#!/bin/sh
#
# Convenience launcher for the GossipRouter

case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;

    Darwin*)
        darwin=true
        ;;
esac
relpath=`dirname $0`

while [ "$1" != "" ]; do
    if [ "$1" = "-debug" ]; then
        debug=true
    fi
    shift
done

CLASSPATH="$relpath/../classes;$relpath/../conf;$relpath/../lib/commons-logging.jar;$relpath/../lib/log4j-1.2.6.jar"

if [ "$debug" = "true" ]; then
    JAVA_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=router"
fi

if [ "$cygwin" = "true" ]; then
    CLASSPATH=`echo $CLASSPATH | sed -e 's/\;/\\\\;/g'`
fi

#-Djava.util.logging.config.file=./java.logging.config
java -classpath $CLASSPATH $JAVA_OPTS org.jgroups.stack.GossipRouter -port 5556

