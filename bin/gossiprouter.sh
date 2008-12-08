#!/bin/sh
#
# Convenience launcher for the GossipRouter

SEP=":"
case "`uname`" in
    CYGWIN*)
        cygwin=true
        SEP=";"
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

CLASSPATH="$relpath/../classes$SEP$relpath/../conf$SEP$relpath/../lib/commons-logging.jar$SEP$relpath/../lib/log4j.jar"

if [ "$debug" = "true" ]; then
    JAVA_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=router"
fi

if [ "$cygwin" = "true" ]; then
    CLASSPATH=`echo $CLASSPATH | sed -e 's/\;/\\\\;/g'`
fi

#-Djava.util.logging.config.file=./java.logging.config

echo $CLASSPATH
java -classpath $CLASSPATH $JAVA_OPTS org.jgroups.stack.GossipRouter $*

