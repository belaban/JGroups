#!/bin/sh

# Runs a single test class from command line, circumventing ant altogether. Useful for
# quick debugging.

TESTCLASS=org.jgroups.blocks.MethodCallTest

reldir=`dirname $0`


# OS specific support (must be 'true' or 'false').
cygwin=false;
case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;
esac

if [ $cygwin = true ]; then
    SEP=";"
else
    SEP=":"
fi

CLASSPATH="$reldir/../classes${SEP}\
$reldir/../conf${SEP}\
$reldir/../lib/junit.jar${SEP}\
$reldir/../lib/log4j-1.2.6.jar${SEP}\
$reldir/../lib/commons-logging.jar"

#if [ $cygwin = "true" ]; then
#   CP=`cygpath -wp $CLASSPATH`
#else
#   CP=$CLASSPATH
#fi

#echo $CLASSPATH
java -cp $CLASSPATH junit.textui.TestRunner $TESTCLASS
