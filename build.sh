#!/bin/sh
# Ant build script for the JGroups project
# The following variables have to be set in the following way
# PATH should include $JAVA_HOME/bin
JG_HOME=.


case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;

    Darwin*)
        darwin=true
        ;;
esac



LIB=lib


#if [ "$cygwin" = "true" ]; then
#    CP=${LIB}/ant.jar\;${LIB}/ant-launcher.jar\;${LIB}/ant-junit.jar\;${LIB}/xalan.jar\;${LIB}/junit.jar
#else
#    CP=${LIB}/ant.jar:${LIB}/ant-launcher.jar:${LIB}/ant-junit.jar:${LIB}/xalan.jar:${LIB}/junit.jar
#fi


if [ "$cygwin" = "true" ]; then
    for i in ${LIB}/*.jar
        do
           CP=${CP}${i}\;
        done
else
    for i in ${LIB}/*.jar
        do
           CP=${CP}${i}:
        done
fi



if [ -n "$JAVA_HOME" ]; then
    if [ -f "$JAVA_HOME/lib/tools.jar" ]; then
        if [ "$cygwin" = "true" ]; then
            CP=${CP}\;${JAVA_HOME}/lib/tools.jar
        else
            CP=${CP}:${JAVA_HOME}/lib/tools.jar
        fi
    fi
else
    echo "WARNING: JAVA_HOME environment variable is not set."
    echo "  If build fails because sun.* classes could not be found"
    echo "  you will need to set the JAVA_HOME environment variable"
    echo "  to the installation directory of java."
fi


java -classpath "${CP}" org.apache.tools.ant.Main -buildfile ${JG_HOME}/build.xml $*
#echo "CP is ${CP}"
