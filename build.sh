#!/bin/sh
# Ant build script for the JGroups project
# The following variables have to be set in the following way
# PATH should include $JAVA_HOME/bin
# Questions: please email filip@filip.net (sourceforge id fhanik)
JG_HOME=.


LIB=lib
CP=${LIB}/ant.jar\;${LIB}/ant-optional.jar\;${LIB}/xercesxmlapi-2.1.0.jar\;${LIB}/xercesimpl-2.1.0.jar\;${LIB}/xalan.jar\;${LIB}/junit.jar



echo "CP is ${CP}"

if [ -n "$JAVA_HOME" ]; then
    if [ -f "$JAVA_HOME/lib/tools.jar" ]; then
	CP=${CP}:${JAVA_HOME}/lib/tools.jar
    fi
else
    echo "WARNING: JAVA_HOME environment variable is not set."
    echo "  If build fails because sun.* classes could not be found"
    echo "  you will need to set the JAVA_HOME environment variable"
    echo "  to the installation directory of java."
fi

java -classpath "${CP}" org.apache.tools.ant.Main -buildfile ${JG_HOME}/build.xml $*
