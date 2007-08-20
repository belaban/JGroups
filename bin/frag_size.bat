@echo off

REM Determines the fragmentation size of your system

set CLASSPATH=..\classes

set CP=%CLASSPATH%
set LOG4J=etc/log4j.xml
set L4J=%LOG4J%



java -cp %CP% org.jgroups.tests.DetermineFragSize
