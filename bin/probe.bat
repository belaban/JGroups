@echo off

REM Discovers all UDP-based members running on a certain mcast address (use -help for help)
REM Probe [-help] [-addr <addr>] [-port <port>] [-ttl <ttl>] [-timeout <timeout>]


set CLASSPATH=..\classes

set LIB=..\lib

set LIBS=%LIB%\log4j-1.2.6.jar;%LIB%\commons-logging.jar;%LIB%\concurrent.jar


set CP=%CLASSPATH%;%LIBS%


java -cp %CP% org.jgroups.tests.Probe %*
