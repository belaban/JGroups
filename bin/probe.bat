@echo off

REM Discovers all UDP-based members running on a certain mcast address (use -help for help)
REM Probe [-help] [-addr <addr>] [-port <port>] [-ttl <ttl>] [-timeout <timeout>]


set CLASSPATH=..\classes

set CP=%CLASSPATH%


java -cp %CP% org.jgroups.tests.Probe %*
