@echo off

REM Determines the interfaces on a machine

set CLASSPATH=..\classes

set CP=%CLASSPATH%

java -cp %CP% org.jgroups.util.GetNetworkInterfaces1_4
