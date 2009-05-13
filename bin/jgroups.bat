@echo off

REM ====================== Script to start JGroups programs ==============================
REM Usage: jgroups.bat demos.Draw -props c:\udp.xml

REM set the value of JG to the root directory in which JGroups is located
set JG=.
set LIB=%JG%

set CP=%JG%\classes\;%JG%\conf\;%LIB%\jgroups-all.jar\;%LIB%\log4j.jar\;%JG%\keystore

set VMFLAGS=-Xmx500M -Xms500M -XX:NewRatio=1 -XX:+AggressiveHeap -verbose:gc -XX:+DisableExplicitGC -XX:ThreadStackSize=32 -XX:CompileThreshold=100

rem LOG="-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.Jdk14Logger -Djava.util.logging.config.file=c:\logging.properties"
set LOG=-Dlog4j.configuration=file:c:\log4j.properties

set FLAGS=-Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=192.168.1.5 -Djgroups.tcpping.initial_hosts=127.0.0.1[7800]

java -Ddisable_canonicalization=false -classpath %CP% %LOG% %VMFLAGS% %FLAGS% -Dcom.sun.management.jmxremote -Dresolve.dns=false org.jgroups.%*
