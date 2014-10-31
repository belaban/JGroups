@echo off

REM ====================== Script to start JGroups programs ==============================
REM Usage: jgroups.bat demos.Draw -props c:\udp.xml

REM set the value of JG to the root directory in which JGroups is located
set JG=..\

set CP=%JG%\classes\;%JG%\conf\;%JG%\lib\*;%JG%\keystore

set VMFLAGS=-Xmx500M -Xms500M

set LOG=-Dlog4j.configurationFile=%JG%\conf\log4j2.xml

set FLAGS=-Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=match-address:192.168.*

java -classpath %CP% %LOG% %VMFLAGS% %FLAGS% -Dcom.sun.management.jmxremote %*
