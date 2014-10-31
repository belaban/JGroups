@echo off

REM ====================== Script to start JGroups programs ==============================
REM Usage: jgroups.bat demos.Draw -props c:\udp.xml

REM set the value of JG to the root directory in which JGroups is located
set JG=..\

set CP=%JG%\classes\;%JG%\conf\;%JG%\lib\*;%JG%\keystore

set VMFLAGS=-Xmx500M -Xms500M

rem LOG="-Dorg.apache.commons.logging.Log=org.apache.commons.logging.impl.Jdk14Logger -Djava.util.logging.config.file=c:\logging.properties"
set LOG=-Dlog4j.configuration=file:c:\log4j.properties

set FLAGS=-Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=match-address:192.168.*

java -classpath %CP% %LOG% %VMFLAGS% %FLAGS% -Dcom.sun.management.jmxremote %*
