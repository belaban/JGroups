@echo off

REM Determines the fragmentation size of your system
REM Set FRAG.frag_size and pbcast.NAKACK.max_xmit_size to the value

set CLASSPATH=..\classes

set CP=%CLASSPATH%
set LOG4J=etc/log4j.xml
set L4J=%LOG4J%



java -cp %CP% org.jgroups.tests.DetermineFragSize
