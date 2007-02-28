@rem Convenience launcher for the Draw demo (contributed by Laran Evans lc278@cornell.edu)
@echo off

set CPATH=../classes;../conf;../lib/commons-logging.jar;../lib/log4j.jar;../lib/log4j-1.2.6.jar;../lib/concurrent.jar;../conf/log4j.properties

set JAVA_OPTS=
if -debug==%1 set JAVA_OPTS=-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=jgc1

if "%LOCALHOSTIP%"=="" echo Warning: You should set environment variable 'LOCALHOSTIP' to your local ip address before running this script.
if "%LOCALHOSTIP%"=="" set LOCALHOSTIP=192.168.1.103
    
@echo on
java -classpath %CPATH% %JAVA_OPTS% -Djgroups.bind_addr=%LOCALHOSTIP% -Djgroups.tcpping.initial_hosts=%LOCALHOSTIP%[7800],%LOCALHOSTIP%[7801] org.jgroups.demos.Draw -props c:\jboss\JGroups\conf\tcp-nio.xml