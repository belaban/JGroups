

@echo off
REM This script assumes that tools.jar is already in the classpath

if "%JAVA_HOME%" == "" goto noJavaHome


set LIB=lib

set LIBS=%LIB%\log4j.jar

set LIBS=%LIB%\ant.jar;%LIB%\ant-junit.jar;%LIB%\ant-launcher.jar;%LIB%\junit.jar;%LIB%\xalan.jar;%LIB%\serializer.jar;
REM echo LIBS=%LIBS%

set CP=%JAVA_HOME%\lib\tools.jar;%JAVA_HOME%\jre\lib\rt.jar;%CLASSPATH%;%LIBS%
%JAVA_HOME%\bin\java -classpath "%CP%" org.apache.tools.ant.Main -buildfile build.xml %1 %2 %3 %4 %5 %6 %7 %8 %9

goto endOfFile

:noJavaHome
if "%_JAVACMD%" == "" set _JAVACMD=java
echo.
echo Warning: JAVA_HOME environment variable is not set.
echo   If build fails because sun.* classes could not be found
echo   you will need to set the JAVA_HOME environment variable
echo   to the installation directory of java.
echo.

:endOfFile
