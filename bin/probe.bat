@echo off

REM Discovers all UDP-based members running on a certain mcast address (use -help for help)
REM Probe [-help] [-addr <addr>] [-port <port>] [-ttl <ttl>] [-timeout <timeout>]


jgroups.bat org.jgroups.tests.Probe %*
