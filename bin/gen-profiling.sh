#!/bin/bash

## Generates a byteman script for profile data

# Add classpath of classes for which to generate rules, e.g.:
# java -cp "/Users/bela/IspnPerfTest/target/libs/*" org.jgroups.util.GenerateProfilingScript $*

java org.jgroups.util.GenerateProfilingScript $*