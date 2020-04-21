#!/bin/bash

 mvn clean -DskipTests -Dmaven.test.skip=true release:prepare release:perform

