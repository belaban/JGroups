#!/bin/bash

mvn clean release:prepare -Darguments="-DskipTests=true -Dmaven.skip.javadoc=true"

