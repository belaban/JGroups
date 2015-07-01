#!/bin/bash


##
## This script releases a new JGroups version to Nexus. It generates the local artifacts (in target/staging), uploads
## them to Nexus staging, the closes and releases the upload.
## Note that if there is an existing artifact in Nexus, the script will fail !
##

# Author: Bela Ban


# DIST=../dist
POM=`dirname $0`/../pom.xml

# JAR=`find $DIST -name "jgroups-*.jar" | grep -v source`
# SRC_JAR=`find $DIST -name "jgroups-*.jar" | grep source`

## Release directly, skipping the staging repo
## REPO=https://repository.jboss.org/nexus/content/repositories/releases/

# REPO=https://repository.jboss.org/nexus/service/local/staging/deploy/maven2
# FLAGS="-Dpackaging=jar -DrepositoryId=jboss-releases-repository"


# echo "Deploying $JAR to $REPO"
# mvn deploy:deploy-file -Dfile=$JAR -Durl=$REPO -DpomFile=$POM $FLAGS


# echo "Deploying $SRC_JAR to $REPO"
# mvn deploy:deploy-file -Dfile=$SRC_JAR -Durl=$REPO -DpomFile=$POM -Dclassifier=sources $FLAGS

mvn -f $POM -Dmaven.test.skip=true deploy

