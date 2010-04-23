#!/bin/bash


# Uploads the artifacts in ./dist (JAR and src JAR) to the Nexus Maven repo at repository.jboss.org/nexus
# The artifacts will be in the staging repo, go to repository.jboss.org/nexus and promote them to the releases repo in
# the next step

# Author: Bela Ban
# version: $Id: release_to_nexus.sh,v 1.1 2010/04/23 06:51:58 belaban Exp $


DIST=../dist
POM=../pom.xml

JAR=`find $DIST -name "jgroups-*.jar" | grep -v source`
SRC_JAR=`find $DIST -name "jgroups-*.jar" | grep source`

REPO=https://repository.jboss.org/nexus

echo "DIST is $DIST"
echo "POM is $POM"
echo "JAR $JAR"
echo "SRC_JAR is $SRC_JAR"

echo "Deploying $JAR to $REPO"
# mvn deploy:deploy-file -Dfile=$JAR -Durl=file://$REPO -DpomFile=$POM

echo "Deploying $SRC_JAR to $REPO"
# mvn deploy:deploy-file -Dfile=$SRC_JAR -Durl=file://$REPO \
#                              -DpomFile=$POM -Dclassifier=sources
