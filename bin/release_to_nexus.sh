#!/bin/bash


# Uploads the artifacts in ./dist (JAR and src JAR) to the Nexus Maven repo at repository.jboss.org/nexus
# The artifacts will be in the staging repo, go to repository.jboss.org/nexus and promote them to the releases repo in
# the next step

# Author: Bela Ban
# version: $Id: release_to_nexus.sh,v 1.6 2010/04/30 12:29:51 belaban Exp $


DIST=../dist
POM=../pom.xml

JAR=`find $DIST -name "jgroups-*.jar" | grep -v source`
SRC_JAR=`find $DIST -name "jgroups-*.jar" | grep source`

REPO=https://repository.jboss.org/nexus/service/local/staging/deploy/maven2


echo "Deploying $JAR to $REPO"
mvn deploy:deploy-file -Dfile=$JAR -Durl=$REPO -DpomFile=$POM -DrepositoryId=jboss-releases-repository


echo "Deploying $SRC_JAR to $REPO"
mvn deploy:deploy-file -Dfile=$SRC_JAR -Durl=$REPO -DpomFile=$POM -Dclassifier=sources -DrepositoryId=jboss-releases-repository

