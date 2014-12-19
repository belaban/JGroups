#!/bin/bash


# Uploads the artifacts in ./dist (JAR and src JAR) to the Nexus Maven repo at repository.jboss.org/nexus
# The artifacts will be in the staging repo, go to repository.jboss.org/nexus and promote them to the releases repo in
# the next step

# Author: Bela Ban


DIST=../dist
POM=../pom.xml

JAR=`find $DIST -name "jgroups-*.jar" | grep -v source`
SRC_JAR=`find $DIST -name "jgroups-*.jar" | grep source`

## Release directly, skipping the staging repo
## REPO=https://repository.jboss.org/nexus/content/repositories/releases/

REPO=https://repository.jboss.org/nexus/service/local/staging/deploy/maven2
FLAGS="-Dpackaging=jar -DrepositoryId=jboss-releases-repository"


echo "Deploying $JAR to $REPO"
mvn deploy:deploy-file -Dfile=$JAR -Durl=$REPO -DpomFile=$POM $FLAGS


echo "Deploying $SRC_JAR to $REPO"
mvn deploy:deploy-file -Dfile=$SRC_JAR -Durl=$REPO -DpomFile=$POM -Dclassifier=sources $FLAGS


