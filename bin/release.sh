#!/bin/bash

## Script to perform release. Performs the steps listed in README-Release.adoc

CURRENT_DIR=`dirname $0`
BASE_DIR="$CURRENT_DIR/../"
POM=$BASE_DIR/pom.xml
POM2=$BASE_DIR/pom2.xml
CURRENT_VERSION=`grep version $POM | head -1 | sed "s/version//g" |  sed "s/[[<>/]*//g" | tr -d " "`
RELEASE_VERSION=`echo $CURRENT_VERSION | sed "s/-SNAPSHOT//g"`
NEW_VERSION=`$BASE_DIR/bin/jgroups.sh org.jgroups.Version -incr $CURRENT_VERSION`
TARGET="$BASE_DIR/target/"
REPO=`grep nexus.server.url pom.xml | head -1 | sed "s/<nexus.server.url>//g" | sed "s/<\/nexus.server.url>//g" | tr -d " "`
TAG=`echo "jgroups-$CURRENT_VERSION" | sed "s/-SNAPSHOT//g"`

echo ""
echo "release version: $RELEASE_VERSION"

read -p "tag:             $TAG: (enter to accept) " answer
if [ ! -z $answer ]; then
   TAG=$answer
   echo "set new tag to $answer"
fi

read -p "new version:     $NEW_VERSION: (enter to accept) " answer
if [ ! -z $answer ]; then
   NEW_VERSION=$answer
   echo "set new version to $answer"
fi

echo ""
echo "changing version in pom.xml from $CURRENT_VERSION to $RELEASE_VERSION:"
#cat $POM | sed "s/$CURRENT_VERSION/$RELEASE_VERSION/g" > $POM2
#mv $POM2 $POM
mvn -B -q -f $POM versions:set -DnewVersion="$RELEASE_VERSION" -DgenerateBackupPoms=false

echo ""
echo ""
echo "Generating artifacts:"
echo "=============================================================="
mvn -B -q -DskipTests -f $POM clean install -Prelease
echo ""

echo "done, artifacts: "
echo ""
echo "`ls -l $TARGET`"
echo "=============================================================="
echo ""

echo "Release this to the nexus repository ($REPO)?"
read -p "[<enter> to proceed | <ctrl-c> to cancel]" $answer
echo ""
case $answer in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "";;
esac

## uncomment
mvn -B -q -f $POM -DskipTests deploy -Prelease
# echo "Please commit and push your changes"

echo "Was the upload successful? Shall I continue with pushing the tag and setting the new version?"
read -p "[<enter> to proceed | <ctrl-c> to cancel]" $answer
echo ""
case $answer in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "";;
esac

## uncomment
git commit -m 'Changed version from $CURRENT_VERSION to $RELEASE_VERSION' . ; git push

echo ""
echo "================================================================"
echo "Tagging the repo with $TAG"
## uncomment
git tag $TAG
## uncomment
git push --tags

echo ""
echo ""

NEXT_VERSION="$NEW_VERSION-SNAPSHOT"
echo "changing pom.xml to version $NEXT_VERSION:"
#cat $POM | sed "s/$RELEASE_VERSION/$NEXT_VERSION/g" > $POM2
#mv $POM2 $POM
mvn -B -q -f $POM versions:set -DnewVersion="$NEXT_VERSION" -DgenerateBackupPoms=false

## uncomment
"git commit -m 'Changed version from $RELEASE_VERSION to $NEXT_VERSION' . ; git push"
echo ""

echo "--------------------------------------------------------"
echo " Release process completed successfully!"
echo " You have released: $RELEASE_VERSION"
echo " Next development iteration in: $NEXT_VERSION"
echo "--------------------------------------------------------"