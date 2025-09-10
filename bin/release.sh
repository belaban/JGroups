#!/bin/bash

## Script to perform release. Performs the steps listed in README-Release.adoc

CURRENT_DIR=`dirname $0`
BASE_DIR="$CURRENT_DIR/../"
POM=$BASE_DIR/pom.xml
POM2=$BASE_DIR/pom2.xml
CURRENT_VERSION=`grep version $POM | head -1 | sed "s/version//g" |  sed "s/[[<>/]*//g" | tr -d " "`
NEW_VERSION=`$BASE_DIR/bin/jgroups.sh org.jgroups.Version -incr $CURRENT_VERSION`
TARGET="$BASE_DIR/target/"
REPO=`grep nexus.server.url pom.xml | head -1 | sed "s/<nexus.server.url>//g" | sed "s/<\/nexus.server.url>//g" | tr -d " "`
NEW_TAG="jgroups-$NEW_VERSION"

echo ""
echo "current version $CURRENT_VERSION"
read -p "new version:    $NEW_VERSION: (enter to accept) " answer
if [ ! -z $answer ]; then
   NEW_VERSION=$answer
   echo "set new version to $answer"
fi

read -p "new tag:        $NEW_TAG: (enter to accept) " answer
if [ ! -z $answer ]; then
   NEW_TAG=$answer
   echo "set new tag to $answer"
fi

echo ""
echo "changing pom.xml to version $NEW_VERSION:"
cat $POM | sed "s/$CURRENT_VERSION/$NEW_VERSION/g" > $POM2
mv $POM2 $POM

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

read -p "Release this to the nexus repository ($REPO)? [Yyn]" $answer
case $answer in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "";;
esac
echo "execute: mvn -DskipTests deploy -Prelease"
echo "Please commit and push your changes"
echo "git commit -m 'Changed version to $NEW_VERSION' ; git push"

echo ""
echo "================================================================"
echo "Tagging the repo with $NEW_TAG"
echo "git tag $NEW_TAG"
echo "git push --tags"

echo ""
echo ""

NEXT_VERSION="$NEW_VERSION-SNAPSHOT"
echo "changing pom.xml to version $NEXT_VERSION:"
cat $POM | sed "s/$NEW_VERSION/$NEXT_VERSION/g" > $POM2
mv $POM2 $POM

echo "Please commit and push the change: git commit -m 'Changed version to $NEXT_VERSION' ; git push"
echo ""