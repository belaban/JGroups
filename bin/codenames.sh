#!/bin/bash

## Prints the codenames of all tagged JGroups versions

TMPFILE=/tmp/versions.txt
CODENAME=codename
VER=./conf/JGROUPS_VERSION.properties

if [[ -z "${JGROUPS_HOME}" ]];
  then echo "$JGROUPS_HOME not set" ; exit 1
fi

cd $JGROUPS_HOME

for i in $(git tag)
  do
    # echo "checking $i"
    git show $i:$VER >>$TMPFILE 2>&1
    # git show $i:$VER | grep codename | sort | uniq
  done

cat $TMPFILE | grep $CODENAME | grep -v fatal | sort | uniq


cd -
rm -f $TMPFILE
