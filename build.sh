#!/bin/bash

PROGNAME=`basename $0`
DIRNAME=`dirname $0`
SED="sed"
ROOT="/"
MVN="mvn"

#  OS specific support (must be 'true' or 'false').
cygwin=false;
darwin=false;
case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;

    Darwin*)
        darwin=true
        ;;
esac

die() {
    echo "${PROGNAME}: $*"
    exit 1
}

warn() {
    echo "${PROGNAME}: $*"
}

update_version_using_property_file() {
    version='undefined'
    property_file=$1
    pom_file=$2
    if [ ! -f "$property_file" ]; then
        die "Property file does not exist!"
    fi

    if [ ! -f "$pom_file" ]; then
        die "Pom file does not exist!"
    fi

    while IFS=$'\n' read -r line || [[ -n "$line" ]]; do
        if [[ $line = jgroups.version* ]]; then
            version=${line#*=}
            break
        fi
    done < $1
    if [[ $version = 'undefined' ]]; then
        die "Version is undefined"
    fi
    $SED -i "0,/<version>.*<\/version>/s//<version>${version}<\/version>/" $pom_file
}

say() {
    if [ $darwin = "true" ]; then
        # On Mac OS, notify via Growl
        which -s growlnotify && growlnotify --name Maven --sticky --message "JGroups build: $@"
    fi
    if [ `uname -s` == "Linux" ]; then
        # On Linux, notify via notify-send
        which notify-send && notify-send "JGroups build: $@"
    fi
}

ulimit_check() {
    if [ $cygwin = "false" ]; then
        HARD_LIMIT=`ulimit -H $1`
        if [[ "$HARD_LIMIT" != "unlimited" && "$HARD_LIMIT" -lt "$2" ]]; then
            warn "The hard limit for $1 is $HARD_LIMIT which is lower than the expected $2. This might be a problem"
        else
            SOFT_LIMIT=`ulimit -S $1`
            if [[ "$SOFT_LIMIT" != "unlimited" && "$SOFT_LIMIT" -lt "$2" ]]; then
                ulimit $1 $2
                if [ $? -ne 0 ]; then
                    warn "Could not set ulimit $1 $2"
                fi
            fi
        fi
    fi
}

#
#  Main function.
#
main() {
    #  If there is a build config file, source it.
    update_version_using_property_file "$DIRNAME/conf/VERSION.properties" "$DIRNAME/pom.xml"

    #  Increase some limits if we can
    ulimit_check -n 1024
    ulimit_check -u 2048

    #  Change to the directory where the script lives, so users are not forced
    #  to be in the same directory as build.xml.
    cd $DIRNAME

    MVN_GOAL="$@";

    #  Default goal if none specified.
    if [ -z "$MVN_GOAL" ]; then MVN_GOAL="install"; fi

    echo "Invoking: $MVN $MVN_GOAL"
    $MVN $MVN_GOAL

    if [ $? -eq 0 ]; then
        say SUCCESS
    else
        say FAILURE
        die $?
    fi
}

##
##  Bootstrap
##
main "$@"

