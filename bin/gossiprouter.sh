#!/bin/sh

`dirname $0`/jgroups.sh org.jgroups.stack.GossipRouter -port 12001 $*

