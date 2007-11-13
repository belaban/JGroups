#!/bin/sh
#
# Uploads the javadoc to SourceForge
# Author: Bela Ban
# Version: $Id: upload_javadocs.sh,v 1.1.2.2 2007/11/13 07:28:46 belaban Exp $

scp -r ../dist/javadoc/* belaban@shell.sf.net:/home/groups/j/ja/javagroups/htdocs/javagroupsnew/docs/javadoc/
