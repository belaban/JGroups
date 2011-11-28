#!/bin/sh
#
# Uploads the javadoc to SourceForge
# Author: Bela Ban
# Version: $Id: upload_javadocs.sh,v 1.1.2.3 2008/10/22 10:15:49 belaban Exp $

scp -r ../dist/javadoc/* belaban,javagroups@web.sourceforge.net:/home/groups/j/ja/javagroups/htdocs/javagroupsnew/docs/javadoc/
