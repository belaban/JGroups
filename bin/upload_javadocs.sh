#!/bin/sh
#
# Uploads the javadoc to SourceForge

scp -r ../dist/javadoc/* belaban,javagroups@web.sourceforge.net:/home/groups/j/ja/javagroups/htdocs/javagroupsnew/docs/javadoc/
