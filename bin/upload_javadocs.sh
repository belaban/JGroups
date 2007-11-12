#!/bin/sh
#
# Uploads the javadoc to SourceForge

scp -r ../dist/javadoc/* belaban@shell.sf.net:/home/groups/j/ja/javagroups/htdocs/javagroupsnew/docs/javadoc/
