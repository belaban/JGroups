#!/bin/sh
#
# Uploads the manual to SourceForge

scp -r ../doc/manual/build/en/* belaban@shell.sf.net:/home/groups/j/ja/javagroups/htdocs/javagroupsnew/docs/manual
