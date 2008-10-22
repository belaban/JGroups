#!/bin/sh
#
# Uploads the manual to SourceForge

scp -r ../doc/manual/build/en/* belaban,javagroups@web.sourceforge.net:/home/groups/j/ja/javagroups/htdocs/javagroupsnew/docs/manual
scp -r ../doc/tutorial/build/en/* belaban,javagroups@web.sourceforge.net:/home/groups/j/ja/javagroups/htdocs/javagroupsnew/docs/tutorial
