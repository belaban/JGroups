### $Id: Makefile,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

include ../Makefile.include


JJ=$(JAVAC) $(JFLAGS)


vpath %.class $(CLASSDIR)



all: subdirs


subdirs:
	 @(cd junit/org/javagroups/service/lease ; make)
	 @(cd junit/org/javagroups/tests         ; make)
	 @(cd other/org/javagroups/tests         ; make)




subdirs-clean:
	 @(cd junit/org/javagroups/service/lease ; make clean)
	 @(cd junit/org/javagroups/tests         ; make clean)
	 @(cd other/org/javagroups/tests         ; make clean)


clean: subdirs-clean
	@rm -fr *~ core*

