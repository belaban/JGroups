## $Id: Makefile,v 1.1.1.1 2003/09/09 01:24:11 belaban Exp $

include ../../../../Makefile.include

vpath %.class $(CLASSDIR)/service



CLASSES=AbstractService.class \
        ServiceRunner.class




this: $(CLASSES) subdirs


subdirs:;
	@(cd lease ; make)


clean: subdirs-clean
	@rm -f $(CLASSDIR)/service/*.class *~


subdirs-clean:;
	@(cd lease ; make clean)






