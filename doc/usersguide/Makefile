
include ../../src/org/javagroups/Makefile.include 

LATEX=latex
DVIPS=dvips
BIBTEX=bibtex


.SUFFIXES: .tex .dvi .ps .bib


.tex.dvi:; $(LATEX) $<
.dvi.ps:;  $(DVIPS) -f $< > $@


all: UsersGuide.ps


UsersGuide.ps: main.dvi
	$(DVIPS) -f $< > $@

main.dvi: refs.bib main.tex
	$(LATEX)  main
	$(BIBTEX) main
	$(LATEX) main
	$(LATEX) main



clean:
	rm -f *~ *.dvi *.ps *.bbl *.blg *.log *.aux *.gz *.toc
	rm -fr main
