

CACHE := $(notdir $(wildcard Makefile.include))




all: check
	@(cd src/org/javagroups ; make)
	@(cd tests              ; make)


clean: check
	@(cd src/org/javagroups ; make clean)
	@(cd tests              ; make clean)

wc: ;
	@(find . -name "*.java" |xargs wc -l)


configure:;
	@echo ""
	@echo "Running configuration script:"
	@(rm -f Makefile.include configure.cache ; ./configure.pl)
	@echo "Done. You can now run make to compile the files."
	@echo ""




check:;
ifeq ($(CACHE),Makefile.include)
#	@echo "Makefile.include was found"
else
	@echo ""
	@echo "--------------------------------------------------------------------------"
	@echo "Your make environment has not been set up yet. Run 'make configure' first."
	@echo "--------------------------------------------------------------------------"
	@echo ""
	@(exit 1)
endif

