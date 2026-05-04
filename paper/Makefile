# Build the Computers and Electronics in Agriculture submission PDF.
#
# Requirements: latexmk + a TeX Live distribution that includes
# microtype, hyperref, cleveref, booktabs, siunitx, natbib, algorithm,
# and algorithmic.
# elsarticle.cls + elsarticle-num.bst are vendored alongside this
# Makefile, so a system install of the Elsevier author kit is not required.

PAPER := csb_cea
LATEXMK := latexmk
LATEXMK_FLAGS := -pdf -interaction=nonstopmode -halt-on-error -file-line-error \
	-bibtex -use-make

.PHONY: install build pdf clean distclean

install:
	$(MAKE) -C .. install

build: $(PAPER).pdf

pdf: build

$(PAPER).pdf: $(PAPER).tex csb.bib elsarticle.cls elsarticle-num.bst $(wildcard sty/*.sty)
	TEXINPUTS="./sty:$$TEXINPUTS" \
	BSTINPUTS="./:./sty:$$BSTINPUTS" \
	$(LATEXMK) $(LATEXMK_FLAGS) $(PAPER).tex

clean:
	$(LATEXMK) -c $(PAPER).tex || true
	rm -f $(PAPER).bbl $(PAPER).run.xml $(PAPER).spl $(PAPER).synctex.gz \
		$(PAPER).aux $(PAPER).log $(PAPER).out $(PAPER).fls \
		$(PAPER).fdb_latexmk $(PAPER).blg

distclean: clean
	rm -f $(PAPER).pdf
