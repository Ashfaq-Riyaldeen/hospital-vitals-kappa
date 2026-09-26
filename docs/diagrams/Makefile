# Compile TikZ diagram sources to vector PDFs and copy to report figures directory.

FIGURES_DIR := ../report/figures
DIAGRAMS := D1-context D2-layered-architecture D3-event-sequence D4-replay-branching
PDFS := $(patsubst %,%.pdf,$(DIAGRAMS))

.PHONY: all clean

all: $(PDFS)
	@mkdir -p $(FIGURES_DIR)
	@cp $(PDFS) $(FIGURES_DIR)/
	@echo "Copied vector diagrams to $(FIGURES_DIR)"

%.pdf: %.tex kappastyle.tex
	pdflatex -interaction=nonstopmode $< >/dev/null || (cat $*.log && exit 1)

clean:
	@rm -f *.aux *.log *.pdf
