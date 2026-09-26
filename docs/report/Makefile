# Build the academic coursework report.

.PHONY: all clean distclean check

all: main.pdf

main.pdf: main.tex $(wildcard figures/*)
	pdflatex -interaction=nonstopmode main.tex >/dev/null || (cat main.log && exit 1)
	pdflatex -interaction=nonstopmode main.tex >/dev/null || (cat main.log && exit 1)
	@echo "Report compilation complete."
	@echo "  Pages: $$(pdfinfo main.pdf | awk '/^Pages/{print $$2}')"

check: main.pdf
	@if grep -qE 'EG/20XX|TODO|XXXX' main.tex; then \
	  echo "  PLACEHOLDERS REMAIN in main.tex"; \
	  grep -nE 'EG/20XX|TODO|XXXX' main.tex | head; exit 1; \
	else \
	  echo "  No placeholders found. Report is ready for submission."; \
	fi

clean:
	@rm -f *.aux *.log *.out *.toc *.fls *.fdb_latexmk *.synctex.gz

distclean: clean
	@rm -f main.pdf
