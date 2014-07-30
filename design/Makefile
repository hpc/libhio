all: design.pdf design.ps

design.ps: design.dvi
design.pdf: design.dvi
	dvipdf design

design.dvi: design.tex
	latex design
