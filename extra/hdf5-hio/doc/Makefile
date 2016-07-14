.PHONY: html latex
all: html latex

html:
	doxygen doxygen.conf

latex:
	make -C latex
	cp latex/refman.pdf hdf5_hio.pdf

clean:
	rm -rf latex html hdf5_hio.pdf *~
