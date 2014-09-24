subdirs=template
EXTRA_DIST=doxygen.conf example.c

latex: doxygen.conf
	doxygen doxygen.conf

docs: latex
	make -C latex
	cp latex/refman.pdf libhio_api.pdf

