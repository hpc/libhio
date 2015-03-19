subdirs=template
EXTRA_DIST=doxygen.conf example.c

latex: doxygen.conf template/header.tex template/footer.tex template/style.sty ${top_srcdir}/src/include/hio.h
	doxygen doxygen.conf

latex/refman.pdf: latex
	make -C latex

libhio_api.pdf:latex/refman.pdf
	cp latex/refman.pdf libhio_api.pdf

docs: libhio_api.pdf

clean-local:
	-rm -rf latex html libhio_api.pdf
