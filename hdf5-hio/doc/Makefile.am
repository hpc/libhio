subdirs=template
EXTRA_DIST=doxygen.conf.in

if HAVE_PDFLATEX
EXTRA_DIST += libhdf5hio_api.pdf
endif

all: libhdf5hio_api.pdf

libhdf5hio_api.pdf:latex/refman.pdf
	cp latex/refman.pdf libhdf5hio_api.pdf

latex/refman.pdf: latex
	make -C latex

latex: doxygen.conf template/header.tex template/footer.tex ${top_srcdir}/hdf5-hio/src/H5FDhio.h
	doxygen doxygen.conf

clean-local:
	-rm -rf latex html libhdf5hio_api.pdf
