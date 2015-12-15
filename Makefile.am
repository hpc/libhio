ACLOCAL_AMFLAGS=-I m4

SUBDIRS = src design tests

EXTRA_DIST = hiobuild autogen.sh extra/json-c-0.12-nodoc.tar.gz
DISTCLEANFILES = libhio.pc

pkgconfigdir=$(libdir)/pkgconfig
pkgconfig_DATA=libhio.pc

docs:
	make -C design docs
