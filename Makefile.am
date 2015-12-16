ACLOCAL_AMFLAGS=-I m4

SUBDIRS = src design tests

EXTRA_DIST = hiobuild autogen.sh extra/json-c-0.12-nodoc.tar.gz extra/json-c.patch \
	extra/json_rename.h
DISTCLEANFILES = libhio.pc

pkgconfigdir=$(libdir)/pkgconfig
pkgconfig_DATA=libhio.pc

docs:
	make -C design docs
