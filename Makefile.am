# -*- Makefile.am -*-
#
# Copyright (c) 2014-2016 Los Alamos National Security, LLC. All rights
#                         reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

ACLOCAL_AMFLAGS=-I m4

dirs = src design test
if HDF5_AVAILABLE
dirs += hdf5-hio
endif

SUBDIRS = ${dirs}
EXTRA_DIST = hiobuild extra/json-c-0.12-nodoc-patched.tar.gz extra/json-c.patch \
	extra/json_rename.h extra/bzip2-1.0.6-patched.tbz2
DISTCLEANFILES = libhio.pc

pkgconfigdir=$(libdir)/pkgconfig
pkgconfig_DATA=libhio.pc

docs:
	make -C design docs
if HDF5_AVAILABLE
	make -C hdf5-hio/doc
endif

dist: dist-bzip2
