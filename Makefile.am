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

SUBDIRS = src design tests

EXTRA_DIST = hiobuild extra/json-c-0.12-nodoc-patched.tar.gz extra/json-c.patch \
	extra/json_rename.h
DISTCLEANFILES = libhio.pc

pkgconfigdir=$(libdir)/pkgconfig
pkgconfig_DATA=libhio.pc

docs:
	make -C design docs
