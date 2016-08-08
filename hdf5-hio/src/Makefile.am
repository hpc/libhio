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

all: libh5fdhio.la

AUTOMAKE_OPTIONS = subdir-objects
CLEANFILES = 

lib_LTLIBRARIES = libh5fdhio.la

include_HEADERS = H5FDhio.h
noinst_HEADERS = 
EXTRA_DIST = 

libh5fdhio_la_CFLAGS = $(AM_CFLAGS) $(XML_CFLAGS)
libh5fdhio_la_LDFLAGS = $(LTLDFLAGS) $(XML_LIBS)
libh5fdhio_la_SOURCES = H5FDhio.c
