# -*- Makefile.am -*-
#
# Copyright (c) 2016-2017 Los Alamos National Security, LLC. All rights
#                         reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

LDADD = ../src/libhio.la
AM_CPPFLAGS = -I$(top_srcdir)/src/include

# install tool manual pages
dist_man_MANS = man/hio_dump.1

bin_PROGRAMS = hio_dump

hio_dump_SOURCES = hio_dump.c
