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

LDADD = ../src/libh5fdhio.la ../../src/libhio.la
AM_CPPFLAGS = -I../src -I../../src/include
EXTRA_DIST = 

clean:
	-rm -f hdf5_hio_test hdf5_hio_test-hdf5_hio_test.o

noinst_PROGRAMS = hdf5_hio_test
if HAVE_MPI
hdf5_hio_test_CFLAGS = $(AM_CFLAGS) $(XML_CFLAGS) -DMPI
hdf5_hio_test_LDFLAGS = $(AM_LDFLAGS) $(XML_LIBS) -lhdf5 -lmpi
else
hdf5_hio_test_CFLAGS = $(AM_CFLAGS) $(XML_CFLAGS) 
hdf5_hio_test_LDFLAGS = $(AM_LDFLAGS) $(XML_LIBS) -lhdf5 
endif
