# -*- Makefile.am -*-
#
# Copyright (c) 2014-2017 Los Alamos National Security, LLC. All rights
#                         reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

LDADD = ../src/libhio.la
AM_CPPFLAGS = -I$(top_srcdir)/src/include
ED1 = run_setup run_combo run01 run02 run03 run04 run05 run07 run08 run09
ED2 = run10 run12 run20 run21 run22
ED3 = run80 run81 run82 run83 run84 run85 run90 run91 run92 
ED4 = dw_simple_sub.sh hio_example.sh check_test dw_rm_all_sess
ED5 = cantest.py README.cantest

EXTRA_DIST = ${ED1} ${ED2} ${ED3} ${ED4} ${ED5}

SUBDIRS = xexec

clean-local:
	-rm -rf .test_root1

if ENABLE_TESTS

noinst_PROGRAMS = test01.x error_test.x hio_example.x

check_PROGRAMS = ${noinst_PROGRAMS}
TESTS = run01 error_test.x
if HAVE_MPI
TESTS += run02 run03 run04 run05 run07 run08 run09 run12
endif

test01_x_SOURCES = test01.c

error_test_x_LDADD = ../src/.libs/libhio.a

endif
