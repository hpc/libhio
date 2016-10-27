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

LDADD = ../src/libhio.la
AM_CPPFLAGS = -I$(top_srcdir)/src/include
EXTRA_DIST = run_setup run_combo run01 run02 run03 run04 run05 run07 run08 run09 run10 run12 run20 run80 run81 run82 run83 run84 run85 run90 run91 dw_simple_sub.sh check_test dw_rm_all_sess cancelme

SUBDIRS = xexec

clean-local:
	-rm -rf .test_root1

if ENABLE_TESTS

noinst_PROGRAMS = test01.x error_test.x

check_PROGRAMS = ${noinst_PROGRAMS}
TESTS = run01 error_test.x
if HAVE_MPI
TESTS += run02 run03 run04 run05 run07 run08 run09 run12
endif

test01_x_SOURCES = test01.c

error_test_x_LDADD = ../src/.libs/libhio.a

endif
