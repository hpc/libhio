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
EXTRA_DIST = run_setup run_combo run01 run02 run03 run04 run05 run07 run08 run09 run10 run12 run20 run90 run91 dw_simple_sub.sh

clean-local:
	-rm -rf .test_root1

noinst_PROGRAMS = test01.x xexec.x error_test.x
check_PROGRAMS = ${noinst_PROGRAMS}
TESTS = run01 run02 run03 run04 run05 run07 run08 run09 run12 error_test.x

test01_x_SOURCES = test01.c
xexec_x_SOURCES = xexec.c cw_misc.c cw_misc.h

xexec_x_CFLAGS = -DMPI -DHIO -DDLFCN
xexec_x_LDFLAGS = -ldl -lm
