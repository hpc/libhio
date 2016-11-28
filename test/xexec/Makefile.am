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

LDADD = ../../src/libhio.la
AM_CPPFLAGS = -I$(top_srcdir)/src/include

noinst_PROGRAMS = xexec.x

xexec_x_SOURCES = xexec.c xexec_base.c xexec_fio.c xexec_hio.c xexec_mpi.c xexec.h cw_misc.c cw_misc.h 
xexec_x_CFLAGS =  -DMPI -DHIO -DDLFCN -g
# CGW uncomment next 2 lines for very complete warnings
xexec_x_CFLAGS += -Wall -Werror -Wextra -Wpedantic -pedantic-errors 
xexec_x_CFLAGS += -Wno-unused-parameter -Wno-missing-field-initializers
xexec_x_LDFLAGS = -ldl -lm -g

