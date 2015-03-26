# -*- Autoconf -*-
# Copyright 2014-2015 Los Alamos National Security, LLC. All rights
#                     reserved.

AC_PREREQ([2.69])
AC_INIT([libhio], [0.1], [hjelmn@lanl.gov])
AC_CONFIG_SRCDIR([src/hio_context.c])
AC_CONFIG_HEADERS([src/include/hio_config.h])
AC_CONFIG_MACRO_DIR([m4])

if test -n "$XTOS_VERSION" ; then
    # Work around Cray idiosyncrasies
    # Don't let the Cray wrapper bring in extra crap
    PE_VERSION=`echo $XTOS_VERSION | sed 's/\..*//g'`
    if test x$PE_VERSION = x4 ; then
        CC="cc -target=native"
    else
        CC="cc --cray-bypass-pkgconfig"
    fi
    # Force cray's linker to act like it should
    LDFLAGS="$LDFLAGS -dynamic"
fi

AM_INIT_AUTOMAKE
LT_INIT

PKG_CHECK_MODULES(XML, libxml-2.0 >= 2.4)
AC_CHECK_PROG(MPICC, [mpicc], [mpicc], [none])

if test ${MPICC} = "mpicc" ; then
    CC=${MPICC}
    hio_use_mpi=1
else
    hio_use_mpi=0
fi

if test x$hio_use_mpi = x0 ; then
    AC_MSG_CHECKING([for craypich])
    if test -n "$CRAY_MPICH2_DIR" ; then
        AC_MSG_RESULT([yes])
        CPPFLAGS="$CPPFLAGS -I$CRAY_MPICH2_DIR/include"
        LDFLAGS="$LDFLAGS -L$CRAY_MPICH2_DIR/lib"
        LIBS="$LIBS -lmpich"
        hio_use_mpi=1
    else
        AC_MSG_RESULT([no])
    fi
fi

# Checks for programs.
AC_PROG_CC_C99

AC_PROG_LEX

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h strings.h sys/types.h sys/time.h pthread.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL

AC_TYPE_INT64_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T

AC_DEFINE_UNQUOTED([HIO_USE_MPI], [$hio_use_mpi], [Whether to use mpi with hio])
AC_DEFINE([_GNU_SOURCE], [1], [Define _GNU_SOURCE feature macro])

# Checks for library functions.

AC_CONFIG_FILES([Makefile design/Makefile src/Makefile tests/Makefile])

AC_OUTPUT
