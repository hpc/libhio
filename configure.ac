# -*- Autoconf -*-
# Copyright 2014      Los Alamos National Security, LLC. All rights
#                     reserved.

AC_PREREQ([2.69])
AC_INIT([libhio], [0.1], [hjelmn@lanl.gov])
AC_CONFIG_SRCDIR([src/hio_context.c])
AC_CONFIG_HEADERS([src/include/config.h])
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE
LT_INIT

# Checks for programs.
AC_PROG_CC_C99

AC_PROG_LEX

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h sys/types.h sys/time.h pthread.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL

AC_TYPE_INT64_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T

AC_CHECK_PROG(MPICC, [mpicc], [mpicc], [${CC}])

if test ${MPICC} = "mpicc" ; then
    hio_use_mpi=1
else
    hio_use_mpi=0
fi

CC=${MPICC}

AC_DEFINE_UNQUOTED([HIO_USE_MPI], [$hio_use_mpi], [Whether to use mpi with hio])

PKG_CHECK_MODULES(XML, libxml-2.0 >= 2.4)

# Checks for library functions.

AC_CONFIG_FILES([Makefile design/Makefile src/Makefile])

AC_OUTPUT
