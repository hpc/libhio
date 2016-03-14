# -*- mode: shell-script-mode -*-
#
# Copyright (c) 2014-2016 Los Alamos National Security, LLC. All rights
#                         reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

AC_PREREQ([2.69])
AC_INIT([libhio], [1.3], [hjelmn@lanl.gov])
AC_CONFIG_SRCDIR([src/hio_context.c])
AC_CONFIG_HEADERS([src/include/hio_config.h])
AC_CONFIG_MACRO_DIR([m4])
AC_CHECK_PROG(MPICC, [mpicc], [mpicc], [none])
AC_CHECK_PROG(MPICXX, [mpicxx], [mpicxx], [none])
AM_SILENT_RULES([yes])

hio_use_mpi=0

if ( test -n "$XTOS_VERSION" || test -n "$CRAYPE_DIR" ) && test ! "$CC" ; then
    # Work around Cray idiosyncrasies
    # Don't let the Cray wrapper bring in extra crap
    PE_VERSION=`echo $XTOS_VERSION | sed 's/\..*//g'`
    # force the cray wrappers to act in a predictable way
    if test x$PE_VERSION = x4 ; then
        CC="cc -target=native -dynamic"
        CXX="CC -target=native -dynamic"
    else
        CC="cc --cray-bypass-pkgconfig -dynamic"
        CXX="CC --cray-bypass-pkgconfig -dynamic"
    fi
    # We may be cross-compiling so trick configure
    cross_compiling=yes
    if test ! -n "$LD" ; then
        LD=ld
    fi
elif test ${MPICC} = "mpicc" ; then
    CC=${MPICC}
    CXX=${MPICXX}
    hio_use_mpi=1
fi

AM_INIT_AUTOMAKE
LT_INIT

HIO_CHECK_JSON
HIO_CHECK_DATAWARP
HIO_CHECK_XPMEM
HIO_CHECK_LUSTRE

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

AC_ARG_VAR([HAVE_PDFLATEX], [Whether pdflatex exists and is executable])
AC_CHECK_PROG([HAVE_PDFLATEX], [pdflatex], [yes], [no])

# Checks for libraries.

# Checks for header files.
AC_CHECK_HEADERS([stdlib.h strings.h sys/types.h sys/time.h pthread.h dlfcn.h sys/stat.h sys/param.h \
                  sys/mount.h sys/vfs.h])
AC_CHECK_FUNCS([access gettimeofday dlopen dlclose dlsym stat statfs MPI_Win_allocate_shared \
                MPI_Comm_split_type MPI_Win_flush])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL

AC_TYPE_INT64_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T

AC_DEFINE_UNQUOTED([HIO_USE_MPI], [$hio_use_mpi], [Whether to use mpi with hio])
AC_DEFINE([_GNU_SOURCE], [1], [Define _GNU_SOURCE feature macro])
AC_DEFINE_UNQUOTED([HIO_PREFIX], ["$prefix"], [HIO install prefix])
AM_CONDITIONAL([HAVE_PDFLATEX], [test $HAVE_PDFLATEX = yes])

AC_SUBST([HIO_PKGCONFIG_REQUIRES], [$hio_pkgconfig_requires])

AC_SUBST(HIO_BUILD_TIMESTAMP,[$(date "+%A %B %d, %Y at %l:%M %p %Z")])

# Checks for library functions.

AC_CONFIG_FILES([Makefile design/Makefile src/Makefile tests/Makefile libhio.pc design/doxygen.conf design/template/header.tex])

AC_OUTPUT
