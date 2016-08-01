# -*- mode: shell-script -*-
# Copyright 2015-2016 Los Alamos National Security, LLC. All rights
#                     reserved.

AC_DEFUN([HIO_CHECK_BZ2],[
    AC_ARG_WITH(bz2, [AS_HELP_STRING([--with-external-bz2=PATH],
                                     [use external bzip2. pass yes to use default version @<:@default=no@:>@])],
                [], [with_external_bz2=no])

    if test ! $with_external_bz2 = no ; then
        if test ! $with_external_bz2 = yes ; then
            if test -d "$with_external_bz2/lib" ; then
                LDFLAGS="$LDFLAGS -L$with_external_bz2/lib"
            else
                LDFLAGS="$LDFLAGS -L$with_external_bz2/lib64"
            fi
        fi

        AC_CHECK_LIB([bz2],[BZ2_bzBuffToBuffCompress],[hio_have_bz2=1])
    else
        abs_builddir=`pwd`
        cd "${srcdir}"
        abs_srcdir=`pwd`
        cd "${abs_builddir}"
        rm -rf extra/bzip2
        mkdir -p extra/bzip2
        tar -C extra/bzip2 -x -j -f "${abs_srcdir}"/extra/bzip2-1.0.6-patched.tbz2
        if test ! "$?" = "0" ; then
            AC_ERROR([failed to decompress bzip2])
        fi
    fi

    AM_CONDITIONAL([INTERNAL_BZ2], [test $with_external_bz2 = no])
])
