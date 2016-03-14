# -*- mode: shell-script -*-
# Copyright 2015-2016 Los Alamos National Security, LLC. All rights
#                     reserved.

AC_DEFUN([HIO_CHECK_XPMEM],[
    # Check for XPMEM
    AC_ARG_WITH(xpmem, [AS_HELP_STRING([--with-xpmem=DIR], [enable support for XPMEM shared memory @<:@default=auto@:>@])],
                [], [with_xpmem=auto])

    # Always try to enable xpmem
    use_xpmem=1
    if test $with_xpmem = auto ; then
        PKG_CHECK_MODULES(xpmem, cray-xpmem >= 0.1, [], [use_xpmem=0])
        if test $use_xpmem = 1 ; then
            # PKG_CHECK_MODULES sets the CFLAGS variable but it only adds -I/path/to/xpmem so
            # just add it to the CPPFLAGS
            CPPFLAGS="$CPPFLAGS $xpmem_CFLAGS"
            LIBS="$LIBS $xpmem_LIBS"
            hio_pkgconfig_requires="$hio_pkgconfig_requires, cray-xpmem >= 0.1"
        fi
    elif ( test $with_xpmem != no && test $with_xpmem != yes ) ; then
        if test -d "$with_xpmem/lib64" ; then
            LDFLAGS="$LDFLAGS -L$with_xpmem/lib64 -Wl,-rpath -Wl,$with_xpmem/lib64 -lxpmem"
        else
            LDFLAGS="$LDFLAGS -L$with_xpmem/lib -Wl,-rpath -Wl,$with_xpmem/lib -lxpmem"
        fi
        CPPFLAGS="$CPPFLAGS -I$with_xpmem/include"
    fi

    if test $use_xpmem = 1 ; then
        AC_CHECK_HEADERS([xpmem.h])
        AC_CHECK_LIB([xpmem], [xpmem_make], [], [use_xpmem=0])
    fi

    if ( test $use_xpmem = 0 && test $with_xpmem != no && test $with_xpmem != auto ) ; then
        AC_ERROR([XPMEM support requested but not found])
    fi

    AC_DEFINE_UNQUOTED([HIO_USE_XPMEM], [$use_xpmem], [Whether to use xpmem for shared memory])
    AM_CONDITIONAL([XPMEM_AVAILABLE], [test $use_xpmem = 1])
])
