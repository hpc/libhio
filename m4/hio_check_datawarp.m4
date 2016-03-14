# -*- mode: shell-script -*-
# Copyright 2015-2016 Los Alamos National Security, LLC. All rights
#                     reserved.

AC_DEFUN([HIO_CHECK_DATAWARP],[
    # Check for datawarp availability
    AC_ARG_WITH(datawarp, [AS_HELP_STRING([--with-datawarp=DIR], [enable support for Cray datawarp @<:@default=auto@:>@])],
                [], [with_datawarp=auto])

    use_datawarp=0
    if test "$with_datawarp" = "auto" ; then
        PKG_CHECK_MODULES(datawarp, cray-datawarp >= 1.0, [use_datawarp=1], [use_datawarp=0])
        if test $use_datawarp = 1 ; then
            # PKG_CHECK_MODULES sets the CFLAGS variable but it only adds -I/path/to/datawarp so
            # just add it to the CPPFLAGS
            CPPFLAGS="$CPPFLAGS $datawarp_CFLAGS"
            LIBS="$LIBS $datawarp_LIBS"
            hio_pkgconfig_requires="$hio_pkgconfig_requires, cray-datawarp >= 1.0"
        fi
    elif test "$with_datawarp" != "no" ; then
        if test -d "$with_datawarp/lib64" ; then
            LDFLAGS="$LDFLAGS -L$with_datawarp/lib64 -Wl,-rpath -Wl,$with_datawarp/lib64 -ldatawarp"
        else
            LDFLAGS="$LDFLAGS -L$with_datawarp/lib -Wl,-rpath -Wl,$with_datawarp/lib -ldatawarp"
        fi
        CPPFLAGS="$CPPFLAGS -I$with_datawarp/include"
        use_datawarp=1
    fi

    if test $use_datawarp = 1 ; then
        AC_CHECK_HEADERS([datawarp.h])
        AC_CHECK_LIB([datawarp], [dw_stage_file_out], [use_datawarp=1], [use_datawarp=0])
    fi

    AC_DEFINE_UNQUOTED([HIO_USE_DATAWARP], [$use_datawarp], [Whether to use datawarp for bb])
    AM_CONDITIONAL([DATAWARP_AVAILABLE], [test x$use_datawarp = x1])
])
