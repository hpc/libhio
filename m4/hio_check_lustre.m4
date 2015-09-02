# -*- mode: shell-script -*-
# Copyright 2014-2015 Los Alamos National Security, LLC. All rights
#                     reserved.

AC_DEFUN([HIO_CHECK_LUSTRE],[

    AC_ARG_WITH([lustre], [AC_HELP_STRING([--with-lustre(=DIR)],
                [Build support for Lustre, optionally adding DIR/include, DIR/lib, and DIR/lib64 to the search paths for libraries and headers])],
        [lustre_specified=yes], [lustre_specified=no])

    if test "$with_lustre" != "no" ; then
        if test "$with_lustre" != "yes" && test "$with_lustre" ; then
            CPPFLAGS="$CPPFLAGS -I$with_lustre/include"
            if test -d "$with_lustre/lib64" ; then
                LDFLAGS="$CPPFLAGS -L$with_lustre/lib64"
            else
                LDFLAGS="$CPPFLAGS -L$with_lustre/lib"
            fi
        fi

        lustre_happy=no

        AC_CHECK_HEADERS([lustre/lustreapi.h lustre/liblustreapi.h lustre/lustre_user.h], [lustre_happy=yes])
        if test "$lustre_happy" = "yes" ; then
            AC_CHECK_LIB([lustreapi], [llapi_file_get_stripe],[],[lustre_happy=no])
            AC_CHECK_FUNCS([llapi_layout_alloc],[lustre_use_layout=yes],[lustre_use_layout=no])
        fi

        if test "$lustre_specified" = "yes" && test "$lustre_happy" = "no" ; then
            AC_ERROR([lustre support requested but not found])
        fi
    fi
])
