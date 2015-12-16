# -*- mode: shell-script -*-
# Copyright 2015      Los Alamos National Security, LLC. All rights
#                     reserved.

AC_DEFUN([HIO_CHECK_JSON],[

AC_ARG_WITH(json, [AS_HELP_STRING([--with-external-json=PATH], [use external json-c. pass yes to use pkgconfig @<:@default=no@:>@])],
                  [], [with_external_json=no])

if test ! $with_external_json = no ; then
    if test $with_external_json = yes ; then
        PKG_CHECK_MODULES(json, json-c >= 0.10)
        hio_pkgconfig_requires="$hio_pkgconfig_requires, json-c >= 0.10"
    else
        json_CFLAGS="-I$with_external_json/include/json-c"
        if test -d "$with_external_json/lib64" ; then
            json_LIBS="-L$with_external_json/lib64"
        else
            json_LIBS="-L$with_external_json/lib"
        fi

        json_LIBS="$json_LIBS -ljson-c"
    fi

    CPPFLAGS="$CPPFLAGS $json_CFLAGS"
    LIBS="$LIBS $json_LIBS"

    AC_CHECK_FUNCS([json_object_new_object],[],[AC_ERROR([external json-c specified but could not be used])])
else
    abs_builddir=`pwd`
    cd "${srcdir}"
    abs_srcdir=`pwd`
    cd "${abs_builddir}"
    AC_MSG_NOTICE([configuring json-c])
    rm -rf extra/json
    mkdir -p extra/json/build
    tar -C extra/json -x -z -f "${abs_srcdir}"/extra/json-c-0.12-nodoc-patched.tar.gz
    cd "${abs_srcdir}"/extra/json/build
    if test $cross_compiling = yes ; then
        "${abs_srcdir}"/extra/json/json-c-0.12/configure --disable-shared --enable-static --with-pic --enable-silent-rules CC="$CC" --host=blackfin-linux &> config.out
    else
        "${abs_srcdir}"/extra/json/json-c-0.12/configure --disable-shared --enable-static --with-pic --enable-silent-rules CC="$CC" &> config.out
    fi
    cd "${abs_builddir}"
    if test ! "$?" = "0" ; then
        AC_ERROR([failed to configure json-c])
    fi
fi

AM_CONDITIONAL([INTERNAL_JSON_C], [test $with_external_json = no])
])
