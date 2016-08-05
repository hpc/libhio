# -*- mode: shell-script -*-
# Copyright 2015-2016 Los Alamos National Security, LLC. All rights
#                     reserved.

AC_DEFUN([HIO_CHECK_HDF5],[
    # Check for HDF5
    AC_ARG_WITH(hdf5, [AS_HELP_STRING([--with-hdf5=DIR], [enable support for the HDF5 plugin @<:@default=no@:>@])],
                [], [with_hdf5=no])
    AC_ARG_WITH(hdf5-src, [AS_HELP_STRING([--with-hdf5-src=DIR], [the HDF5 source dir for building the plugin @<:@default=auto@:>@])],
                [], [with_hdf5_src=auto])


    # Always try to enable hdf5
    use_hdf5=1
    if ( test $with_hdf5 = no ) ; then
    	use_hdf5=0
    fi

    if ( test $with_hdf5 = yes ) ; then
        PKG_CHECK_MODULES(hdf5, hdf5 >= 1.8.16, [], [use_hdf5=0])
        if test $use_hdf5 = 1 ; then
            # PKG_CHECK_MODULES sets the CFLAGS variable but it only adds -I/path/to/hdf5 so
            # just add it to the CPPFLAGS
            CPPFLAGS="$CPPFLAGS $hdf5_CFLAGS"
            LIBS="$LIBS $hdf5_LIBS"
            hio_pkgconfig_requires="$hio_pkgconfig_requires, hdf5 >= 1.8.16"
        fi
    elif ( test $use_hdf5 = 1 && test $with_hdf5 != yes ) ; then
        if test -d "$with_hdf5/lib64" ; then
            LDFLAGS="$LDFLAGS -L$with_hdf5/lib64 -Wl,-rpath -Wl,$with_hdf5/lib64 -lhdf5"
        else
            LDFLAGS="$LDFLAGS -L$with_hdf5/lib -Wl,-rpath -Wl,$with_hdf5/lib -lhdf5"
        fi
        CPPFLAGS="$CPPFLAGS -I$with_hdf5/include"
    fi

    if ( test $use_hdf5 = 1 && test $with_hdf5 = yes ) ; then
        AC_ERROR([HDF5 support requested but not found])
        use_hdf5=0
    elif ( test $use_hdf5 = 1 && test $with_hdf5_src = auto ) ; then
        AC_ERROR([HDF5 support requested but hdf5 source dir not specified])
	use_hdf5=0
    else
        CPPFLAGS="$CPPFLAGS -I$with_hdf5_src/src"
    fi

    if test $use_hdf5 = 1 ; then
        AC_CHECK_HEADERS([hdf5.h])
    fi

    AC_DEFINE_UNQUOTED([HIO_USE_HDF5], [$use_hdf5], [Whether to build the hdf5 plugin])
    AM_CONDITIONAL([HDF5_AVAILABLE], [test $use_hdf5 = 1])
])
