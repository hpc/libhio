# -*- mode: shell-script -*-
# Copyright 2015-2016 Los Alamos National Security, LLC. All rights
#                     reserved.

AC_DEFUN([HIO_CHECK_MPI_FEATURES],[
    # Check for MPI-3.0 functions
    AC_CHECK_FUNCS_ONCE([MPI_Win_allocate_shared MPI_Comm_split_type])
])
