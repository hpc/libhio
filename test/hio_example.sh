#! /bin/bash
# -*- Mode: sh; sh-basic-offset:2 ; indent-tabs-mode:nil -*- */
#
# Copyright (c) 2014-2016 Los Alamos National Security, LLC.  All rights
#                         reserved.
# $COPYRIGHT$
#
# Additional copyrights may follow
#
# $HEADER$
#

#=============================================================================
# hio_example.sh - Runs hio example program
#=============================================================================

# Assume running from the libhio source tree "test" directory
libs=$PWD/../src/.libs
if   [[ ${OSTYPE:0:6} == "darwin" ]]; then 
  export DYLD_LIBRARY_PATH=$libs:$DYLD_LIBRARY_PATH
  mpicmd="mpirun"
elif [[ ${OSTYPE:0:5} == "linux" ]];  then 
  export LD_LIBRARY_PATH=$libs:$LD_LIBRARY_PATH
  mpicmd="aprun"
else
  echo "Error: OSTYPE \"$OSTYPE\" not recognized"
fi

# Create input deck specifying data root
echo "#HIO data_roots = posix:$PWD" > hio_example.input.deck

# Configuration can also be specified via the environment
export HIO_verbose=20

# Remove HIO example dataset written previously
# Run on compute node due to NFS latency
$mpicmd -n 1 rm -fR hio_example_context.hio

# Run the example
$mpicmd -n 1 .libs/hio_example.x

# -- end of hio_example.sh ---
