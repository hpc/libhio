/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_types.h"

int hio_element_close (hio_element_t *element) {
  int rc;

  if (NULL == element || HIO_OBJECT_NULL == *element) {
    return HIO_ERR_BAD_PARAM;
  }

  rc = (*element)->e_close (*element);

  *element = HIO_OBJECT_NULL;

  return rc;
}
