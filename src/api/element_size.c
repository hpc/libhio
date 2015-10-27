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

int hio_element_size (hio_element_t element, int64_t *e_size) {
  if (HIO_OBJECT_NULL == element || NULL == e_size) {
    return HIO_ERR_BAD_PARAM;
  }

  *e_size = element->e_size;
  return HIO_SUCCESS;
}
