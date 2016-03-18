/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_internal.h"

int hio_element_open (hio_dataset_t dataset, hio_element_t *element_out, const char *element_name,
                      int flags) {
  hio_context_t context;

  if (NULL == dataset || NULL == element_out || NULL == element_name) {
    return HIO_ERR_BAD_PARAM;
  }

  context = hioi_object_context (&dataset->ds_object);

  return hioi_element_open_internal (dataset, element_out, element_name, flags, context->c_rank);
}
