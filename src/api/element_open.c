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

#include "hio_types.h"

int hio_element_open (hio_dataset_t dataset, hio_element_t *element_out, const char *element_name,
                      int flags) {
  if (NULL == dataset || NULL == element_out || NULL == element_name) {
    return HIO_ERR_BAD_PARAM;
  }

  return dataset->ds_element_open (dataset, element_out, element_name, flags);
}
