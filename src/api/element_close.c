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
  hio_dataset_t dataset = (*element)->element_dataset;
  hio_module_t *module = dataset->dataset_module;
  int rc;

  if (NULL == element || NULL == *element) {
    return HIO_ERR_BAD_PARAM;
  }

  rc = module->element_close (module, *element);

  *element = HIO_ELEMENT_NULL;

  return rc;
}
