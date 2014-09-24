/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_types.h"
#include "hio_context.h"

int hio_close (hio_element_t *element) {
  hio_dataset_t dataset = (*element)->element_dataset;
  hio_module_t *module = dataset->dataset_module;
  int rc;

  rc = module->element_close (module, *element);

  return rc;
}
