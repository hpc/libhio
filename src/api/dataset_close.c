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

int hio_dataset_close (hio_dataset_t *set) {
  hio_module_t *module = (*set)->dataset_module;
  int rc;

  rc = module->dataset_close (module, *set);

  hioi_dataset_release (set);

  return rc;
}
