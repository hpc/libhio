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

int hio_dataset_open (hio_context_t context, hio_dataset_t *set_out, const char *name,
                      int64_t set_id, hio_flags_t flags, hio_dataset_mode_t mode) {
  hio_module_t *module;
  int rc;

  module = hioi_context_select_module (context);
  if (NULL == module) {
    hio_err_push (HIO_ERROR, context, NULL, "Could not select hio module");
    return rc;
  }

  /* Several things need to be done here:
   * 1) check if the user is requesting a specific dataset or the newest available,
   * 2) check if the dataset specified already exists in any module,
   * 3) if the dataset does not exist and we are creating then use the current
   *    module to open (create) the dataset. */

  return module->dataset_open (module, set_out, name, set_id, flags, mode);
}
