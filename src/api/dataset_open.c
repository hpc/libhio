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

#include <stdlib.h>

int hio_dataset_open (hio_context_t context, hio_dataset_t *set_out, const char *name,
                      int64_t set_id, hio_flags_t flags, hio_dataset_mode_t mode) {
  hio_module_t *module;
  int rc;

  if (NULL == context || NULL == set_out || NULL == name) {
    return HIO_ERR_BAD_PARAM;
  }

  if (0 == context->context_module_count) {
    /* create hio modules for each item in the specified data roots */
    rc = hioi_context_create_modules (context);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  if (HIO_DATASET_ID_LAST == set_id) {
    hio_module_t *best_module = NULL;
    int64_t highest_id = 0, *set_ids;
    int num_set_ids;

    for (int i = 0 ; i < context->context_module_count ; ++i) {
      module = context->context_modules[i];

      rc = module->dataset_list (module, name, &set_ids, &num_set_ids);
      if (HIO_SUCCESS == rc) {
        for (int j = 0 ; j < num_set_ids ; ++j) {
          if (set_ids[j] >= highest_id) {
            highest_id = set_ids[j];
            best_module = module;
          }
        }
      }

      if (set_ids) {
        free (set_ids);
      }
    }

    if (NULL == best_module) {
      return HIO_ERR_NOT_FOUND;
    }

    module = best_module;
    set_id = highest_id;
  } else {
    module = hioi_context_select_module (context);
    if (NULL == module) {
      hio_err_push (HIO_ERROR, context, NULL, "Could not select hio module");
      return HIO_ERR_NOT_FOUND;
    }
  }

  /* Several things need to be done here:
   * 1) check if the user is requesting a specific dataset or the newest available,
   * 2) check if the dataset specified already exists in any module,
   * 3) if the dataset does not exist and we are creating then use the current
   *    module to open (create) the dataset. */

  return module->dataset_open (module, set_out, name, set_id, flags, mode);
}
