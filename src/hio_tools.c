/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "hio_internal.h"

int hio_dataset_dump (const char *data_roots, const char *context_name, const char *dataset_name, int64_t dataset_id, uint32_t flags, int rank, FILE *fh) {
  hio_context_t context;
  int rc;

  if (NULL == context_name) {
    return HIO_ERR_BAD_PARAM;
  }

  rc = hio_init_single (&context, NULL, NULL, context_name);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  do {
    if (NULL != data_roots) {
      rc = hio_config_set_value ((hio_object_t) context, "data_roots", data_roots);
      if (HIO_SUCCESS != rc) {
        break;
      }
    }

    /* create hio modules for each item in the specified data roots */
    rc = hioi_context_create_modules (context);
    if (HIO_SUCCESS != rc) {
      break;
    }

    for (int i = 0 ; i < context->c_mcount ; ++i) {
      hio_module_t *module = context->c_modules[i];

      rc = module->dataset_dump (module, dataset_name, dataset_id, flags, rank, fh);
      if (HIO_SUCCESS != rc) {
        hioi_log (context, HIO_VERBOSE_WARN, "dataset_dump: error dumping dataset(s) from data root %s\n", module->data_root);
      }
    }
  } while (0);

  (void) hio_fini (&context);
  return rc;
}
