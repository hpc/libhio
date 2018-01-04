/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "hio_internal.h"

int hio_dataset_dump (const char *data_roots, const char *context_name, const char *dataset_name, const int64_t dataset_id, uint32_t flags, int rank, FILE *fh) {
  hio_dataset_list_t *list;
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

    list = hioi_dataset_list_get (context, dataset_name, dataset_id < 0 ? dataset_id : HIO_DATASET_ID_ANY);
    if (NULL == list) {
      break;
    }

    for (size_t i = 0 ; i < list->header_count ; ++i) {
      if (HIO_DATASET_ID_ANY == dataset_id || dataset_id == list->headers[i].ds_id) {
        hio_module_t *module = list->headers[i].module;
        (void) module->dataset_dump (module, list->headers + i, flags, rank, fh);

        if (HIO_DATASET_ID_NEWEST == dataset_id || HIO_DATASET_ID_HIGHEST == dataset_id) {
          /* if only the highest or newest id is request we are done */
          break;
        }
      }
    }

    hioi_dataset_list_release (list);
  } while (0);

  (void) hio_fini (&context);
  return rc;
}
