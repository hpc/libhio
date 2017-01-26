/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_internal.h"

#include <stdlib.h>

static int hioi_dataset_open_last (hio_dataset_t dataset) {
  hio_context_t context = hioi_object_context ((hio_object_t) dataset);
  hio_dataset_header_t *headers = NULL;
  int item_count = 0, rc, count = 0;
  int64_t id = dataset->ds_id;
  hio_module_t *module;
  void *tmp;

  for (int i = 0 ; i < context->c_mcount ; ++i) {
    module = context->c_modules[i];

    rc = module->dataset_list (module, hioi_object_identifier (dataset), &headers, &count);
    if (HIO_SUCCESS != rc) {
      hioi_err_push (rc, &dataset->ds_object, "dataset_open: error listing datasets on data root %s",
                     module->data_root);
    }
  }

  if (0 == count) {
    free (headers);
    return HIO_ERR_NOT_FOUND;
  }

  hioi_dataset_headers_sort (headers, count, id);

  /* debug output */
  if (0 == context->c_rank && HIO_VERBOSE_DEBUG_MED <= context->c_verbose) {
    hioi_log (context, HIO_VERBOSE_DEBUG_MED, "found %d dataset ids accross all data roots:", count);

    for (int i = 0 ; i < count ; ++i) {
      hioi_log (context, HIO_VERBOSE_DEBUG_MED, "dataset %s::%" PRId64 ": mtime = %ld, status = %d, "
                "data_root = %s", hioi_object_identifier (&dataset->ds_object), headers[i].ds_id,
                headers[i].ds_mtime, headers[i].ds_status, headers[i].module->data_root);
    }
  }

  for (int i = count - 1 ; i >= 0 ; --i) {
    module = headers[i].module;

    if (0 != headers[i].ds_status) {
      hioi_log (context, HIO_VERBOSE_DEBUG_MED, "skipping dataset with non-zero status: %s::%" PRId64
                ". status = %d", hioi_object_identifier (&dataset->ds_object), headers[i].ds_id,
                headers[i].ds_status);
      continue;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_MED, "attempting to open dataset %s::%" PRId64 " on data root "
              "%s (module: %p). index %d", hioi_object_identifier (&dataset->ds_object),
              headers[i].ds_id, module->data_root, module, i);

    /* set the current dataset id to the one we are attempting to open */
    dataset->ds_id = headers[i].ds_id;
    rc = hioi_dataset_open_internal (module, dataset);
    if (HIO_SUCCESS == rc) {
      break;
    }

    /* reset the id to the id originally requested */
    dataset->ds_id = id;
  }

  free (headers);

  return rc;
}

static int hioi_dataset_open_specific (hio_context_t context, hio_dataset_t dataset) {
  int rc = HIO_ERR_NOT_FOUND;

  for (int i = 0 ; i <= context->c_mcount ; ++i) {
    int module_index = (context->c_cur_module + i) % context->c_mcount;

    hio_module_t *module = context->c_modules[module_index];
    if (NULL == module) {
      /* internal error */
      return HIO_ERROR;
    }

    rc = hioi_dataset_open_internal (module, dataset);
    if (HIO_SUCCESS == rc) {
      break;
    }
  }

  return rc;
}

int hio_dataset_open (hio_dataset_t dataset) {
  hio_context_t context;

  if (HIO_OBJECT_NULL == dataset) {
    return HIO_ERR_BAD_PARAM;
  }

  if (dataset->ds_flags & HIO_FLAG_TRUNC) {
    /* ensure we take the create path later */
    dataset->ds_flags |= HIO_FLAG_CREAT;
  }

  context = hioi_object_context ((hio_object_t) dataset);

  if (HIO_DATASET_ID_HIGHEST == dataset->ds_id || HIO_DATASET_ID_NEWEST == dataset->ds_id) {
    return hioi_dataset_open_last (dataset);
  }

  return hioi_dataset_open_specific (context, dataset);
}
