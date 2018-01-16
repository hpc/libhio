/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_internal.h"

#include <stdlib.h>

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
  hio_context_t context = hioi_object_context ((hio_object_t) dataset);
  hio_dataset_list_t *list;
  const int64_t id = dataset->ds_id;
  hio_module_t *module;
  int rc = HIO_ERR_NOT_FOUND;

  if (HIO_OBJECT_NULL == dataset) {
    return HIO_ERR_BAD_PARAM;
  }

  if (dataset->ds_flags & HIO_FLAG_TRUNC) {
    /* ensure we take the create path later */
    dataset->ds_flags |= HIO_FLAG_CREAT;
  }

  list = hioi_dataset_list_get (context, hioi_object_identifier (dataset), id);
  if (NULL == list) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  for (int i = list->header_count - 1 ; i >= 0 ; --i) {
    module = list->headers[i].module;

    if (0 != list->headers[i].ds_status) {
      hioi_log (context, HIO_VERBOSE_DEBUG_MED, "skipping dataset with non-zero status: %s::%" PRId64
                ". status = %d", hioi_object_identifier (&dataset->ds_object), list->headers[i].ds_id,
                list->headers[i].ds_status);
      continue;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_MED, "attempting to open dataset %s::%" PRId64 " on data root "
              "%s (module: %p). index %d", hioi_object_identifier (&dataset->ds_object),
              list->headers[i].ds_id, module->data_root, module, i);

    if (HIO_DATASET_ID_HIGHEST == id || HIO_DATASET_ID_NEWEST == id || id == list->headers[i].ds_id) {
      /* set the current dataset id to the one we are attempting to open */
      dataset->ds_id = list->headers[i].ds_id;
      rc = hioi_dataset_open_internal (module, dataset);
      if (HIO_SUCCESS == rc) {
        break;
      }

      /* reset the id to the id originally requested */
      dataset->ds_id = id;
    }
  }

  hioi_dataset_list_release (list);

  if (HIO_SUCCESS != rc) {
    /* If nothing was found just try to open the dataset. This is meant to support importing a POSIX
     * file. At some point I plan to improve the logic so this isn't needed. */
    return hioi_dataset_open_specific (context, dataset);
  }

  return rc;
}
