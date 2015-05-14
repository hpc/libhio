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

int hio_dataset_close (hio_dataset_t *set) {
  hio_module_t *module = (*set)->dataset_module;
  int rc;

  if (NULL == set || NULL == *set) {
    return HIO_ERR_BAD_PARAM;
  }

  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "Closing dataset %s::%llu",
            (*set)->dataset_object.identifier, (*set)->dataset_id);

  rc = module->dataset_close (module, *set);

  if (HIO_SUCCESS == rc && (HIO_FLAG_WRONLY & (*set)->dataset_flags)) {
    hio_dataset_data_t *ds_data = (*set)->dataset_data;
    /* update dataset data */
    ds_data->dd_last_id = (*set)->dataset_id;
    ds_data->dd_last_write_completion = time (NULL);

    if (0 == ds_data->dd_average_write_time) {
      ds_data->dd_average_write_time = (*set)->dataset_write_time;
    } else {
      ds_data->dd_average_write_time = (uint64_t) ((float) ds_data->dd_average_write_time * 0.8);
      ds_data->dd_average_write_time += (uint64_t) ((float) (*set)->dataset_write_time * 0.2);
    }
  }

  hioi_dataset_release (set);

  return rc;
}
