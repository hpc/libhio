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

int hio_dataset_close (hio_dataset_t *set) {
  hio_module_t *module;
  hio_context_t context;
  hio_dataset_t dataset;
  int rc;

  if (NULL == set || NULL == *set) {
    return HIO_ERR_BAD_PARAM;
  }

  dataset = *set;
  module = dataset->ds_module;
  context = hioi_object_context (&dataset->ds_object);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Closing dataset %s::%llu",
            (*set)->ds_object.identifier, (*set)->ds_id);

  if (context->c_print_stats) {
    double speed, aggregate_time;
    printf ("Dataset %s:%llu statistics:\n", dataset->ds_object.identifier, dataset->ds_id);

    if (dataset->ds_bread) {
      aggregate_time = (double) dataset->ds_rtime;
      speed = ((double) dataset->ds_bread) / aggregate_time;

      printf ("  Bytes read: %" PRIu64 " in %llu usec (%1.2f MB/sec)\n", dataset->ds_bread,
              dataset->ds_rtime, speed);
    }

    if (dataset->ds_bwritten) {
      aggregate_time = (double) dataset->ds_wtime;
      speed = ((double) dataset->ds_bwritten) / aggregate_time;

      printf ("  Bytes written: %" PRIu64 " in %llu usec (%1.2f MB/sec)\n", dataset->ds_bwritten,
              dataset->ds_wtime, speed);
    }
  }

  context->c_bread = dataset->ds_bread;
  context->c_bwritten = dataset->ds_bwritten;

  rc = module->dataset_close (module, dataset);

  if (HIO_SUCCESS == rc && (HIO_FLAG_WRITE & dataset->ds_flags)) {
    hio_dataset_data_t *ds_data = dataset->ds_data;
    /* update dataset data */
    ds_data->dd_last_id = (*set)->ds_id;
    ds_data->dd_last_write_completion = time (NULL);

    if (0 == ds_data->dd_average_write_time) {
      ds_data->dd_average_write_time = (*set)->ds_wtime;
    } else {
      ds_data->dd_average_write_time = (uint64_t) ((float) ds_data->dd_average_write_time * 0.8);
      ds_data->dd_average_write_time += (uint64_t) ((float) (*set)->ds_wtime * 0.2);
    }
  }

  hioi_dataset_release (set);

  return rc;
}
