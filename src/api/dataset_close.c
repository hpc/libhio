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
  module = dataset->dataset_module;
  context = dataset->dataset_context;

  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "Closing dataset %s::%llu",
            (*set)->dataset_object.identifier, (*set)->dataset_id);

  if (context->context_print_statistics) {
    double speed, aggregate_time;
    printf ("Dataset %s:%llu statistics:\n", dataset->dataset_object.identifier, dataset->dataset_id);

    if (dataset->dataset_bytes_read) {
      aggregate_time = (double) dataset->dataset_read_time;
      speed = ((double) dataset->dataset_bytes_read) / aggregate_time;

      printf ("  Bytes read: %" PRIu64 " in %llu usec (%1.2f MB/sec)\n", dataset->dataset_bytes_read,
              dataset->dataset_read_time, speed);
    }

    if (dataset->dataset_bytes_written) {
      aggregate_time = (double) dataset->dataset_write_time;
      speed = ((double) dataset->dataset_bytes_written) / aggregate_time;

      printf ("  Bytes written: %" PRIu64 " in %llu usec (%1.2f MB/sec)\n", dataset->dataset_bytes_written,
              dataset->dataset_write_time, speed);
    }
  }

  context->context_bytes_read = dataset->dataset_bytes_read;
  context->context_bytes_written = dataset->dataset_bytes_written;

  rc = module->dataset_close (module, dataset);

  if (HIO_SUCCESS == rc && (HIO_FLAG_WRONLY & dataset->dataset_flags)) {
    hio_dataset_data_t *ds_data = dataset->dataset_data;
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
