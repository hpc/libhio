/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2016 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_internal.h"

int hio_dataset_close (hio_dataset_t dataset) {
  uint64_t tmp[4] = {0, 0, 0, 0};
  double aggregate_time;
  hio_context_t context;
  uint64_t rctime;
  int rc;

  if (HIO_OBJECT_NULL == dataset) {
    return HIO_ERR_BAD_PARAM;
  }

  context = hioi_object_context (&dataset->ds_object);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Closing dataset %s::%llu",
            hioi_object_identifier (dataset), dataset->ds_id);

  tmp[0] = dataset->ds_stat.s_bread;
  tmp[1] = dataset->ds_stat.s_bwritten;

  if (dataset->ds_stat.s_bread) {
    aggregate_time = dataset->ds_stat.s_rtime;
    tmp[2] = (1000 * dataset->ds_stat.s_bread) / aggregate_time;
  }

  if (dataset->ds_stat.s_bwritten) {
    aggregate_time = dataset->ds_stat.s_wtime;
    tmp[3] = (1000 * dataset->ds_stat.s_bwritten) / aggregate_time;
  }

  context->c_bread = dataset->ds_stat.s_bread;
  context->c_bwritten = dataset->ds_stat.s_bwritten;

  rc = hioi_dataset_close_internal (dataset);

  rctime = hioi_gettime ();

  if (HIO_SUCCESS == rc && (HIO_FLAG_WRITE & dataset->ds_flags)) {
    hio_dataset_data_t *ds_data = dataset->ds_data;
    /* update dataset data */
    ds_data->dd_last_id = dataset->ds_id;
    ds_data->dd_last_write_completion = time (NULL);

    if (0 == ds_data->dd_average_write_time) {
      ds_data->dd_average_write_time = dataset->ds_stat.s_wtime;
    } else {
      ds_data->dd_average_write_time = (uint64_t) ((float) ds_data->dd_average_write_time * 0.8);
      ds_data->dd_average_write_time += (uint64_t) ((float) dataset->ds_stat.s_wtime * 0.2);
    }
  }
#if HIO_USE_MPI
  if (1 != context->c_size) {
    MPI_Reduce (0 == context->c_rank ? MPI_IN_PLACE : tmp, tmp, 4, MPI_DOUBLE, MPI_SUM, 0, context->c_comm);
  }
#endif

  if (0 == context->c_rank && context->c_print_stats) {
    printf ("Dataset %s:%llu statistics:\n", dataset->ds_object.identifier, dataset->ds_id);

    printf ("  Overall bytes read: %" PRIu64 " in %" PRIu64 " usec, aggregate speed: %1.2f MB/sec, overall speed: %1.2f MB/sec\n",
            tmp[0], rctime - dataset->ds_rotime, (double) tmp[2] / 1000.0, (double) tmp[0] / (double) (rctime - dataset->ds_rotime));

    printf ("  Overall bytes written: %" PRIu64 " in %" PRIu64 " usec, aggregate speed: %1.2f MB/sec, overall speed: %1.2f MB/sec\n",
            tmp[1], rctime - dataset->ds_rotime, (double) tmp[3] / 1000.0, (double) tmp[1] / (double) (rctime - dataset->ds_rotime));
  }

  /* reset the id to the id originally requested */
  dataset->ds_id = dataset->ds_id_requested;

  return rc;
}
