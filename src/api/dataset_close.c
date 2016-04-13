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
  uint64_t tmp[6];
  hio_context_t context;
  uint64_t rctime;
  int rc;

  if (HIO_OBJECT_NULL == dataset) {
    return HIO_ERR_BAD_PARAM;
  }

  if (dataset->ds_flags & HIO_FLAG_WRITE) {
    rc = hio_dataset_flush (dataset, HIO_FLUSH_MODE_COMPLETE);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  context = hioi_object_context (&dataset->ds_object);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Closing dataset %s::%llu",
            hioi_object_identifier (dataset), dataset->ds_id);

  tmp[0] = dataset->ds_stat.s_bread;
  tmp[1] = dataset->ds_stat.s_bwritten;
  tmp[2] = dataset->ds_stat.s_rtime;
  tmp[3] = dataset->ds_stat.s_wtime;
  tmp[4] = atomic_load(&dataset->ds_stat.s_rcount);
  tmp[5] = atomic_load(&dataset->ds_stat.s_wcount);

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
    MPI_Reduce (0 == context->c_rank ? MPI_IN_PLACE : tmp, tmp, 6, MPI_UINT64_T, MPI_SUM, 0, context->c_comm);
  }
#endif

  if (0 == context->c_rank && context->c_print_stats) {
    printf ("hio.dataset.stat %s.%s.%" PRIu64 " RW Bytes%" PRIu64 "B %" PRIu64 "B, RW Ops %" PRIu64 "ops %" PRIu64 "ops, %" PRIu64
            "us, RW API Time %" PRIu64 "us, Walltime %" PRIu64 "us\n", hioi_object_identifier (&context->c_object),
            hioi_object_identifier (&dataset->ds_object), dataset->ds_id, tmp[0], tmp[1], tmp[4], tmp[5], tmp[2],
            tmp[3], rctime - dataset->ds_rotime);
  }

  /* reset the id to the id originally requested */
  dataset->ds_id = dataset->ds_id_requested;

  return rc;
}
