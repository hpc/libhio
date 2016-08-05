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
#include <stdlib.h>
#include <string.h>

static int request_compare (const void *a, const void *b) {
  const hio_internal_request_t **reqa = (const hio_internal_request_t **) a;
  const hio_internal_request_t **reqb = (const hio_internal_request_t **) b;

  /* sort by element then by application offset */
  if (reqa[0]->ir_element > reqb[0]->ir_element) {
    return 1;
  } else if (reqa[0]->ir_element < reqb[0]->ir_element) {
    return -1;
  }

  if (reqa[0]->ir_offset > reqb[0]->ir_offset) {
    return 1;
  }

  return -1;
}

int hioi_dataset_buffer_flush (hio_dataset_t dataset) {
  hio_internal_request_t **reqs, *req, *next;
  int rc;

  if (0 == dataset->ds_buffer.b_reqcount) {
    /* nothing to do */
    return HIO_SUCCESS;
  }

  reqs = malloc (sizeof (*reqs) * dataset->ds_buffer.b_reqcount);
  if (NULL == reqs) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* sort the request list and pass it off to the backend */
  int i = 0;
  hioi_list_foreach_safe(req, next, dataset->ds_buffer.b_reqlist, hio_internal_request_t, ir_list) {
    reqs[i++] = req;
    hioi_list_remove (req, ir_list);
  }

  qsort ((void *) reqs, dataset->ds_buffer.b_reqcount, sizeof (*reqs), request_compare);

  rc = dataset->ds_process_reqs (dataset, reqs, dataset->ds_buffer.b_reqcount);

  /* NTH: this is temporary code to plug a leak until better code is ready */
  for (i = 0 ; i < dataset->ds_buffer.b_reqcount ; ++i) {
    free (reqs[i]);
  }

  free (reqs);
  /* end temporary code */

  dataset->ds_buffer.b_reqcount = 0;
  dataset->ds_buffer.b_remaining = dataset->ds_buffer.b_size;

  return rc;
}

#if HIO_MPI_HAVE(3)

int hioi_dataset_shared_init (hio_dataset_t dataset, int stripes) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  size_t ds_buffer_size = dataset->ds_buffer_size;
  size_t control_block_size;
  MPI_Win shared_win;
  MPI_Aint data_size;
  int rc, disp_unit;
  void *base;

  if (MPI_COMM_NULL == context->c_shared_comm) {
    return HIO_SUCCESS;
  }

  /* ensure data block starts on a cache line boundary */
  control_block_size = (sizeof (hio_shared_control_t) + stripes * sizeof (dataset->ds_shared_control->s_stripes[0]) + 127) & ~127;
  data_size = ds_buffer_size + control_block_size * (0 == context->c_shared_rank);

  rc = MPI_Win_allocate_shared (data_size, 1, MPI_INFO_NULL,
                                context->c_shared_comm, &base, &shared_win);
  if (MPI_SUCCESS != rc) {
    hioi_log (context, HIO_VERBOSE_WARN, "could not allocate shared memory window, size: %td", data_size);
    return HIO_ERR_NOT_AVAILABLE;
  }

  if (0 == context->c_shared_rank) {
    pthread_mutexattr_t mutex_attr;

    /* initialize the control structure */
    memset (base, 0, control_block_size);
    dataset->ds_shared_control = (hio_shared_control_t *) (intptr_t) base;
    dataset->ds_shared_control->s_master = context->c_rank;

    pthread_mutexattr_init (&mutex_attr);
    pthread_mutexattr_setpshared (&mutex_attr, PTHREAD_PROCESS_SHARED);

    /* fixme - not sure this is the right way to ensure stripe 0 mutex gets init'd */
    for (int i = 0 ; i < stripes ; ++i) {
      pthread_mutex_init (&dataset->ds_shared_control->s_stripes[i].s_mutex, &mutex_attr);
      atomic_init (&dataset->ds_shared_control->s_stripes[i].s_index, 0);
    }

    pthread_mutexattr_destroy (&mutex_attr);
    /* master base follows the control block */
    dataset->ds_buffer.b_base = (void *)((intptr_t) base + control_block_size);
  } else {
    dataset->ds_buffer.b_base = base;
  }

  dataset->ds_buffer.b_size = ds_buffer_size;
  dataset->ds_buffer.b_remaining = ds_buffer_size;
  dataset->ds_buffer.b_reqcount = 0;
  hioi_list_init (dataset->ds_buffer.b_reqlist);

  rc = MPI_Win_shared_query (shared_win, 0, &data_size, &disp_unit, &base);
  if (MPI_SUCCESS != rc) {
    hioi_log (context, HIO_VERBOSE_WARN, "error querying shared memory window, rc: %d", rc);
    MPI_Win_free (&shared_win);
    return HIO_ERROR;
  }

  dataset->ds_shared_win = shared_win;
  dataset->ds_shared_control = (hio_shared_control_t *) base;

  MPI_Barrier (context->c_shared_comm);

  return HIO_SUCCESS;
}

int hioi_dataset_shared_fini (hio_dataset_t dataset) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  if (hioi_context_using_mpi (context)) {
    if (MPI_WIN_NULL == dataset->ds_shared_win) {
      return HIO_SUCCESS;
    }

    MPI_Win_free (&dataset->ds_shared_win);
  }

  return HIO_SUCCESS;
}

#endif /* HIO_MPI_HAVE(3) */
