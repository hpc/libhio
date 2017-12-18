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
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#if !defined(PATH_MAX)
#define PATH_MAX 2048
#endif

/* define this for here until we have a fix */
#if !defined(OMPI_MAJOR_VERSION)

#define HIO_CRAY_BUFFER_BUG_1 1

#else
/* no such issue with Open MPI */

#define HIO_CRAY_BUFFER_BUG_1 0

#endif

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
  size_t req_count = hioi_list_length (&dataset->ds_buffer.b_reqlist);
  hio_internal_request_t **reqs, *req, *next;
  int rc;

  if (0 == req_count) {
    /* nothing to do */
    return HIO_SUCCESS;
  }

  reqs = malloc (sizeof (*reqs) * req_count);
  if (NULL == reqs) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* sort the request list and pass it off to the backend */
  int i = 0;
  hioi_list_foreach_safe(req, next, dataset->ds_buffer.b_reqlist, hio_internal_request_t, ir_list) {
    reqs[i++] = req;
    hioi_list_remove (req, ir_list);
  }

  qsort ((void *) reqs, req_count, sizeof (*reqs), request_compare);

  rc = dataset->ds_process_reqs (dataset, reqs, req_count);

  /* NTH: this is temporary code to plug a leak until better code is ready */
  for (i = 0 ; i < req_count ; ++i) {
    free (reqs[i]);
  }

  free (reqs);
  /* end temporary code */

  dataset->ds_buffer.b_remaining = dataset->ds_buffer.b_size;

  return rc;
}

#if HIO_MPI_HAVE(3)

/* NTH: There is a bug in Cray MPICH that prevents us from writing to DataWarp from a buffer that
 * was allocated using MPI_Win_shared_allocate(). For this reason we use mmap instead. I will
 * re-enable this code once I can confirm the bug is fixed. */
#if !HIO_CRAY_BUFFER_BUG_1

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

#if !HAVE_CRAY_BUFFER_BUG_1
  dataset->ds_buffer.b_size = ds_buffer_size;
  dataset->ds_buffer.b_remaining = ds_buffer_size;
#endif

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
    /* reset the buffer pointer to NULL so it isn't freed again */
    dataset->ds_buffer.b_base = NULL;
  }

  return HIO_SUCCESS;
}

#else

int hioi_dataset_shared_init (hio_dataset_t dataset, int stripes) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  size_t ds_buffer_size = dataset->ds_buffer_size;
  size_t control_block_size, shared_region_size, data_size;
  char file_name[PATH_MAX];
  int rc, disp_unit;
  void *base;
  int fd;

  if (MPI_COMM_NULL == context->c_shared_comm) {
    return HIO_SUCCESS;
  }

  /* ensure data block starts on a cache line boundary */
  control_block_size = (sizeof (hio_shared_control_t) + stripes * sizeof (dataset->ds_shared_control->s_stripes[0]) + 127) & ~127;
  /* NTH: There is a bug in Cray DataWarp that prevents us from writing to DataWarp from a buffer that
   * was allocated using MPI_Win_shared_allocate(). For this reason the shared buffers are disabled
   * and will be re-enabled once the bug is fixed. */
  data_size = context->c_shared_size * ds_buffer_size + control_block_size * (0 == context->c_shared_rank);
  shared_region_size = control_block_size + ds_buffer_size * context->c_shared_size;

  if (0 == access ("/dev/shm", R_OK)) {
    snprintf (file_name, sizeof (file_name), "/dev/shm/hio_buffer.%s.%" PRIx64 ".tmp", hioi_object_identifier (&dataset->ds_object),
              dataset->ds_id);
  } else {
    snprintf (file_name, sizeof (file_name), "/tmp/hio_buffer.%s.%" PRIx64 ".tmp", hioi_object_identifier (&dataset->ds_object),
              dataset->ds_id);
  }

  if (0 == context->c_shared_rank) {
    fd = open (file_name, O_CREAT | O_RDWR, 0644);
    if (-1 != fd) {
      rc = ftruncate (fd, shared_region_size);
      if (-1 == rc) {
        close (fd);
        unlink (file_name);
        fd = -1;
      }
    }
  }

  MPI_Barrier (context->c_shared_comm);

  if (0 != context->c_shared_rank) {
    fd = open (file_name, O_RDWR);
  }

  if (-1 == fd) {
    return HIO_ERR_NOT_AVAILABLE;
  }

  /* attempt to map the file */
  base = (void *) mmap (NULL, shared_region_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

  /* make sure all ranks have mapped the file before tearing it down */
  MPI_Barrier (context->c_shared_comm);

  /* no longer need the backing file */
  close (fd);

  if (0 == context->c_shared_rank) {
    close (fd);
    unlink (file_name);
  }

  if (MAP_FAILED == base) {
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
  }

  dataset->ds_buffer.b_base = (void *)((intptr_t) base + control_block_size + ds_buffer_size * context->c_shared_rank);
  dataset->ds_buffer.b_size = ds_buffer_size;
  dataset->ds_buffer.b_remaining = ds_buffer_size;

  dataset->ds_shared_control = (hio_shared_control_t *) base;
  dataset->ds_shared_region_size = shared_region_size;

  MPI_Barrier (context->c_shared_comm);

  return HIO_SUCCESS;
}

int hioi_dataset_shared_fini (hio_dataset_t dataset) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  if (hioi_context_using_mpi (context)) {
    munmap (dataset->ds_shared_control, dataset->ds_shared_region_size);
    /* reset the buffer pointer to NULL so it isn't freed again */
    dataset->ds_buffer.b_base = NULL;
    dataset->ds_shared_control = NULL;
    dataset->ds_shared_region_size = 0;
  }

  return HIO_SUCCESS;
}
#endif /* HIO_CRAY_BUFFER_BUG_1 */

#endif /* HIO_MPI_HAVE(3) */
