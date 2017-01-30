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

#include "hio_manifest.h"

#if HIO_MPI_HAVE(1)
int hioi_manifest_gather_comm (hio_manifest_t *manifest_out, MPI_Comm comm) {
  hio_context_t context = (*manifest_out)->context;
  long int recv_size_left = 0, recv_size_right = 0, send_size, alloc_size;
  int left, right, parent, c_rank, c_size = 1, rc, nreqs = 0;
  unsigned char *remote_data;
  hio_manifest_t manifest, manifest2;
  MPI_Request reqs[2];

  manifest = *manifest_out;

  if (hioi_context_using_mpi (context)) {
    MPI_Comm_size (comm, &c_size);
    MPI_Comm_rank (comm, &c_rank);

    parent = (c_rank - 1) >> 1;
    left = c_rank * 2 + 1;
    right = left + 1;

    /* the needs of this routine are a little more complicated than MPI_Reduce. the data size may
     * grow as the results are reduced. this function implements a basic reduction algorithm on
     * the hio dataset. prepost receives for child sizes. */

    if (right < c_size) {
      MPI_Irecv (&recv_size_right, 1, MPI_LONG, right, 1001, comm, reqs + 1);
      ++nreqs;
    }

    if (left < c_size) {
      MPI_Irecv (&recv_size_left, 1, MPI_LONG, left, 1001, comm, reqs);
      ++nreqs;
    }
  }

  if (1 == c_size) {
    return HIO_SUCCESS;
  }

  if (nreqs) {
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "waiting on %d requests", nreqs);

    hioi_timed_call(MPI_Waitall (nreqs, reqs, MPI_STATUSES_IGNORE));

    alloc_size = recv_size_right > recv_size_left ? recv_size_right : recv_size_left;
    if (0 >= alloc_size) {
      /* internal error for now */
      return HIO_ERROR;
    }

    remote_data = malloc (alloc_size);
    if (NULL == remote_data) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    if (right < c_size) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "receiving %lu bytes of manifest data from %d", recv_size_right,
                right);
      hioi_timed_call(MPI_Recv (remote_data, recv_size_right, MPI_CHAR, right, 1002, comm, MPI_STATUS_IGNORE));
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "merging manifest data from %d", right);
      rc = hioi_manifest_deserialize (context, remote_data, recv_size_right, &manifest2);
      if (HIO_SUCCESS != rc) {
        return rc;
      }

      hioi_timed_call(hioi_manifest_merge_data (manifest, manifest2));
      hioi_manifest_release (manifest2);
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "receiving %lu bytes of manifest data from %d", recv_size_left,
              left);
    hioi_timed_call(MPI_Recv (remote_data, recv_size_left, MPI_CHAR, left, 1002, comm, MPI_STATUS_IGNORE));
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "merging manifest data from %d", left);

    rc = hioi_manifest_deserialize (context, remote_data, recv_size_left, &manifest2);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    hioi_timed_call(hioi_manifest_merge_data (manifest, manifest2));
    hioi_manifest_release (manifest2);
    free (remote_data);
  }

  if (parent >= 0) {
    unsigned char *my_data;
    size_t my_data_size;

    rc = hioi_manifest_serialize (manifest, &my_data, &my_data_size, false);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    send_size = my_data_size;
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "sending %lu bytes of manifest data from %d to %d", send_size,
              c_rank, parent);

    MPI_Ssend (&send_size, 1, MPI_LONG, parent, 1001, comm);
    MPI_Send (my_data, send_size, MPI_CHAR, parent, 1002, comm);

    free (my_data);
  } else {
    *manifest_out = manifest;
  }

  return HIO_SUCCESS;
}

#endif /* HIO_MPI_HAVE(1) */
