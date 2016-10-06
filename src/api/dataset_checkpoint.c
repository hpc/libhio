/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "hio_internal.h"

#include <stdlib.h>
#include <math.h>

static int hioi_get_numnodes (void) {
  const char *num_nodes;

  num_nodes = getenv ("SLURM_STEP_NUM_NODES");
  if (NULL == num_nodes) {
    num_nodes = getenv ("PBS_NUM_NODES");
    if (NULL == num_nodes) {
      num_nodes = "1";
    }
  }

  return strtol (num_nodes, NULL, 10);
}

static uint64_t hioi_calculate_interval (hio_context_t context, uint64_t write_time) {
  int nodes = hioi_get_numnodes ();
  double mtti, d;

  /* NTH -- TODO: calculate the approximate time to write a checkpoint based on:
   *  1) number of nodes
   *  2) average memory/node
   *  3) filesystem bandwidth
   * The filesystem bandwidth will probably have to be an additional system parameter. */

  /* write_time is in us. calculate in minutes */
  d = (double) write_time;

  mtti = 3600.0e9 / ((double) (context->c_job_sys_int_rate + (context->c_job_node_int_rate + context->c_job_node_sw_rate) * nodes));

  if (d < 2.0 * mtti) {
    return (uint64_t) floor (sqrt (2.0 * d * mtti) * (1.0 + sqrt (d / (2.0 * mtti)) / 3.0 + (d / (18.0 * mtti))) - d);
  }

  return (int64_t) floor(mtti);
}

hio_recommendation_t hio_dataset_should_checkpoint (hio_context_t context, const char *name) {
  int recommendation = HIO_SCP_NOT_NOW;

  if (hioi_signal_time) {
    /* SIGUSR1 can be used to warn about the upcoming end of job. If the signal has
     * been received then go ahead and update the job end time to match what the
     * perceived end of job time is. */
    context->c_end_time = hioi_signal_time + context->c_job_sigusr1_warning_time;
  }

  if (time (NULL) > context->c_end_time) {
    return HIO_SCP_NOT_NOW;
  }

  if (0 == context->c_rank) {
    uint64_t interval, since_last, current_time, left, write_time;
    hio_dataset_data_t *ds_data = NULL;

    current_time = time (NULL);
    left = context->c_end_time - current_time;

    (void) hioi_dataset_data_lookup (context, name, &ds_data);

    /* calculate write time in seconds */
    write_time = ds_data->dd_average_write_time / 1000000;

    interval = hioi_calculate_interval (context, write_time);
    since_last = (current_time - ds_data->dd_last_completion);

    if ((since_last / 60) >= interval || (left > ((write_time * 11) / 10) && left < ((write_time * 12) / 10))) {
      recommendation = HIO_SCP_MUST_CHECKPOINT;
    }
  }

#if HIO_MPI_HAVE(1)
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (&recommendation, 1, MPI_INT, 0, context->c_comm);
  }
#endif

  return recommendation;
}
