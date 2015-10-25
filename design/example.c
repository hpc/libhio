/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/**
 * @file example.c
 * @brief Example usage of libhio
 *
 * This file demonstates the usage of libhio.
 */
#include <stdlib.h>
#include <stdio.h>

#include <mpi.h>

#include <hio.h>

#define IF_PRT_X(cond, args...)                            \
  if ((cond)) {                                            \
    (void) hio_err_print_all (hio_context, stderr, args);  \
    MPI_Finalize ();                                       \
    exit (EXIT_FAILURE);                                   \
  }

hio_context_t hio_context;
static int my_rank, nranks;

int initialize_hio (MPI_Comm comm) {
  hio_return_t hrc;

  MPI_Comm_rank (comm, &my_rank);
  MPI_Comm_size (comm, &nranks);

  /* initialize the hio state using mpi using the file input.deck for configuration */
  hrc = hio_init_mpi (&hio_context, &comm, "input.deck", "#HIO", "example_context");
  if (HIO_SUCCESS != hrc) {
    /* print out the error */
    (void) hio_err_print_all (hio_context, stderr, "error from hio_init_mpi");
    return -1;
  }

  /* add data will appear in example_context.hio */

  return 0;
}

void finalize_hio (void) {
  hio_return_t hrc;

  hrc = hio_fini (&hio_context);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_fini error %d", hrc);
}

void checkpoint (void *data, size_t size, int timestep) {
  hio_element_t hio_element;
  hio_dataset_t hio_set;
  hio_request_t hio_requests[3];
  ssize_t bytes_transferred[3];
  hio_return_t hrc;
  hio_recommendation_t hint;
 
  // Not yet implemented, fake it for now
  #define hio_should_checkpoint(a, b) *(b) = HIO_SCP_MUST_CHECKPOINT
 
  hio_should_checkpoint (hio_context, &hint);
  if (HIO_SCP_NOT_NOW == hint) {
    return;
  }

  /* Open the dataset associated with this timestep. The timestep can be used
   * later to ensure ordering when automatically rolling back to a prior
   * timestep on data failure. Specify that files in this dataset have unique
   * offset spaces. */
  hrc = hio_dataset_open (hio_context, &hio_set, "restart", timestep, HIO_FLAG_CREAT | HIO_FLAG_WRITE,
                          HIO_SET_ELEMENT_UNIQUE);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_open error %d", hrc);

  /* Set the stage mode on the dataset */
  hrc = hio_config_set_value ((hio_object_t)hio_set, "datawarp_stage_mode", "lazy");
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_config_set_value datawarp_stage_mode error %d", hrc);

  hrc = hio_config_set_value ((hio_object_t)hio_set, "stripe_width", "4096");
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_config_set_value stripe_width error %d", hrc);

  hrc = hio_element_open (hio_set, &hio_element, "data", HIO_FLAG_CREAT | HIO_FLAG_WRITE);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_open error %d", hrc);

  off_t offset = 0;
  offset += hio_element_write (hio_element, offset, 0, &size, 1, sizeof (size));

  offset += hio_element_write_strided (hio_element, offset, 0, data, size, sizeof (float), 16);

  hrc = hio_element_write_strided (hio_element, offset, 0, (void *)((intptr_t) data +
                           sizeof (float)), size, sizeof (int), 16);

  /* flush all data locally (not really needed here since the close will flush everything) */
  hrc = hio_element_flush (hio_element, HIO_FLUSH_MODE_LOCAL);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_flush error %d", hrc);

  /* close the file and force all data to be written to the backing store */
  hrc = hio_element_close (&hio_element);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_close error %d", hrc);

  /* collective call finalizing the dataset */
  hrc = hio_dataset_close (&hio_set);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_close error %d", hrc);

  /* Open the dataset associated with this timestep. The timestep can be used
   * later to ensure ordering when automatically rolling back to a prior
   * timestep on data failure. Specify that files in this dataset have shared
   * offset spaces. */
  hrc = hio_dataset_open (hio_context, &hio_set, "restartShared", timestep,
                          HIO_FLAG_CREAT | HIO_FLAG_WRITE, HIO_SET_ELEMENT_SHARED);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_open error %d", hrc);

  /* example of writing out a shared file */
  hrc = hio_element_open (hio_set, &hio_element, "shared", HIO_FLAG_CREAT | HIO_FLAG_WRITE);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_open error %d", hrc);

  offset = my_rank * sizeof (size_t);
  hrc = hio_element_write_nb (hio_element, hio_requests, offset, 0, &size, 1, sizeof (size_t));
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_write_nb error %d", hrc);

  offset = nranks * sizeof (size_t) + size * sizeof (float) * my_rank;
  hrc = hio_element_write_strided_nb (hio_element, hio_requests + 1, offset, 0, data, size,
                              sizeof (float), 16);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_write_strided_nb error %d", hrc);

  offset = nranks * (sizeof (size_t) + size * sizeof (float)) + my_rank * size * sizeof (int);
  hrc = hio_element_write_strided_nb (hio_element, hio_requests + 2, offset, 0, (void *)((intptr_t) data +
                              sizeof (float)), size, sizeof (int), 16);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_write_strided_nb error %d", hrc);

  hrc = hio_request_wait (hio_requests, 3, bytes_transferred);

  hrc = hio_element_close (&hio_element);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_close error %d", hrc);

  /* collective call finalizing the dataset */
  hrc = hio_dataset_close (&hio_set);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_close error %d", hrc);

  return;
}
