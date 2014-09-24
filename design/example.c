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

#define PRINT_AND_EXIT(args...) {                  \
    /* print out the error */                      \
    (void) hio_err_print_all (hio_context, args);  \
    MPI_Finalize ();                               \
    exit (EXIT_FAILURE)                            \
}

hio_context_t hio_context;
static int my_rank, nranks;

int initialize_hio (MPI_Comm comm) {
  hio_return_t hrc;

  MPI_Comm_rank (comm, &my_rank);
  MPI_Comm_size (comm, &nranks);

  /* initialize the hio state using mpi using the file input.deck for configuration */
  hrc = hio_init_mpi (&hio_context, &comm, "input.deck", "#HIO", "CFL");
  if (HIO_SUCCESS != hrc) {
    /* print out the error */
    (void) hio_err_print_all (hio_context, args);
    return -1;
  }

  return 0;
}

void finalize_hio (void) {
  hio_return_t hrc;

  hrc = hio_fini (&hio_context);
  if (HIO_SUCCESS != rc) {
    PRINT_AND_EXIT("Error encountered when finalizing HIO context");
  }
}

void checkpoint (void *data, size_t size, int timestep) {
  hio_element_t hio_element;
  hio_dataset_t hio_set;
  hio_request_t hio_requests[3], master_request;
  hio_return_t hrc;
  int hint;

  hio_should_checkpoint (hio_context, &hint);
  if (HIO_SCP_NOT_NOW == hint) {
    return;
  }

  /* Open the dataset associated with this timestep. The timestep can be used
   * later to ensure ordering when automatically rolling back to a prior
   * timestep on data failure. Specify that files in this dataset have unique
   * offset spaces. */
  hrc = hio_dataset_open (hio_context, &hio_set, "restart", timestep, HIO_FLAG_CREAT | HIO_FLAG_WRONLY,
                          HIO_SET_FILE_UNIQUE);
  if (HIO_SUCCESS != hrc) {
    PRINT_AND_EXIT("Could not open data set \"restart\"");
  }

  /* Set the stage mode on the dataset */
  hrc = hio_config_set_value (hio_set, "dataset_stage_mode", "lazy");
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not set stage mode configuration on HIO n-n dataset");
  }

  hrc = hio_config_set_value (hio_set, "dataset_stripe_width", "4096");
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not set stripe width configuration on HIO dataset");
  }

  hrc = hio_open (hio_set, &hio_element, "data", HIO_FLAG_CREAT | HIO_FLAG_EXCLUSIVE | HIO_FLAG_WRONLY, 0644);
  if (HIO_SUCCESS != rc) {
    PRINT_AND_EXIT("Could not open element \"data\" for writing");
  }

  offset = 0;
  hrc = hio_write (hio_element, offset, &size, sizeof (size_t));
  if (hrc > 0) {
    offset += hrc;
  }

  hrc = hio_write_strided (hio_element, offset, data, size, sizeof (float), 16);
  if (hrc > 0) {
    offset += hrc;
  }

  hrc = hio_write_strided (hio_element, offset, (void *)((intptr_t) data +
                           sizeof (float)), size, sizeof (int), 16);
  if (hrc > 0) {
    offset += hrc;
  }
  /* flush all data locally (not really needed here since the close will flush everything) */
  hrc = hio_flush (hio_element, HIO_FLUSH_MODE_LOCAL);
  if (HIO_SUCCESS != hrc) {
    PRINT_AND_EXIT("Error writing to HIO file");
  }

  /* close the file and force all data to be written to the backing store */
  hrc = hio_close (&hio_element);
  if (HIO_SUCCESS != rc) {
    PRINT_AND_EXIT("Error when closing HIO file");
  }

  hrc = hio_dataset_close (&hio_set);
  if (HIO_SUCCESS != rc) {
    PRINT_AND_EXIT("Error when closing n-n HIO dataset");
  }

  /* Open the dataset associated with this timestep. The timestep can be used
   * later to ensure ordering when automatically rolling back to a prior
   * timestep on data failure. Specify that files in this dataset have shared
   * offset spaces. */
  hrc = hio_dataset_open (hio_context, &hio_set, "restartShared", timestep, HIO_FLAG_CREAT | HIO_FLAG_WRONLY,
                          HIO_SET_FILE_SHARED);
  if (HIO_SUCCESS != hrc) {
    PRINT_AND_EXIT("Could not open data set \"restartShared\"");
  }

  /* example of writing out a shared file */
  hrc = hio_open (hio_set, &hio_element, "shared", HIO_FLAG_CREAT | HIO_FLAG_SHARED | HIO_FLAG_WRONLY, 0644);
  if (HIO_SUCCESS != rc) {
    PRINT_AND_EXIT("Could not open element \"shared\" for writing");
  }

  offset = my_rank * sizeof (size_t);
  hrc = hio_write_nb (hio_element, hio_requests, offset, &size, sizeof (size_t));

  offset = nranks * sizeof (size_t) + size * sizeof (float) * my_rank;
  hrc = hio_write_strided_nb (hio_element, hio_requests + 1, offset, data, size,
                              sizeof (float), 16);

  offset = nranks * (sizeof (size_t) + size * sizeof (float)) + my_rank * size * sizeof (int);
  hrc = hio_write_strided_nb (hio_element, hio_requests + 2, offset, (void *)((intptr_t) data +
                              sizeof (float)), size, sizeof (int), 16);

  hrc = hio_request_join (hio_requests, 3, &master_request);

  hrc = hio_wait (&master_request);

  hrc = hio_close (&hio_element);
  if (HIO_SUCCESS != rc) {
    PRINT_AND_EXIT("Error encountered when closing HIO element");
  }

  hrc = hio_set_close (&hio_set);
  if (HIO_SUCCESS != rc) {
    (void) hio_err_print_last (hio_context, "Error encountered when closing HIO dataset");
  }

  return EXIT_SUCCESS;
}
