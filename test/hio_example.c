/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/**
 * @file hio_example.c
 * @brief Example usage of libhio
 *
 * This file demonstates the usage of libhio.
 */
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#include <mpi.h>
#include <hio.h>

/* To disable api rc trace, comment out the first fprintf line */
#define IF_PRT_X(cond, args...)                                      \
  fprintf(stderr, "Trace: " args); fprintf(stderr, "\n");            \
  if ((cond)) {                                                      \
    fprintf(stderr, "Error: " args); fprintf(stderr, "\n");          \
    (void) hio_err_print_all (hio_context, stderr, "Msg: " args);  \
    MPI_Finalize ();                                                 \
    exit (EXIT_FAILURE);                                             \
  }

hio_context_t hio_context;
static int my_rank, nranks;

int initialize_hio (MPI_Comm comm) {
  hio_return_t hrc;

  MPI_Comm_rank (comm, &my_rank);
  MPI_Comm_size (comm, &nranks);

  /* initialize the hio state using mpi using the file input.deck for configuration */
  hrc = hio_init_mpi (&hio_context, &comm, "hio_example.input.deck", "#HIO", "hio_example_context");
  if (HIO_SUCCESS != hrc) {
    /* print out the error */
    (void) hio_err_print_all (hio_context, stderr, "error from hio_init_mpi");
    return -1;
  }

  /* add data will appear in hio_example_context.hio */

  return 0;
}

void finalize_hio (void) {
  hio_return_t hrc;

  hrc = hio_fini (&hio_context);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_fini rc: %d", hrc);
}

/* Dummy hio_should_checkpoint -- will be available in 1.4 */
hio_recommendation_t hio_dataset_should_checkpoint(hio_context_t ctx, const char * name) {
  /* Always return that it's time to checkpoint */
  return HIO_SCP_MUST_CHECKPOINT;
}

void checkpoint (void *data, size_t count, size_t stride, int timestep) {
  hio_element_t hio_element;
  hio_dataset_t hio_set;
  hio_request_t hio_requests[3];
  ssize_t bytes_transferred[3];
  hio_return_t hrc;
  hio_recommendation_t hint;
 
  hint = hio_dataset_should_checkpoint (hio_context, "restart");
  if (HIO_SCP_NOT_NOW == hint) return;

  /* Open the dataset associated with this timestep. The timestep can be used
   * later to ensure ordering when automatically rolling back to a prior
   * timestep on data failure. Specify that files in this dataset have unique
   * offset spaces. */
  hrc = hio_dataset_alloc (hio_context, &hio_set, "restart", timestep, HIO_FLAG_CREAT | HIO_FLAG_WRITE,
                          HIO_SET_ELEMENT_UNIQUE);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_alloc rc: %d", hrc);

  /* Set the stage mode on the dataset */
  hrc = hio_config_set_value ((hio_object_t)hio_set, "datawarp_stage_mode", "lazy");
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_config_set_value datawarp_stage_mode rc: %d", hrc);

  hrc = hio_config_set_value ((hio_object_t)hio_set, "stripe_width",
                              "4096");
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_config_set_value stripe_width rc: %d", hrc);

  hrc = hio_dataset_open(hio_set);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_open rc: %d", hrc);

  hrc = hio_element_open (hio_set, &hio_element, "data", HIO_FLAG_CREAT | HIO_FLAG_WRITE);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_open rc: %d", hrc);

  /* Print all HIO config and performance variables to stdout */
  hrc = hio_print_vars ((hio_object_t)hio_element, ".", ".",
                         stdout, "print_vars:");
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_print_vars rc: %d", hrc);

  off_t offset = 0; /* Offset for next write */
  ssize_t bw;       /* bytes written */

  bw = hio_element_write (hio_element, offset, 0, &count, 1, sizeof (count));
  IF_PRT_X( bw < 0, "hio_element_write bw: %zd", bw);
  offset += bw;

  bw = hio_element_write_strided (hio_element, offset, 0, data, count, sizeof (float), stride);
  IF_PRT_X( bw < 0, "hio_element_write_strided bw: %zd", bw);
  offset += bw;

  bw = hio_element_write_strided (hio_element, offset, 0,
             (void *)((intptr_t) data + sizeof (float)),
              count, sizeof (int), stride);
  IF_PRT_X( bw < 0, "hio_element_write_strided bw: %zd", bw);

  /* flush all data locally (not really needed here since the close will flush everything) */
  hrc = hio_element_flush (hio_element, HIO_FLUSH_MODE_LOCAL);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_flush rc: %d", hrc);

  /* close the file and force all data to be written to the backing store */
  hrc = hio_element_close (&hio_element);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_close rc: %d", hrc);

  /* collective call finalizing the dataset */
  hrc = hio_dataset_close (hio_set);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_close rc: %d", hrc);

  /* Dataset performance variables coule be retrieved between close and free */
  hrc = hio_dataset_free (&hio_set);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_close rc: %d", hrc);

  /* Open the dataset associated with this timestep. The timestep can be used
   * later to ensure ordering when automatically rolling back to a prior
   * timestep on data failure. Specify that files in this dataset have shared
   * offset spaces. */
  hrc = hio_dataset_alloc (hio_context, &hio_set, "restartShared", timestep,
                          HIO_FLAG_CREAT | HIO_FLAG_WRITE, HIO_SET_ELEMENT_SHARED);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_alloc rc: %d", hrc);
  hrc = hio_dataset_open (hio_set);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_open rc: %d", hrc);

  /* example of writing out a shared file */
  hrc = hio_element_open (hio_set, &hio_element, "shared", HIO_FLAG_CREAT | HIO_FLAG_WRITE);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_open rc: %d", hrc);

  offset = my_rank * sizeof (size_t);
  hrc = hio_element_write_nb (hio_element, hio_requests, offset, 0, &count, 1, sizeof (size_t));
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_write_nb rc: %d", hrc);

  offset = nranks * sizeof (size_t) + count * sizeof (float) * my_rank;
  hrc = hio_element_write_strided_nb (hio_element, hio_requests + 1, offset, 0, data, count,
                              sizeof (float), stride);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_write_strided_nb rc: %d", hrc);

  offset = nranks * (sizeof (size_t) + count * sizeof (float)) + my_rank * count * sizeof (int);
  hrc = hio_element_write_strided_nb (hio_element, hio_requests + 2, offset, 0,
           (void *)((intptr_t) data + sizeof (float)), count, sizeof (int), stride);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_write_strided_nb rc: %d", hrc);

  hrc = hio_request_wait (hio_requests, 3, bytes_transferred);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_request_wait rc: %d", hrc);

  hrc = hio_element_close (&hio_element);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_element_close rc: %d", hrc);

  /* collective call finalizing the dataset */
  hrc = hio_dataset_close (hio_set);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_close rc: %d", hrc);
  hrc = hio_dataset_free (&hio_set);
  IF_PRT_X( HIO_SUCCESS != hrc, "hio_dataset_free rc: %d", hrc);

  return;
}

int main(int argc, char * * argv) {
  float data[4096];

  MPI_Init(NULL, NULL);

  initialize_hio(MPI_COMM_WORLD);
  checkpoint(data, 32, 8, 1);
  finalize_hio();

  MPI_Finalize();

  return 0;

}
