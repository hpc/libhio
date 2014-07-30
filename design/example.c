#include <stdlib.h>
#include <stdio.h>

#include <mpi.h>

#include <hio.h>


int main (int argc, char *argv[]) {
  MPI_Comm comm_world;
  hio_context_t hio_context;
  hio_return_t hrc;
  char *err_string;
  int rc;

  /* initialize MPI state */
  rc = MPI_Init (NULL, NULL);
  if (MPI_SUCCESS != rc) {
    exit (EXIT_FAILURE);
  }

  /* HIO needs a pointer to a communicator containing all processes that will
   * participate in the IO. Since MPI_COMM_WORLD may be a #define we need to
   * store the communicator. */
  comm_world = MPI_COMM_WORLD;

  /* initialize the hio state using mpi */
  hrc = hio_init_mpi (&hio_context, &comm_world, "restart.data");
  if (HIO_SUCCESS != hrc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not initialize HIO context");

    MPI_Finalize ();
    exit (EXIT_FAILURE);
  }

  /* parse hio options from a file */
  hrc = hio_config_parse_file (hio_context, "input.deck", "#HIO.");
  if (HIO_SUCCESS != hrc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not parse HIO configuration options from input file");

    hio_fini (&hio_context);

    MPI_Finalize ();
    exit (EXIT_FAILURE);
  }

  /* example of writing out a file per rank.
   * file will appear on filesystem as restart.data.hio/rank.%d */
  ret = asprintf (&filename, "step.%d", timestep);
  if (0 > ret) {
    (void) hio_fini (&hio_context);
    MPI_Finalize ();
    exit (EXIT_FAILURE);
  }

  /* Open the dataset associated with this timestep. The timestep can be used
   * later to ensure ordering when automatically rolling back to a prior
   * timestep on data failure. */
  hrc = hio_set_open (hio_context, &hio_set, filename, timestep, HIO_FLAG_CREAT);
  if (HIO_SUCCESS != hrc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not open data set %s", filename);
    free (filename);
    MPI_Finalize ();
    exit (EXIT_FAILURE)
  }

  hrc = hio_set_hint (hio_set, "hio_stage_mode", "lazy");
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not set stage mode hint on HIO file");
  }

  /* example of writing out a file per rank.
   * file will appear on filesystem as restart.data.hio/rank.%d */
  ret = asprintf (&filename, "rank.%d", rank);
  if (0 > ret) {
    (void) hio_fini (&hio_context);
    MPI_Finalize ();
    exit (EXIT_FAILURE);
  }

  hrc = hio_open (hio_context, &hio_fh, filename, HIO_FLAG_CREAT |
		  HIO_FLAG_EXCLUSIVE | HIO_FLAG_WRONLY | HIO_FLAG_NONBLOCK, 0644);
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not open %s for writing");

    MPI_Finalize ();
    exit (EXIT_FAILURE);
  }
  /* no longer need this */
  free (filename);

  hrc = hio_set_hint (hio_fh, "hio_stripe_width", "4096");
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not set stripe width hint on HIO file");
  }

  hrc = hio_file_write (hio_fh, offset, HIO_OFFSET_BEGIN, &size, sizeof (size_t));
  hrc = hio_file_write_strided (hio_fh, 0, HIO_OFFSET_LAST, data, size,
                                sizeof (float), 16, size, sizeof (float), 0);
  hrc = hio_file_write_strided (hio_fh, 0, HIO_OFFSET_LAST, (void *)((intptr_t) data +
                                sizeof (float)), size, sizeof (int), 16, 0);
  /* flush all data locally (not really needed here since the close will flush everything) */
  hrc = hio_flush (hio_fh, HIO_FLUSH_MODE_LOCAL);

  /* close the file and force all data to be written to the backing store */
  hrc = hio_close (&hio_fh);
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Error when closing HIO file");

    MPI_Finalize ();
    exit (EXIT_FAILURE);
  }

  /* example of writing out a shared file. file will show up on the
   * filesystem as restart.data.hio/shared.* with a description at
   * restart.data.hio/shared.layout.xml */
  hrc = hio_open (hio_context, hio_set, &hio_fh, "shared", HIO_FLAG_CREAT |
                  HIO_FLAG_SHARED | HIO_FLAG_WRONLY, 0644);
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not open shared for writing");

    MPI_Finalize ();
    exit (EXIT_FAILURE);
  }

  hrc = hio_set_hint (hio_fh, "hio_stripe_width", "4096");
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not set stipe width hint for file");
  }

  hrc = hio_set_hint (hio_fh, "hio_write_mode", "no_overlap");
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Could not set write mode hint for file");
  }

  offset = my_rank * sizeof (size_t);
  hrc = hio_file_write_nb (hio_fh, offset, hio_requests, &size, sizeof (size_t));

  offset = nranks * sizeof (size_t) + size * sizeof (float) * my_rank;
  hrc = hio_file_write_strided_nb (hio_fh, offset, hio_requests + 1, data, size,
                                   sizeof (float), 16, size, sizeof (float), 0);

  offset = nranks * (sizeof (size_t) + size * sizeof (float)) + my_rank * size * sizeof (int);
  hrc = hio_file_write_strided_nb (hio_fh, offset, hio_requests + 2, (void *)((intptr_t) data +
                                   sizeof (float)), size, sizeof (int), 16, 0);

  hrc = hio_request_join (hio_requests, 3, &master_request);

  hrc = hio_wait (&master_request);

  hrc = hio_close (&hio_fh);
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Error encountered when closing HIO file");

    MPI_Finalize ();
    exit (EXIT_FAILURE);
  }

  hrc = hio_set_close (&hio_set);
  if (HIO_SUCCESS != rc) {
    (void) hio_err_print_last (hio_context, "Error encountered when closing HIO data set");
  }

  hrc = hio_fini (&hio_context);
  if (HIO_SUCCESS != rc) {
    /* print out the error */
    (void) hio_err_print_last (hio_context, "Error encountered when finalizing HIO context");

    MPI_Finalize ();
    exit (EXIT_FAILURE);
  }

  MPI_Finalize ();

  return EXIT_SUCCESS;
}
