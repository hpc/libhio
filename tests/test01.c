#include <stdlib.h>

#include <hio.h>


int main (int argc, char *argv[]) {
  int data[10] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29};
  int data2[10] = {1, 1, 2, 3, 5, 8, 13, 21, 34, 55};
  int data_read[10];
  hio_context_t context;
  hio_dataset_t dataset;
  hio_element_t element;
  int rc;

  rc = hio_init_single (&context, argv[1], "#HIO.", "foo");
  if (HIO_SUCCESS != rc) {
    fprintf (stderr, "Could not initialize hio\n");
    exit (EXIT_FAILURE);
  }

  rc = hio_dataset_open (context, &dataset, "restart", 100, HIO_FLAG_WRITE | HIO_FLAG_CREAT | HIO_FLAG_TRUNC,
			 HIO_SET_ELEMENT_SHARED);
  if (HIO_SUCCESS != rc) {
      if (HIO_ERR_EXISTS == rc) {
          hio_dataset_unlink (context, "restart", 100, HIO_UNLINK_MODE_FIRST);
          rc = hio_dataset_open (context, &dataset, "restart", 100, HIO_FLAG_WRITE | HIO_FLAG_CREAT,
                                 HIO_SET_ELEMENT_SHARED);

      }

      if (HIO_SUCCESS != rc) {
          fprintf (stderr, "Could not create restart dataset. reason: %d\n", rc);
          hio_fini (&context);
          return 1;
      }
  }

  rc = hio_element_open (dataset, &element, "data", 0);
  if (HIO_SUCCESS == rc) {
    rc = hio_element_write (element, 0, 0, data, 10, sizeof (int));
    if (0 > rc) {
      fprintf (stderr, "Could not write data to offset 0. reason: %d\n", rc);
    }

    rc = hio_element_write (element, 40, 0, data, 10, sizeof (int));
    if (0 > rc) {
      fprintf (stderr, "Could not write data to offset 0. reason: %d\n", rc);
    }

    rc = hio_element_write (element, 1000, 0, data2, 10, sizeof (int));
    if (0 > rc) {
      fprintf (stderr, "Could not write data to offset 0. reason: %d\n", rc);
    }

    rc = hio_element_close (&element);
    if (HIO_SUCCESS != rc) {
      fprintf (stderr, "Error closing element. reason: %d\n", rc);
    }
  } else {
      fprintf (stderr, "Could not create dataset element. reason: %d\n", rc);
  }

  rc = hio_dataset_close (&dataset);
  if (HIO_SUCCESS != rc) {
      fprintf (stderr, "Error closing dataset. reason: %d\n", rc);
  }



  rc = hio_dataset_open (context, &dataset, "restart", 50, HIO_FLAG_WRITE | HIO_FLAG_CREAT | HIO_FLAG_TRUNC,
			 HIO_SET_ELEMENT_SHARED);
  if (HIO_SUCCESS != rc) {
      if (HIO_ERR_EXISTS == rc) {
          hio_dataset_unlink (context, "restart", 100, HIO_UNLINK_MODE_FIRST);
          rc = hio_dataset_open (context, &dataset, "restart", 100, HIO_FLAG_WRITE | HIO_FLAG_CREAT,
                                 HIO_SET_ELEMENT_SHARED);

      }

      if (HIO_SUCCESS != rc) {
          fprintf (stderr, "Could not create restart dataset. reason: %d\n", rc);
          hio_fini (&context);
          return 1;
      }
  }

  rc = hio_element_open (dataset, &element, "data", 0);
  if (HIO_SUCCESS == rc) {
    rc = hio_element_write (element, 0, 0, data, 10, sizeof (int));
    if (0 > rc) {
      fprintf (stderr, "Could not write data to offset 0. reason: %d\n", rc);
    }

    rc = hio_element_write (element, 40, 0, data, 10, sizeof (int));
    if (0 > rc) {
      fprintf (stderr, "Could not write data to offset 0. reason: %d\n", rc);
    }

    rc = hio_element_write (element, 1000, 0, data2, 10, sizeof (int));
    if (0 > rc) {
      fprintf (stderr, "Could not write data to offset 0. reason: %d\n", rc);
    }

    rc = hio_element_close (&element);
    if (HIO_SUCCESS != rc) {
      fprintf (stderr, "Error closing element. reason: %d\n", rc);
    }
  } else {
      fprintf (stderr, "Could not create dataset element. reason: %d\n", rc);
  }

  rc = hio_dataset_close (&dataset);
  if (HIO_SUCCESS != rc) {
      fprintf (stderr, "Error closing dataset. reason: %d\n", rc);
  }



  rc = hio_dataset_open (context, &dataset, "restart", HIO_DATASET_ID_NEWEST, HIO_FLAG_READ,
			 HIO_SET_ELEMENT_SHARED);
  if (HIO_SUCCESS != rc) {
      fprintf (stderr, "Could not load restart dataset. reason: %d\n", rc);
      hio_fini (&context);
      return 1;
  }

  rc = hio_element_open (dataset, &element, "data", 0);
  if (HIO_SUCCESS == rc) {
    rc = hio_element_read (element, 1028, 0, data_read, 5, sizeof (int));
    if (0 < rc) {
      for (int i = 0 ; i < rc/sizeof(int) ; ++i) {
	fprintf (stderr, "Read %d: %d\n", i, data_read[i]);
      }
    } else {
      fprintf (stderr, "Error reading from dataset. reason: %d\n", rc);
    }

    rc = hio_element_close (&element);
    if (HIO_SUCCESS != rc) {
      fprintf (stderr, "Error closing element. reason: %d\n", rc);
    }
  } else {
      fprintf (stderr, "Could not create dataset element. reason: %d\n", rc);
  }

  rc = hio_dataset_close (&dataset);
  if (HIO_SUCCESS != rc) {
      fprintf (stderr, "Error closing dataset. reason: %d\n", rc);
  }

  (void) hio_fini (&context);

  return 0;
}
