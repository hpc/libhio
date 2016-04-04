#include <stdlib.h>
#include <stdio.h>

#include "hio_internal.h"

int main (int argc, char *argv[]) {
  hio_context_t context;
  int rc;

  rc = hio_init_single (&context, NULL, NULL, "FOO");
  if (HIO_SUCCESS != rc) {
    fprintf (stderr, "Could not initialize hio context FOO\n");
    hio_err_print_all (NULL, stderr, "error_test");
    return EXIT_FAILURE;
  }

  hioi_err_push (HIO_ERROR, NULL, "This is test error 1");
  rc = hio_err_print_last (NULL, stderr, "APP MESSAGE 1");
  if (0 >= rc) {
    fprintf (stderr, "Error printing last error message\n");
    hio_err_print_all (NULL, stderr, "error_test");
    hio_fini (&context);
    return EXIT_FAILURE;
  }

  hioi_err_push (HIO_ERROR, NULL, "This is another error");
  hioi_err_push (HIO_ERROR, NULL, "This is test error 2");
  hioi_err_push (HIO_ERROR, &context->c_object, "This is a context specific error");
  hioi_err_push (HIO_ERROR, NULL, "Last error");
  rc = hio_err_print_all (NULL, stderr, "APP MESSAGE");
  if (HIO_SUCCESS != rc) {
    fprintf (stderr, "Error printing all error messages\n");
    hio_fini (&context);
    return EXIT_FAILURE;
  }

  hio_err_print_all (context, stderr, "CONTEXT ERROR");

  hio_fini (&context);

  return 0;
}
