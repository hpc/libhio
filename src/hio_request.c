/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_types.h"

#include <stdlib.h>

#include <sys/time.h>

hio_request_t hioi_request_alloc (hio_context_t context) {
  hio_request_t request;

  request = (hio_request_t) calloc (1, sizeof (*request));
  if (NULL == request) {
    return NULL;
  }

  request->request_object.type = HIO_OBJECT_TYPE_REQUEST;

  return request;
}

void hioi_request_release (hio_request_t request) {
  free (request);
}

int hio_test (hio_request_t *request, ssize_t *bytes_transferred, bool *complete) {
  if (*request == HIO_REQUEST_NULL) {
    *bytes_transferred = 0;
    *complete = true;
    return HIO_SUCCESS;
  }

  if ((*request)->request_complete) {
    *complete = true;
    *bytes_transferred = (*request)->request_transferred;
    *request = HIO_REQUEST_NULL;
  }

  return HIO_SUCCESS;
}

int hio_wait (hio_request_t *request, ssize_t *bytes_transferred) {
  if (*request == HIO_REQUEST_NULL) {
    *bytes_transferred = 0;
    return HIO_SUCCESS;
  }

  while (!(*request)->request_complete) {
    struct timespec interval = {.tv_sec = 0, .tv_nsec = 1000};
    nanosleep (&interval, NULL);
  }

  *bytes_transferred = (*request)->request_transferred;
  *request = HIO_REQUEST_NULL;
  return HIO_SUCCESS;
}
