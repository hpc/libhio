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
  if (HIO_OBJECT_NULL != request) {
    free (request);
  }
}

int hio_request_test (hio_request_t *request, ssize_t *bytes_transferred, bool *complete) {
  if (*request == HIO_OBJECT_NULL) {
    *bytes_transferred = 0;
    *complete = true;
    return HIO_SUCCESS;
  }

  if ((*request)->request_complete) {
    *complete = true;
    *bytes_transferred = (*request)->request_transferred;
    hioi_request_release (request);
    *request = HIO_OBJECT_NULL;
  }

  return HIO_SUCCESS;
}

int hio_request_wait (hio_request_t *request, ssize_t *bytes_transferred) {
  bool complete = false;
  int rc;

  rc = hio_request_test (request, bytes_transferred, &complete);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  while (!complete) {
    struct timespec interval = {.tv_sec = 0, .tv_nsec = 1000};
    nanosleep (&interval, NULL);
    rc = hio_request_test (request, bytes_transferred, &complete);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return HIO_SUCCESS;
}
