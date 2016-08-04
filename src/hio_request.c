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

#include "hio_internal.h"

#include <stdlib.h>

#include <sys/time.h>

hio_request_t hioi_request_alloc (hio_context_t context) {
  hio_request_t request;

  request = (hio_request_t) calloc (1, sizeof (*request));
  if (NULL == request) {
    return NULL;
  }

  request->req_object.type = HIO_OBJECT_TYPE_REQUEST;

  return request;
}

void hioi_request_release (hio_request_t request) {
  if (HIO_OBJECT_NULL != request) {
    free (request);
  }
}

int hio_request_test_internal (hio_request_t *requests, int nrequests, ssize_t *bytes_transferred, bool *complete,
                               bool noset_null) {
  int ncomplete = 0;

  for (int i = 0 ; i < nrequests ; ++i) {
    if (requests[i] == HIO_OBJECT_NULL) {
      if (!noset_null) {
        if (bytes_transferred) {
          bytes_transferred[i] = 0;
        }

        if (complete) {
          complete[i] = true;
        }
      }

      ++ncomplete;
    } else if (requests[i]->req_complete) {
      if (complete) {
        complete[i] = true;
      }

      if (bytes_transferred) {
        bytes_transferred[i] = requests[i]->req_transferred;
      }

      hioi_request_release (requests[i]);
      requests[i] = HIO_OBJECT_NULL;
      ++ncomplete;
    }
  }

  return ncomplete;
}

int hio_request_test (hio_request_t *requests, int nrequests, ssize_t *bytes_transferred, bool *complete) {
  if (NULL == requests) {
    return HIO_ERR_BAD_PARAM;
  }

  return hio_request_test_internal (requests, nrequests, bytes_transferred, complete, false);
}

int hio_request_wait (hio_request_t *requests, int nrequests, ssize_t *bytes_transferred) {
  bool first = true;
  int rc;

  do {
    rc = hio_request_test_internal (requests, nrequests, bytes_transferred, NULL, !first);
    if (nrequests == rc) {
      return HIO_SUCCESS;
    }

    if (0 > rc) {
      return rc;
    }

    first = false;

    struct timespec interval = {.tv_sec = 0, .tv_nsec = 1000};
    nanosleep (&interval, NULL);
  } while (1);

  return HIO_SUCCESS;
}
