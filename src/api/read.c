/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_types.h"

ssize_t hio_read (hio_element_t element, off_t offset, unsigned long reserved0, void *ptr,
                  size_t count, size_t size) {
  hio_request_t request = NULL;
  ssize_t bytes_transferred;
  int rc;

  rc = hio_read_nb (element, &request, offset, reserved0, ptr, count, size);
  if (HIO_SUCCESS != rc && NULL == request) {
      return rc;
  }

  hio_wait (&request, &bytes_transferred);

  return bytes_transferred;
}

int hio_read_nb (hio_element_t element, hio_request_t *request, off_t offset,
                 unsigned long reserved0, void *ptr, size_t count, size_t size) {
  hio_dataset_t dataset = element->element_dataset;
  hio_module_t *module = dataset->dataset_module;
  int rc;

  if (NULL == element) {
    return HIO_ERR_BAD_PARAM;
  }

  rc = module->element_read_nb (module, element, request, offset, ptr,
                                count, size);
  if (HIO_SUCCESS != rc && (NULL == request || NULL == *request)) {
      return rc;
  }

  return rc;
}
