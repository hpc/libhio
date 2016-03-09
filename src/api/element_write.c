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

#include "hio_types.h"

ssize_t hio_element_write (hio_element_t element, off_t offset, unsigned long reserved0, const void *ptr,
                           size_t count, size_t size) {
  return hio_element_write_strided (element, offset, reserved0, ptr, count, size, 0);
}

int hio_element_write_nb (hio_element_t element, hio_request_t *request, off_t offset,
                          unsigned long reserved0, const void *ptr, size_t count, size_t size) {
  return hio_element_write_strided_nb (element, request, offset, reserved0, ptr, count, size, 0);
}

ssize_t hio_element_write_strided (hio_element_t element, off_t offset, unsigned long reserved0, const void *ptr,
                                   size_t count, size_t size, size_t stride) {
  hio_request_t request = NULL;
  ssize_t bytes_transferred;
  int rc;

  rc = hio_element_write_strided_nb (element, &request, offset, reserved0, ptr, count, size, stride);
  if (HIO_SUCCESS != rc && NULL == request) {
      return rc;
  }

  hio_request_wait (&request, 1, &bytes_transferred);

  return bytes_transferred;
}

int hio_element_write_strided_nb (hio_element_t element, hio_request_t *request, off_t offset,
                                  unsigned long reserved0, const void *ptr, size_t count, size_t size,
                                  size_t stride) {
  if (NULL == element || offset < 0) {
    return HIO_ERR_BAD_PARAM;
  }

  int rc = element->e_write_strided_nb (element, request, offset, ptr, count, size, stride);
  if (HIO_SUCCESS != rc && (NULL == request || NULL == *request)) {
      return rc;
  }

  return rc;
}

int hio_element_flush (hio_element_t element, hio_flush_mode_t mode) {
  return element->e_flush (element, mode);
}

int hio_dataset_flush (hio_dataset_t dataset, hio_flush_mode_t mode) {
  hio_element_t element;

  hioi_list_foreach(element, dataset->ds_elist, struct hio_element, e_list) {
    int rc = element->e_flush (element, mode);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return HIO_SUCCESS;
}
