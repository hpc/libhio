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

ssize_t hio_write (hio_element_t element, off_t offset, unsigned long reserved0, void *ptr,
                   size_t count, size_t size) {
  hio_request_t request = NULL;
  ssize_t bytes_transferred;
  int rc;

  rc = hio_write_nb (element, &request, offset, reserved0, ptr, count, size);
  if (HIO_SUCCESS != rc && NULL == request) {
      return rc;
  }

  hio_wait (&request, &bytes_transferred);

  return bytes_transferred;
}

int hio_write_nb (hio_element_t element, hio_request_t *request, off_t offset,
                 unsigned long reserved0, void *ptr, size_t count, size_t size) {
  hio_dataset_t dataset = element->element_dataset;
  hio_module_t *module = dataset->dataset_module;
  int rc;

  rc = module->element_write_nb (module, element, request, offset, ptr,
                                 count, size);
  if (HIO_SUCCESS != rc && (NULL == request || NULL == *request)) {
      return rc;
  }

  return rc;
}

int hio_flush (hio_element_t element, hio_flush_mode_t mode) {
  hio_dataset_t dataset = element->element_dataset;
  hio_module_t *module = dataset->dataset_module;

  return module->element_flush (module, element, mode);
}

int hio_flush_all (hio_dataset_t dataset, hio_flush_mode_t mode) {
  hio_module_t *module = dataset->dataset_module;
  hio_element_t element;
  int rc;

  hioi_list_foreach(element, dataset->dataset_element_list, struct hio_element_t, element_list) {
    rc = module->element_flush (module, element, mode);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return HIO_SUCCESS;
}
