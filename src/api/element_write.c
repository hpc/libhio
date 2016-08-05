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
#include <string.h>

int hioi_dataset_buffer_append (hio_dataset_t dataset, hio_element_t element, off_t offset, const void *ptr,
                                size_t count, size_t size, size_t stride) {
  hio_buffer_t *buffer = &dataset->ds_buffer;
  hio_internal_request_t *req;
  int rc = HIO_SUCCESS;
  uint64_t start, stop;

  hioi_object_lock (&dataset->ds_object);

  if (buffer->b_reqcount) {
    /* check if this request can be appended to the previous one */
    req = (hio_internal_request_t *) buffer->b_reqlist.prev;
    if (req->ir_element != element || (req->ir_offset + req->ir_size) != offset) {
      /* create a new request */
      req = NULL;
    }
  } else {
    req = NULL;
  }

  for (size_t i = 0 ; i < count && HIO_SUCCESS == rc ; ++i) {
    for (size_t block = size, to_write = 0 ; block ; block -= to_write) {
      to_write = (buffer->b_remaining > block) ? block : buffer->b_remaining;

      start = hioi_gettime ();

      if (NULL == req) {
        /* allocate and fill in new request */
        req = calloc (1, sizeof (*req));
        if (NULL == req) {
          rc = HIO_ERR_OUT_OF_RESOURCE;
          break;
        }
        req->ir_element = element;
        req->ir_offset = offset;
        req->ir_data.w = (const void *)((intptr_t) buffer->b_base + buffer->b_size - buffer->b_remaining);
        req->ir_count = 1;
        req->ir_size = 0;
        req->ir_stride = 0;
        req->ir_type = HIO_REQUEST_TYPE_WRITE;
        req->ir_urequest = NULL;
        hioi_list_append (req, buffer->b_reqlist, ir_list);
        ++buffer->b_reqcount;
      }

      memcpy ((void *)((intptr_t) req->ir_data.r + req->ir_size), ptr, to_write);

      req->ir_size += to_write;
      buffer->b_remaining -= to_write;
      ptr = (const void *) ((intptr_t) ptr + to_write);
      offset += to_write;

      stop = hioi_gettime ();

      /* add buffering time to the overall write time */
      dataset->ds_stat.s_wtime += stop - start;

      if (block - to_write) {
        rc = hioi_dataset_buffer_flush (dataset);
        if (HIO_SUCCESS != rc) {
          break;
        }
        req = NULL;
      }
    }

    ptr = (const void *) ((intptr_t) ptr + stride);
  }

  hioi_object_unlock (&dataset->ds_object);

  return rc;
}

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
  hio_dataset_t dataset = hioi_element_dataset (element);
  hio_internal_request_t req, *reqs[1] = {&req};
  int rc;

  if (NULL == element || offset < 0) {
    return HIO_ERR_BAD_PARAM;
  }

  if (!(dataset->ds_flags & HIO_FLAG_WRITE)) {
    return HIO_ERR_PERM;
  }

  (void) atomic_fetch_add (&dataset->ds_stat.s_wcount, 1);

  if (size * count < (dataset->ds_buffer.b_size >> 2)) {
    rc = hioi_dataset_buffer_append (dataset, element, offset, ptr, count, size, stride);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    if (request) {
      hio_context_t context = hioi_object_context (&dataset->ds_object);
      hio_request_t new_request = hioi_request_alloc (context);
      if (NULL == new_request) {
        return HIO_ERR_OUT_OF_RESOURCE;
      }

      *request = new_request;
      new_request->req_transferred = size * count;
      new_request->req_complete = true;
      new_request->req_status = HIO_SUCCESS;
    }

    return HIO_SUCCESS;
  }

  req.ir_element = element;
  req.ir_offset = offset;
  req.ir_data.w = ptr;
  req.ir_count = count;
  req.ir_size = size;
  req.ir_stride = stride;
  req.ir_type = HIO_REQUEST_TYPE_WRITE;
  req.ir_urequest = request;

  return dataset->ds_process_reqs (dataset, (hio_internal_request_t **) &reqs, 1);
}

int hio_element_flush (hio_element_t element, hio_flush_mode_t mode) {
  hio_dataset_t dataset = hioi_element_dataset (element);
  int rc;

  rc = hioi_dataset_buffer_flush (dataset);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  return element->e_flush (element, mode);
}

int hio_dataset_flush (hio_dataset_t dataset, hio_flush_mode_t mode) {
  hio_element_t element;
  int rc;

  /* flush buffers to the backing store */
  rc = hioi_dataset_buffer_flush (dataset);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  hioi_list_foreach(element, dataset->ds_elist, struct hio_element, e_list) {
    rc = element->e_flush (element, mode);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return HIO_SUCCESS;
}
