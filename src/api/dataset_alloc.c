/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_types.h"

hio_return_t hio_dataset_alloc (hio_context_t context, hio_dataset_t *set_out, const char *name,
                                int64_t set_id, int flags, hio_dataset_mode_t mode) {
  if (NULL == set_out || NULL == name || HIO_OBJECT_NULL == context) {
    return HIO_ERR_BAD_PARAM;
  }

  if (0 == context->c_mcount) {
    /* create hio modules for each item in the specified data roots */
    int rc = hioi_context_create_modules (context);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  *set_out = hioi_dataset_alloc (context, name, set_id, flags, mode);
  if (HIO_OBJECT_NULL == *set_out) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  return HIO_SUCCESS;
}


hio_return_t hio_dataset_free (hio_dataset_t *dataset) {
  if (NULL == dataset) {
    return HIO_ERR_BAD_PARAM;
  }

  hioi_object_release (&(*dataset)->ds_object);
  *dataset = HIO_OBJECT_NULL;

  return HIO_SUCCESS;
}
