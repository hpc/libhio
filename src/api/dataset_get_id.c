/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "hio_types.h"

int hio_dataset_get_id (hio_dataset_t dataset, int64_t *set_id) {
  if (NULL == dataset || NULL == set_id) {
    return HIO_ERR_BAD_PARAM;
  }

  *set_id = dataset->ds_id;

  return HIO_SUCCESS;
}
