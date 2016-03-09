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

#include <string.h>

hio_return_t hio_object_get_name (hio_object_t object, char **name_out) {
  if (NULL == name_out || HIO_OBJECT_NULL == object) {
    return HIO_ERR_BAD_PARAM;
  }

  *name_out = strdup (object->identifier);
  if (NULL == *name_out) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }
  return HIO_SUCCESS;
}
