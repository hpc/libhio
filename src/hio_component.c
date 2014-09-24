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

#include "hio_component.h"

extern hio_component_t builtin_posix_component;

hio_component_t *hio_builtin_components[] = {&builtin_posix_component, NULL};

static int hio_component_init_count = 0;

int hioi_component_init (void) {
  int rc;

  if (hio_component_init_count++ > 0) {
    return HIO_SUCCESS;
  }

  for (int i = 0 ; hio_builtin_components[i] ; ++i) {
    hio_component_t *component = hio_builtin_components[i];

    rc = component->init ();
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return rc;
}

int hioi_component_fini (void) {
  if (0 == hio_component_init_count || --hio_component_init_count) {
    return HIO_SUCCESS;
  }

  for (int i = 0 ; hio_builtin_components[i] ; ++i) {
    hio_component_t *component = hio_builtin_components[i];

    (void) component->fini ();
  }

  return HIO_SUCCESS;
}

int hioi_component_query (hio_context_t context, const char *data_root, hio_module_t **module) {
  int rc;

  for (int i = 0 ; hio_builtin_components[i] ; ++i) {
    hio_component_t *component = hio_builtin_components[i];

    rc = component->query (context, data_root, module);
    if (HIO_SUCCESS == rc) {
      return HIO_SUCCESS;
    }
  }

  return HIO_ERR_NOT_FOUND;
}
