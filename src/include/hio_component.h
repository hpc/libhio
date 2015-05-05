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

/**
 * @file component.h
 * @brief hio api component interface
 */

#if !defined(HIO_COMPONENT_H)
#define HIO_COMPONENT_H

#include "hio_module.h"

/**
 * Initialize the hio component.
 *
 * This function is responsible for the following:
 *  - Registering any component specific configuration and performance
 *    variables.
 *  - Verifying the component can be used.
 *  ...
 */
typedef int (*hio_component_init_fn_t) (void);

/**
 * Finalize the hio component.
 *
 * This function is responsible for the following:
 *  - Deregistering any component specific configuration and performance
 *    variables.
 *  ...
 */
typedef int (*hio_component_fini_fn_t) (void);

/**
 * Get an api module for the given data root
 *
 * This function generates an hio module for the given data root if
 * possible. If a module can not be created the function should
 * return the error code most closely matching the error and set
 * module to NULL.
 *
 * It is safe to push the error code onto the error stack.
 */
typedef int (*hio_component_query_t) (hio_context_t context, const char *data_root, const char *next_data_root,
                                      hio_module_t **module);

typedef uint64_t hio_component_flags_t;
enum {
  HIO_COMPONENT_FLAG_DEFAULT   = 0,
};

typedef struct hio_component_t {
  /** initialize the component */
  hio_component_init_fn_t init;
  /** finalize the component */
  hio_component_fini_fn_t fini;

  /** get an api module for the given data root */
  hio_component_query_t   query;

  /** flags */
  hio_component_flags_t   flags;

  /** relative priority 0-100 (0 - lowest, 100 - highest) */
  int                     priority;
} hio_component_t;

int hioi_component_init (void);
int hioi_component_fini (void);
int hioi_component_query (hio_context_t context, const char *data_root, const char *next_data_root,
                          hio_module_t **module);

#endif /* !defined(HIO_COMPONENT_H) */
