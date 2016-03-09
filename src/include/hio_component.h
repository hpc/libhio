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

#if !defined(HIO_COMPONENT_H)
#define HIO_COMPONENT_H

#include "hio_internal.h"

#if defined(HAVE_STDINT_H)
#include <stdint.h>
#endif

struct hio_module_t;
struct hio_dataset_header_t;

/**
 * Open a dataset with an hio module
 *
 * @param[in]  module      hio module in use
 * @param[in]  dataset     dataset object
 * @param[in]  name        dataset name
 * @param[in]  set_id      dataset identifier
 * @param[in]  flags       dataset open flags
 * @param[in]  mode        dataset mode
 *
 * @returns HIO_SUCCESS on success
 * @returns hio error code on failure
 *
 * This function is responsible for creating hio dataset objects. The module
 * is allowed to allocate as much space for the dataset as is needed to store
 * the hio_dataset_t structure and any internal state. If succesfully opened
 * the module must fill in the function pointers on the dataset object.
 */
typedef int (*hio_module_dataset_open_fn_t) (struct hio_module_t *module,
                                             hio_dataset_t dataset);

/**
 * Remove the specified dataset from the data root
 *
 * @param[in] module   hio module
 * @param[in] name     dataset name
 * @param[in] set_id   dataset identifier
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_NOT_FOUND if the specified dataset does not exist on the
 *          data root.
 * @returns hio error on other failure
 *
 * This function removes the specified dataset from the data root associated
 * with the hio module.
 */
typedef int
(*hio_module_dataset_unlink_fn_t) (struct hio_module_t *module,
				   const char *name, int64_t set_id);

/**
 * List all dataset identifiers on the data root for a given dataset name
 *
 * @param[in]  module       hio module associated with the dataset element
 * @param[in]  name         hio dataset name
 * @param[out] set_ids      available set ids
 * @param[out] set_id_count number of available set ids
 *
 * This functions queries the data root for all existing set identifiers associated
 * with the given dataset name. This function is allowed to return incomplete or
 * failed dataset identifiers.
 */
typedef int
(*hio_module_dataset_list_fn_t) (struct hio_module_t *module, const char *name,
                                 struct hio_dataset_header_t **headers, int *count);

/**
 * Finalize a module and release all resources.
 *
 * A well-designed version of this function will release all
 * resources belonging to the module. It is not safe to use
 * a module once it has been finalized.
 */
typedef int
(*hio_module_fini_fn_t) (struct hio_module_t *module);

typedef struct hio_module_t {
  /** open a dataset/id from this data root */
  hio_module_dataset_open_fn_t   dataset_open;

  /** delete a dataset/id from this data root */
  hio_module_dataset_unlink_fn_t  dataset_unlink;

  /** list all available datasets in this module's data root */
  hio_module_dataset_list_fn_t    dataset_list;

  /** function to finalize this module */
  hio_module_fini_fn_t            fini;

  /** associated hio context */
  hio_context_t                   context;

  /** backing store for this data root */
  char                           *data_root;

  /** minimum size needed for a dataset object */
  size_t                          ds_object_size;
} hio_module_t;

/**
 * Initialize the hio component.
 *
 * This function is responsible for the following:
 *  - Registering any component specific configuration and performance
 *    variables.
 *  - Verifying the component can be used.
 *  ...
 */
typedef int (*hio_component_init_fn_t) (hio_context_t);

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

/**
 * Initialize the hio component system
 *
 * This call locates and initializes all available backend components. Backend
 * components can be used to create backend modules for writing to various
 * types of data stores.
 */
int hioi_component_init (hio_context_t);

/**
 * Finalize and cleanup the hio component system
 */
int hioi_component_fini (void);

/**
 * Get an hio module for the given data root
 *
 * @param[in]   context        hio context
 * @param[in]   data_root      data root
 * @param[in]   next_data_root next data root on the context (or NULL)
 * @param[out]  module         new hio I/O module
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_NOT_FOUND if a backend module could not be found
 *
 * This function allocates and returns an hio module for the given data
 * root.
 */
int hioi_component_query (hio_context_t context, const char *data_root, const char *next_data_root,
                          hio_module_t **module);

#endif /* !defined(HIO_COMPONENT_H) */
