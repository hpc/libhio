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
 * @param[in]  module      hio module
 * @param[out] set_out     new dataset object
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
 * the hio_dataset_t structure and any internal state.
 */
typedef int
(*hio_module_dataset_open_fn_t) (struct hio_module_t *module,
				 hio_dataset_t *set_out, const char *name,
				 int64_t set_id, hio_flags_t flags,
				 hio_dataset_mode_t mode);

/**
 * Close a dataset and release any internal state
 *
 * @param[in] module  module associated with the dataset
 * @param[in] set     dataset object
 *
 * @returns HIO_SUCCESS if the dataset was successfully closed
 * @returns hio error code on failure
 *
 * This function closes the dataset and releases any internal data
 * stored on the dataset. The function is not allowed to release
 * the dataset itself. If the dataset has been modified the module
 * should ensure that either the data is committed to the data root
 * or the appropriate error code is returned.
 *
 * @note The module should not attempt to unlink/delete the data
 *       set if an error occurs. Instead, the module should be able
 *       to detect the failure on a subsequent open call. It is up
 *       to the hio user to unlink failed datsets.
 */
typedef int
(*hio_module_dataset_close_fn_t) (struct hio_module_t *module,
				  hio_dataset_t set);

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
 * Open an element of the dataset
 *
 * @param[in]  module       hio module associated with the dataset
 * @param[in]  set          hio dataset object
 * @param[out] element_out  new hio element object
 * @param[in]  element_name name of the hio dataset element
 * @param[in]  flags        element open flags
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_NOT_FOUND if the element does not exist (read-only datasets)
 * @returns HIO_ERR_OUT_OF_RESOURCE on resource exhaustion
 * @returns hio error on other failure
 *
 * This function opens a named element on an hio dataset.
 */
typedef int
(*hio_module_element_open_fn_t) (struct hio_module_t *module,
				 hio_dataset_t set,
				 hio_element_t *element_out,
				 const char *element_name,
				 hio_flags_t flags);

/**
 * Write to an hio dataset element
 *
 * @param[in]  module       hio module associated with the dataset element
 * @param[in]  element      hio dataset element object
 * @param[out] request      new request object if requested (can be NULL)
 * @param[in]  offset       element offset to write to
 * @param[in]  ptr          data to write
 * @param[in]  count        number of blocks to write
 * @param[in]  size         size of blocks
 * @param[in]  stride       number of bytes between blocks
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_PERM if the element can not be written to
 * @returns hio error on other error
 *
 * This function schedules data to be written to a dataset element. Modules are free
 * to delay any updates to the dataset element until hio_element_close() or
 * hio_element_flush() at the latest.
 */
typedef int
(*hio_module_element_write_strided_nb_fn_t) (struct hio_module_t *module,
                                             hio_element_t element,
                                             hio_request_t *request,
                                             off_t offset, void *ptr,
                                             size_t count, size_t size, size_t stride);

/**
 * Read from an hio dataset element
 *
 * @param[in]  module       hio module associated with the dataset element
 * @param[in]  element      hio dataset element object
 * @param[out] request      new request object if requested (can be NULL)
 * @param[in]  offset       element offset to read from
 * @param[in]  ptr          data to write
 * @param[in]  count        number of blocks to read
 * @param[in]  size         size of blocks
 * @param[in]  stride       number of bytes between blocks
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_PERM if the element can not be written to
 * @returns hio error on other error
 *
 * This function schedules data to be read from a dataset element. Modules are free
 * to delay any reads from the dataset element until hio_element_complete().
 */
typedef int
(*hio_module_element_read_strided_nb_fn_t) (struct hio_module_t *module,
                                            hio_element_t element,
                                            hio_request_t *request,
                                            off_t offset, void *ptr,
                                            size_t count, size_t size, size_t stride);

/**
 * Flush writes to a dataset element
 *
 * @param[in]  module       hio module associated with the dataset element
 * @param[in]  element      hio dataset element object
 * @param[in]  module       hio flush mode
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_PERM if the element can not be written to
 * @returns hio error on other error
 *
 * This function flushes all outstanding writes to a dataset element. When this
 * call returns the user should be free to update any buffers associated with
 * writes on the dataset element.
 */
typedef int
(*hio_module_element_flush_fn_t) (struct hio_module_t *module,
				  hio_element_t element,
				  hio_flush_mode_t mode);

/**
 * Complete all reads from a dataset element
 *
 * @param[in]  module       hio module associated with the dataset element
 * @param[in]  element      hio dataset element object
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_PERM if the element can not be read from
 * @returns hio error on other error
 *
 * This function completes all outstanding reads from a dataset element. When this
 * call returns all requested data (if available) should be available in the
 * user's buffers.
 */
typedef int
(*hio_module_element_complete_fn_t) (struct hio_module_t *module,
				     hio_element_t element);

/**
 * Close a dataset element
 *
 * @param[in] module       hio module associated with the dataset element
 * @param[in] element      element to close
 *
 * @returns HIO_SUCCESS on success
 * @returns hio error code if any error occurred on the element that has not
 *          already been returned
 *
 * This function closes and hio dataset element and frees any internal data
 * allocated by the backend module. hio modules are allowed to defer reporting
 * any errors until hio_element_close() or hio_dataset_close().
 */
typedef int
(*hio_module_element_close_fn_t) (struct hio_module_t *module,
				  hio_element_t element);

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
  hio_module_dataset_open_fn_t      dataset_open;
  hio_module_dataset_close_fn_t     dataset_close;
  hio_module_dataset_unlink_fn_t    dataset_unlink;

  hio_module_element_open_fn_t      element_open;
  hio_module_element_close_fn_t     element_close;

  hio_module_element_write_strided_nb_fn_t  element_write_strided_nb;
  hio_module_element_read_strided_nb_fn_t   element_read_strided_nb;

  hio_module_element_flush_fn_t     element_flush;
  hio_module_element_complete_fn_t  element_complete;

  hio_module_dataset_list_fn_t      dataset_list;

  hio_module_fini_fn_t              fini;

  hio_context_t                     context;
  char                             *data_root;
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

/**
 * Initialize the hio component system
 *
 * This call locates and initializes all available backend components. Backend
 * components can be used to create backend modules for writing to various
 * types of data stores.
 */
int hioi_component_init (void);

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
