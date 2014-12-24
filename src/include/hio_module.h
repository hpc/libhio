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

#if !defined(HIO_MODULE_H)
#define HIO_MODULE_H

#include "config.h"

#include "hio.h"

#if defined(HAVE_STDINT_H)
#include <stdint.h>
#endif

struct hio_module_t;

typedef int
(*hio_module_dataset_open_fn_t) (struct hio_module_t *module,
				 hio_dataset_t *set_out, const char *name,
				 int64_t set_id, hio_flags_t flags,
				 hio_dataset_mode_t mode);

typedef int
(*hio_module_dataset_close_fn_t) (struct hio_module_t *module,
				  hio_dataset_t set);

typedef int
(*hio_module_dataset_unlink_fn_t) (struct hio_module_t *module,
				   const char *name, int64_t set_id);

typedef int
(*hio_module_element_open_fn_t) (struct hio_module_t *module,
				 hio_dataset_t set,
				 hio_element_t *element_out,
				 const char *element_name,
				 hio_flags_t flags);

typedef int
(*hio_module_element_write_nb_fn_t) (struct hio_module_t *module,
				     hio_element_t element,
				     hio_request_t *request,
				     off_t offset, void *ptr,
				     size_t count, size_t size);

typedef int
(*hio_module_element_read_nb_fn_t) (struct hio_module_t *module,
				    hio_element_t element,
				    hio_request_t *request,
				    off_t offset, void *ptr,
				    size_t count, size_t size);

typedef int
(*hio_module_element_flush_fn_t) (struct hio_module_t *module,
				  hio_element_t element,
				  hio_flush_mode_t mode);

typedef int
(*hio_module_element_complete_fn_t) (struct hio_module_t *module,
				     hio_element_t element);

typedef int
(*hio_module_element_close_fn_t) (struct hio_module_t *module,
				  hio_element_t element_out);

typedef int
(*hio_module_dataset_list_fn_t) (struct hio_module_t *module, const char *name,
                                 int64_t **set_ids, int *set_id_count);

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

  hio_module_element_write_nb_fn_t  element_write_nb;
  hio_module_element_read_nb_fn_t   element_read_nb;

  hio_module_element_flush_fn_t     element_flush;
  hio_module_element_complete_fn_t  element_complete;

  hio_module_dataset_list_fn_t      dataset_list;

  hio_module_fini_fn_t              fini;

  hio_context_t                     context;
  char                             *data_root;
} hio_module_t;

#endif /* !defined(HIO_MODULE_H) */
