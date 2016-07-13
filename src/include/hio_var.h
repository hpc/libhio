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

/**
 * @file context.h
 * @brief Internal hio context
 */

#if !defined(HIO_VAR_H)
#define HIO_VAR_H

#include "hio_types.h"

/**
 * Find the index of the given variable in the configuration
 *
 * @param[in] config  configuration
 * @param[in] name    variable name to look up
 *
 * @returns -1 if not found
 * @returns index if found
 */
int hioi_var_lookup (hio_var_array_t *var_array, const char *name);

/**
 * Add a configuration to an object
 *
 * @param[in] context     context for file values
 * @param[in] object      object to modify
 * @param[in] addr        storage address for this variable
 * @param[in] name        variable name
 * @param[in] type        variable type
 * @param[in] enum        value enumerator (integer types only) may be NULL
 * @param[in] description brief description of the variable (must
 *                        persist at least as long as the object)
 * @param[in] flags       variable flags
 */
int hioi_config_add (hio_context_t context, hio_object_t object, void *addr, const char *name,
		     hio_config_type_t type, hio_var_enum_t *var_enum, const char *description, int flags);

int hioi_config_parse (hio_context_t context, const char *config_file, const char *prefix);

int hioi_perf_add (hio_context_t context, hio_object_t object, void *addr, const char *name,
                   hio_config_type_t type, void *reserved0, const char *description, int flags);

void hioi_config_list_init (hio_config_kv_list_t *list);
void hioi_config_list_release (hio_config_kv_list_t *list);

/**
 * Initialize the configuration component of an hio object
 *
 * @param[in] object  object to initialize
 */
int hioi_var_init (hio_object_t object);

/**
 * Finalize the configuration component of an hio object
 *
 * @param[in] object  object to finalize
 */
void hioi_var_fini (hio_object_t object);

#endif /* !defined(HIO_VAR_H) */
