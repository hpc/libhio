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
 * @file context.h
 * @brief Internal hio context
 */

#if !defined(HIO_CONFIG_H)
#define HIO_CONFIG_H

#include "hio.h"
#include "config.h"

#if defined(HAVE_STDINT_H)
#include <stdint.h>
#endif

enum {
  /** default flag: read-write variable */
  HIO_VAR_FLAG_DEFAULT  = 0,
  /** variable is not constant but is read-only */
  HIO_VAR_FLAG_READONLY = 1,
  /** variable value will never change (informational) */
  HIO_VAR_FLAG_CONSTANT = 2,
};

typedef union hio_var_value_t {
  bool     boolval;
  char    *strval;
  int32_t  int32val;
  uint32_t uint32val;
  int64_t  int64val;
  uint64_t uint64val;
  float    floatval;
  double   doubleval;
} hio_var_value_t;

typedef struct hio_config_kv_t {
  int object_type;
  char *object_identifier;

  char *key;
  char *value;
} hio_config_kv_t;

typedef struct hio_config_var_t {
  /** unique name for this variable (allocated) */
  char             *var_name;
  /** basic type */
  hio_config_type_t var_type;
  /** location where this variable is stored */
  hio_var_value_t  *var_storage;
  /** variable flags (read only, etc) */
  int               var_flags;
  /** brief description (allocated) */
  const char       *var_description;
} hio_config_var_t;

typedef struct hio_config_t {
  /** array of configurations */
  hio_config_var_t *config_var;
  /** current number of valid configurations */
  int               config_var_count;
  /** current size of configuration array */
  int               config_var_size;
} hio_config_t;

/**
 * Find the index of the given variable in the configuation
 *
 * @param[in] config  configuration
 * @param[in] name    variable name to look up
 *
 * @returns -1 if not found
 * @returns index if found
 */
int hioi_config_lookup (hio_config_t *config, const char *name);

/**
 * Add a configuration to an object
 *
 * @param[in] context     context for file values
 * @param[in] object      object to modify
 * @param[in] addr        storage address for this variable
 * @param[in] name        variable name
 * @param[in] type        variable type
 * @param[in] reserved0   reserved for future use (must be NULL)
 * @param[in] description brief description of the variable (must
 *                        persist at least as long as the object)
 * @param[in] flags       variable flags
 */
int hioi_config_add (hio_context_t context, hio_object_t object, void *addr, const char *name,
		     hio_config_type_t type, void *reserved0, const char *description, int flags);

int hioi_config_parse (hio_context_t context, const char *config_file, const char *prefix);

/**
 * Initialize the configuration component of an hio object
 *
 * @param[in] object  object to initialize
 */
int hioi_config_init (hio_object_t object);

/**
 * Finalize the configuration component of an hio object
 *
 * @param[in] object  object to finalize
 */
void hioi_config_fini (hio_object_t object);

#endif /* !defined(HIO_CONFIG_H) */
