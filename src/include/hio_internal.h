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

#if !defined(HIO_INTERNAL_H)
#define HIO_INTERNAL_H

#include "hio_config.h"

#include <stddef.h>

#if defined(HAVE_PTHREAD_H)
#include <pthread.h>
#endif

/**
 * Maximum number of data roots.
 */
#define HIO_MAX_DATA_ROOTS   64

typedef enum {
  HIO_OBJECT_TYPE_CONTEXT,
  HIO_OBJECT_TYPE_DATASET,
  HIO_OBJECT_TYPE_ELEMENT,
  HIO_OBJECT_TYPE_REQUEST,
  HIO_OBJECT_TYPE_ANY,
} hio_object_type_t;

/**
 * Verbosity levels
 */
enum {
  HIO_VERBOSE_ERROR      = 0,
  HIO_VERBOSE_WARN       = 10,
  HIO_VERBOSE_DEBUG_LOW  = 20,
  HIO_VERBOSE_DEBUG_MED  = 50,
  HIO_VERBOSE_DEBUG_HIGH = 90,
  HIO_VERBOSE_MAX        = 100,
};

struct hio_config_t;

/**
 * Base of all hio objects
 */
struct hio_object_t {
  hio_object_type_t type;

  /** identifer for this object (context, dataset, or element name) */
  char             *identifier;

  /** in hio configuration is done per context, dataset, or element.
   * this part of the object stores all the registered configuration
   * variables */
  hio_config_t      configuration;
};

/**
 * @brief Push an hio error onto the hio error stack
 *
 * @param[in] hrc     hio error code
 * @param[in] context hio context
 * @param[in] object  hio object in use at the time of the error
 * @param[in] format  error string to push
 * @param[in] ...     error string arguments
 *
 * This function pushes the specified error string onto the error stack.
 */
void hio_err_push (int hrc, hio_context_t context, hio_object_t object, char *format, ...);

/**
 * @brief Push an MPI error onto the hio error stack
 *
 * @param[in] mpirc   MPI error code
 * @param[in] context hio context
 * @param[in] object  hio object in use at the time of the error
 * @param[in] format  error string to push
 * @param[in] ...     error string arguments
 *
 * This function pushes the specified error string onto the error stack
 * and appends the MPI error string.
 */
void hio_err_push_mpi (int mpirc, hio_context_t context, hio_object_t object, char *format, ...);

/**
 * @brief Return hio error code for MPI error code.
 *
 * @param[in] mpirc   MPI error code
 *
 * @returns hio error code that is equivalent to the mpi error code
 *
 * This is a helper function that will give the closest hio error code to
 * the provided mpi error code.
 */
int hio_err_mpi (int mpirc);

/**
 * Log a message to stderr
 *
 * @param[in] context  current context
 * @param[in] level    message log level
 * @param[in] format   output format
 * @param[in] ...      format arguments
 */
void hioi_log (hio_context_t context, int level, char *format, ...);

/**
 * Return an hio error code for the given errno
 *
 * @param[in] err   error code
 *
 * @returns hio error code
 */
int hioi_err_errno (int err);

#endif /* !defined(HIO_INTERNAL_H) */
