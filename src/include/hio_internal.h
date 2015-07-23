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

/**
 * @file context.h
 * @brief Internal hio context
 */

#if !defined(HIO_INTERNAL_H)
#define HIO_INTERNAL_H

#include "hio_config.h"

#if HIO_USE_MPI
#include <mpi.h>
#endif

#include "hio.h"

#include <stddef.h>
#include <inttypes.h>

#if defined(HAVE_PTHREAD_H)
#include <pthread.h>
#endif

#if defined(HAVE_SYS_TIME_H)
#include <sys/time.h>
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
 * Log a message to stderr.  Don't invoke directly, use hioi_log macro.
 *
 * @param[in] context  current context
 * @param[in] level    message log level
 * @param[in] format   output format
 * @param[in] ...      format arguments
 */
void hioi_log_unconditional (hio_context_t context, int level, char *format, ...);
 
#define hioi_log(context, level,  ...)                            \
  if ((context)->context_verbose >= level) {                      \
    hioi_log_unconditional ( (context), (level), __VA_ARGS__);    \
  }

/**
 * Return an hio error code for the given errno
 *
 * @param[in] err   error code
 *
 * @returns hio error code
 */
int hioi_err_errno (int err);


/**
 * Create HIO dataset modules based on the current data roots
 *
 * @param[in] context  context
 *
 * @returns hio error code
 */
int hioi_context_create_modules (hio_context_t context);

/**
 * Get the current time (relative to system boot) in usec
 *
 * @returns monotonically increasing time in usec
 */
uint64_t hioi_gettime (void);

/**
 * Make the component directories of the specified path
 *
 * @param[in] context     context - used for logging
 * @param[in] path        path to make
 * @param[in] access_mode permissions
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERROR on error
 *
 * Additional information on failures can be read from the
 * errno global variable. See the man page for mkdir(2) for
 * more information.
 */
int hio_mkpath (hio_context_t context, const char *path, mode_t access_mode);

#endif /* !defined(HIO_INTERNAL_H) */
