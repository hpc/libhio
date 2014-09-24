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

#if !defined(HIO_CONTEXT_H)
#define HIO_CONTEXT_H

#include "hio_internal.h"
#include "hio_component.h"

#include <sys/time.h>

struct hio_context_t {
  struct hio_object_t context_object;

#if HIO_USE_MPI
  /** internal communicator for this context */
  MPI_Comm            context_comm;
#endif

  /** my rank in the context */
  int                 context_rank;
  /** numner of ranks using this context */
  int                 context_size;

  /** unreported errors on this context */
  void               *context_error_stack;
  /** threading lock */
  pthread_mutex_t     context_lock;
  /** comma-separated list of data roots available */
  char               *context_data_roots;
  /** expected checkpoint size */
  uint64_t            context_checkpoint_size;
  /** print statistics on close */
  bool                context_print_statistics;
  /** number of bytes written to this context (local) */
  uint64_t            context_bytes_written;
  /** number of bytes read from this context (local) */
  uint64_t            context_bytes_read;
  /** context verbosity */
  uint32_t            context_verbose;
  /** time of last dataset completion */
  struct timeval      context_last_checkpoint;
  /** file configuration for the context */
  hio_config_kv_t    *context_file_configuration;
  int                 context_file_configuration_count;
  int                 context_file_configuration_size;

  /** io modules (one for each data root) */
  hio_module_t        *context_modules[HIO_MAX_DATA_ROOTS];
  /** number of data roots */
  int                  context_module_count;
  /** current active data root */
  int                  context_current_module;
};

static inline bool hioi_context_using_mpi (hio_context_t context) {
#if HIO_USE_MPI
  if (context->context_rank >= 0) {
    return true;
  }
#endif

  return false;
}

hio_module_t *hioi_context_select_module (hio_context_t context);

#endif /* !defined(HIO_CONTEXT_H) */
