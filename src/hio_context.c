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
 * @file context.c
 * @brief hio context implementation
 */

#include "hio_internal.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/param.h>
#include <assert.h>

/**
 * Release all resources associated with a context
 */
static void hioi_context_release (hio_object_t object) {
  hio_dataset_data_t *ds_data, *next;
  hio_context_t context = (hio_context_t) object;

  for (int i = 0 ; i < context->c_mcount ; ++i) {
    context->c_modules[i]->fini (context->c_modules[i]);
  }

  hioi_config_list_release (&context->c_fconfig);

  /* clean up any mpi resources */
#if HIO_USE_MPI
  if (context->c_use_mpi) {
    int rc;

    rc = MPI_Comm_free (&context->c_comm);
    if (MPI_SUCCESS != rc) {
      hioi_err_push_mpi (rc, NULL, "Error freeing MPI communicator");
    }
  }
#endif

#if HIO_USE_DATAWARP
  free (context->c_dw_root);
#endif

#if HAVE_MPI_COMM_SPLIT_TYPE
  if (MPI_COMM_NULL != context->c_shared_comm) {
    MPI_Comm_free (&context->c_shared_comm);
  }

  free (context->c_shared_ranks);
#endif

  free (context->c_droots);

  /* clean up dataset data structures */
  hioi_list_foreach_safe(ds_data, next, context->c_ds_data, hio_dataset_data_t, dd_list) {
    hio_dataset_backend_data_t *db_data, *db_next;

    hioi_list_remove(ds_data, dd_list);

    /* clean up backend data */
    hioi_list_foreach_safe(db_data, db_next, ds_data->dd_backend_data, hio_dataset_backend_data_t, dbd_list) {
      hioi_list_remove(db_data, dbd_list);
      free ((void *) db_data->dbd_backend_name);
      free (db_data);
    }

    free ((void *) ds_data->dd_name);
    free (ds_data);
  }
}

/* Init or update the msg_id string.  The msg_id string is a preformatted
 * value (to reduce logging overhead) consisting of:
 *   <hostname>:<rank> <context_name>  (:<rank> only if MPI active)
 */
void hio_context_msg_id(hio_context_t context, int include_rank) {
  char tmp_id[256];
  char * p;
  int rc;

  /* Free any existing msg_id */
  free (context->c_msg_id);

  /* Get the hostname and truncate it at the first period */
  rc = gethostname(tmp_id, sizeof(tmp_id));
  if (0 != rc) strcpy(tmp_id, "<unknown>");
  p = strchr(tmp_id, '.');
  if (p) *p = '\0';
  p = tmp_id + strlen(tmp_id);

  /* Add the optional :rank string */
  if (include_rank) p += snprintf(p, sizeof(tmp_id) - (p - tmp_id), ":%d", context->c_rank);
 
  /* Add the context name */
  *p++ = ' '; 
  strncpy(p, context->c_object.identifier, sizeof(tmp_id) - (p - tmp_id));
  tmp_id[sizeof(tmp_id)-1] = '\0';
  
  /* Copy it into the context object */
  context->c_msg_id = strdup(tmp_id);
}


static hio_context_t hio_context_alloc (const char *identifier) {
  hio_context_t new_context;

  new_context = (hio_context_t) hioi_object_alloc (identifier, HIO_OBJECT_TYPE_CONTEXT,
                                                   NULL, sizeof (*new_context),
                                                   hioi_context_release);
  if (NULL == new_context) {
    return NULL;
  }

  /* default context configuration */
  new_context->c_verbose = HIO_VERBOSE_ERROR;
  new_context->c_print_stats = false;
  new_context->c_rank = 0;
  new_context->c_size = 1;
  hio_context_msg_id(new_context, 0); 

#if HAVE_MPI_COMM_SPLIT_TYPE
  new_context->c_shared_comm = MPI_COMM_NULL;
  new_context->c_shared_size = 1;
  new_context->c_shared_rank = 0;
#endif

  hioi_config_list_init (&new_context->c_fconfig);

  // If env set, pick up verbose value from context or global env name
  char buf[256];
  snprintf (buf, sizeof(buf), "HIO_context_%s_verbose", new_context->c_object.identifier);
  char *env_name = buf;
  char *verbose_env = getenv (env_name);
  if (!verbose_env) {
    env_name = "HIO_verbose";
    verbose_env = getenv (env_name);
  }

  if (verbose_env) {
    char * endptr;
    int verbose_val = strtol (verbose_env, &endptr, 0);
    if (*endptr) {
      hioi_log (new_context, HIO_VERBOSE_ERROR, "Environment variable %s value \"%s\" not valid",
                env_name, verbose_env);
    } else {
      new_context->c_verbose = verbose_val;
    }
  }

  hioi_list_init (new_context->c_ds_data);

  return new_context;
}

static int hioi_context_scatter (hio_context_t context) {
  int rc;

  rc = hioi_string_scatter (context, &context->c_droots);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

#if HIO_USE_DATAWARP
  rc = hioi_string_scatter (context, &context->c_dw_root);
  if (HIO_SUCCESS != rc) {
    return rc;
  }
#endif

  return HIO_SUCCESS;
}

int hioi_context_create_modules (hio_context_t context) {
  char *data_roots, *data_root, *next_data_root, *last;
  hio_module_t *module = NULL;
  int num_modules = 0;
  int rc;

  rc = hioi_context_scatter (context);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  data_roots = strdup (context->c_droots);
  if (NULL == data_roots) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  data_root = strtok_r (data_roots, ",", &last);

  do {
    next_data_root = strtok_r (NULL, ",", &last);

    rc = hioi_component_query (context, data_root, next_data_root, &module);
    if (HIO_SUCCESS != rc) {
      hioi_err_push (rc, &context->c_object, "Could not find an hio io module for data root %s",
                    data_root);
      break;
    }

    /* the module may be used in this context. see if the dataset size needs to
     * be increased for this module */
    if (context->c_ds_size < module->ds_object_size) {
      context->c_ds_size = module->ds_object_size;
    }

    context->c_modules[num_modules++] = module;
    if (HIO_MAX_DATA_ROOTS <= num_modules) {
      hioi_log (context, HIO_VERBOSE_WARN,
                "Maximum number of IO (%d) modules reached for this context", HIO_MAX_DATA_ROOTS);
      break;
    }
    data_root = next_data_root;
  } while (NULL != data_root);

  context->c_mcount = num_modules;
  context->c_cur_module = 0;

  free (data_roots);

  return rc;
}

static int hioi_context_set_default_droot (hio_context_t context) {
  char cwd_buffer[MAXPATHLEN] = "";
  int rc;

  /* default data root is the current working directory */
  if (NULL == getcwd (cwd_buffer, MAXPATHLEN)) {
    return HIO_ERROR;
  }

  rc = asprintf (&context->c_droots, "posix:%s", cwd_buffer);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  return HIO_SUCCESS;
}

static int hio_init_common (hio_context_t context, const char *config_file, const char *config_file_prefix,
                            const char *context_name) {
  int rc;

  rc = hioi_component_init (context);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, NULL, "Could not initialize the hio component interface");
    return rc;
  }

  rc = hioi_config_parse (context, config_file, config_file_prefix);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  hioi_config_add (context, &context->c_object, &context->c_verbose,
                   "verbose", HIO_CONFIG_TYPE_UINT32, NULL, "Debug level", 0);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Set context verbosity to %d", context->c_verbose);

  hioi_config_add (context, &context->c_object, &context->c_droots,
                   "data_roots", HIO_CONFIG_TYPE_STRING, NULL,
                   "Comma-separated list of data roots to use with this context "
                   "(default: posix:$PWD)", 0);

  /* if the data root string is empty set it to the default */
  if (NULL == context->c_droots || 0 == strlen (context->c_droots)) {
    rc = hioi_context_set_default_droot (context);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  hioi_config_add (context, &context->c_object, &context->c_print_stats,
                   "print_statistics", HIO_CONFIG_TYPE_BOOL, NULL, "Print statistics "
                   "to stdout when the context is closed (default: 0)", 0);

#if HIO_USE_DATAWARP
  context->c_dw_root = strdup ("auto");
  hioi_config_add (context, &context->c_object, &context->c_dw_root,
                   "datawarp_root", HIO_CONFIG_TYPE_STRING, NULL, "Mount path "
                   "for datawarp (burst-buffer) (default: auto-detect)", 0);
#endif

  hioi_perf_add (context, &context->c_object, &context->c_bread,
                 "bytes_read", HIO_CONFIG_TYPE_UINT64, NULL, "Total number of bytes "
                 "read in this context", 0);

  hioi_perf_add (context, &context->c_object, &context->c_bwritten,
                 "bytes_written", HIO_CONFIG_TYPE_UINT64, NULL, "Total number of bytes "
                 "written in this context", 0);

  if (context->c_verbose > HIO_VERBOSE_MAX) {
    context->c_verbose = HIO_VERBOSE_MAX;
  }

  return HIO_SUCCESS;
}

int hio_init_single (hio_context_t *new_context, const char *config_file, const char *config_file_prefix,
                     const char *context_name) {
  hio_context_t context;
  int rc;

  context = hio_context_alloc (context_name);
  if (NULL == context) {
    hioi_err_push (HIO_ERR_OUT_OF_RESOURCE, NULL, "Could not allocate space for new hio context");
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  rc = hio_init_common (context, config_file, config_file_prefix, context_name);
  if (HIO_SUCCESS != rc) {
    hioi_object_release (&context->c_object);
    return rc;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Created new single context with identifier %s",
            context_name);

  *new_context = context;

  return HIO_SUCCESS;
}

#if HIO_USE_MPI
#if HAVE_MPI_COMM_SPLIT_TYPE
static int hioi_context_init_shared (hio_context_t context) {
  int shared_rank, shared_size;
  MPI_Comm shared_comm;
  int rc;

  rc = MPI_Comm_split_type (context->c_comm, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL,
                            &shared_comm);
  if (MPI_SUCCESS != rc) {
    hioi_err_push_mpi (rc, &context->c_object, "Error splitting MPI communicator");
    return hioi_err_mpi (rc);
  }

  MPI_Comm_size (shared_comm, &shared_size);
  MPI_Comm_rank (shared_comm, &shared_rank);

  if (0 == shared_rank) {
    context->c_shared_ranks = calloc (shared_size, sizeof (int));
    if (NULL == context->c_shared_ranks) {
      MPI_Comm_free (&shared_comm);
      return HIO_ERR_OUT_OF_RESOURCE;
    }
  }
  MPI_Gather (&context->c_rank, 1, MPI_INT, context->c_shared_ranks, 1, MPI_INT,
              0, shared_comm);

  context->c_shared_comm = shared_comm;
  context->c_shared_rank = shared_rank;
  context->c_shared_size = shared_size;

  return HIO_SUCCESS;
}
#endif /* HAVE_MPI_COMM_SPLIT_TYPE */

int hio_init_mpi (hio_context_t *new_context, MPI_Comm *comm, const char *config_file,
                  const char *config_file_prefix, const char *context_name) {
  hio_context_t context;
  MPI_Comm comm_in;
  int rc, flag = 0;

  (void) MPI_Initialized (&flag);
  if (!flag) {
    hioi_err_push (HIO_ERROR, NULL, "Attempted to initialize hio before MPI");
    return HIO_ERROR;
  }

  (void) MPI_Finalized (&flag);
  if (flag) {
    hioi_err_push (HIO_ERROR, NULL, "Attempted to initialize hio after MPI was finalized");
    return HIO_ERROR;
  }

  context = hio_context_alloc (context_name);
  if (NULL == context) {
    hioi_err_push (HIO_ERR_OUT_OF_RESOURCE, NULL, "Could not allocate space for new hio context");
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  comm_in = comm ? *comm : MPI_COMM_WORLD;

  rc = MPI_Comm_dup (comm_in, &context->c_comm);
  if (MPI_COMM_NULL == context->c_comm) {
    hioi_err_push_mpi (rc, &context->c_object, "Error duplicating MPI communicator");
    hioi_object_release (&context->c_object);
    return hioi_err_mpi (rc);
  }

  context->c_use_mpi = true;

  MPI_Comm_rank (context->c_comm, &context->c_rank);
  MPI_Comm_size (context->c_comm, &context->c_size);
  hio_context_msg_id(context, 1); 

  rc = hio_init_common (context, config_file, config_file_prefix, context_name);
  if (HIO_SUCCESS != rc) {
    hioi_object_release (&context->c_object);
    return rc;
  }

#if HAVE_MPI_COMM_SPLIT_TYPE
  rc = hioi_context_init_shared (context);
  if (HIO_SUCCESS != rc) {
    hioi_object_release (&context->c_object);
    return rc;
  }
#endif

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Created new mpi context with identifier %s",
            context_name);

  *new_context = context;

  return HIO_SUCCESS;
}
#endif

int hio_fini (hio_context_t *context) {
  if (NULL == context || NULL == *context) {
    return HIO_SUCCESS;
  }

  hioi_log (*context, HIO_VERBOSE_DEBUG_LOW, "Destroying context with identifier %s",
            (*context)->c_object.identifier);
  
  free((*context)->c_msg_id); 
  hioi_object_release (&(*context)->c_object);
  *context = HIO_OBJECT_NULL;

  return hioi_component_fini ();
}

void hio_should_checkpoint (hio_context_t ctx, int *hint) {
  /* TODO -- implement this */
  *hint = HIO_SCP_MUST_CHECKPOINT;
}

hio_module_t *hioi_context_select_module (hio_context_t context) {
  /* TODO -- finish implementation */
  if (-1 == context->c_cur_module) {
    context->c_cur_module = 0;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Selected module %d with data root %s",
            context->c_cur_module, context->c_modules[context->c_cur_module]->data_root);

  return context->c_modules[context->c_cur_module];
}
