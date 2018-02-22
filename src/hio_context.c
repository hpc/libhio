/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC.  All rights
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

/** used to scan the environment */
extern char **environ;

/**
 * Release all resources associated with a context
 */
static void hioi_context_release (hio_object_t object) {
  hio_dataset_data_t *ds_data, *next;
  hio_context_t context = (hio_context_t) object;

  for (int i = 0 ; i < context->c_mcount ; ++i) {
    hioi_module_release (context->c_modules[i]);
  }

  hioi_config_list_release (&context->c_fconfig);

  /* clean up any mpi resources */
#if HIO_MPI_HAVE(1)
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

#if HIO_MPI_HAVE(3)
  if (MPI_COMM_NULL != context->c_shared_comm) {
    MPI_Comm_free (&context->c_shared_comm);
  }

  free (context->c_shared_ranks);
  free (context->c_node_leaders);

  if (MPI_COMM_NULL != context->c_node_leader_comm) {
    MPI_Comm_free (&context->c_node_leader_comm);
  }
#endif

  free (context->c_droots);

  /* clean up dataset data structures */
  hioi_list_foreach_safe(ds_data, next, context->c_ds_data, hio_dataset_data_t, dd_list) {
    hio_dataset_backend_data_t *db_data, *db_next;

    hioi_list_remove(ds_data, dd_list);

    /* clean up backend data */
    hioi_list_foreach_safe(db_data, db_next, ds_data->dd_backend_data, hio_dataset_backend_data_t, dbd_list) {
      hioi_list_remove(db_data, dbd_list);
      if (db_data->dbd_release_fn) {
        db_data->dbd_release_fn (db_data);
      }
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
static void hioi_context_msg_id(hio_context_t context, int include_rank) {
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
  hioi_context_msg_id(new_context, 0); 

#if HIO_MPI_HAVE(3)
  new_context->c_shared_comm = MPI_COMM_NULL;
  new_context->c_node_leader_comm = MPI_COMM_NULL;
  new_context->c_node_leaders = NULL;
  new_context->c_node_count = 0;
#endif

  new_context->c_shared_size = 1;
  new_context->c_shared_rank = 0;

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

#if HIO_MPI_HAVE(3)
int hioi_context_generate_leader_list (hio_context_t context) {
  int color = (0 == context->c_shared_rank) ? 0 : MPI_UNDEFINED;
  MPI_Comm leader_comm;
  int my_rank = 0, rc;

  if (NULL != context->c_node_leaders || !hioi_context_using_mpi (context)) {
    return HIO_SUCCESS;
  }

  hioi_object_lock (&context->c_object);
  if (NULL != context->c_node_leaders) {
    hioi_object_unlock (&context->c_object);
    return HIO_SUCCESS;
  }

  rc = MPI_Comm_split (context->c_comm, color, 0, &leader_comm);
  if (MPI_SUCCESS != rc) {
    hioi_object_unlock (&context->c_object);
    return hioi_err_mpi (rc);
  }

  if (MPI_COMM_NULL != leader_comm) {
    MPI_Comm_size (leader_comm, &context->c_node_count);
    MPI_Comm_rank (leader_comm, &my_rank);
  }

  if (1 < context->c_shared_size) {
    MPI_Bcast (&context->c_node_count, 1, MPI_INT, 0, context->c_shared_comm);
  }

  context->c_node_leaders = malloc (sizeof (int) * context->c_node_count);

  if (MPI_COMM_NULL != leader_comm) {
    context->c_node_leaders[my_rank] = context->c_rank;
    MPI_Allgather (MPI_IN_PLACE, 1, MPI_INT, context->c_node_leaders, 1, MPI_INT,
                   leader_comm);
  }

  if (1 < context->c_shared_size) {
    MPI_Bcast (context->c_node_leaders, context->c_node_count, MPI_INT, 0,
               context->c_shared_comm);
  }

  context->c_node_leader_comm = leader_comm;

  hioi_object_unlock (&context->c_object);

  return HIO_SUCCESS;
}
#endif

int hioi_context_add_data_root (hio_context_t context, const char *data_root) {
  hio_module_t *module;
  int rc = HIO_SUCCESS;
  bool found = false;
  char *tmp;

  hioi_object_lock (&context->c_object);

  do {
    for (int i = 0 ; i < context->c_mcount ; ++i) {
      if (context->c_modules[i]->compare (context->c_modules[i], data_root)) {
        found = true;
        break;
      }
    }

    if (found) {
      break;
    }

    if (HIO_MAX_DATA_ROOTS <= context->c_mcount) {
      hioi_log (context, HIO_VERBOSE_WARN, "Maximum number of IO modules (%d) reached for this context",
                HIO_MAX_DATA_ROOTS);
      rc = HIO_ERR_OUT_OF_RESOURCE;
      break;
    }

    rc = hioi_component_query (context, data_root, NULL, &module);
    if (HIO_SUCCESS != rc) {
      hioi_log (context, HIO_VERBOSE_WARN, "Could not find an hio io module for data root %s", data_root);
      break;
    }

    /* the module may be used in this context. see if the dataset size needs to
     * be increased for this module */
    if (context->c_ds_size < module->ds_object_size) {
      context->c_ds_size = module->ds_object_size;
    }

    context->c_modules[context->c_mcount++] = module;

    rc = asprintf (&tmp, "%s,%s", context->c_droots, data_root);
    if (0 > rc) {
      rc = HIO_ERR_OUT_OF_RESOURCE;
      break;
    }

    rc = HIO_SUCCESS;

    free (context->c_droots);
    context->c_droots = tmp;
  } while (0);

  hioi_object_unlock (&context->c_object);

  return rc;
}

static char **hioi_context_data_root_to_array (const char *data_roots) {
  char *data_root_tmp = strdup (data_roots), **out, *data_root, *last;
  int root_count = 1;

  for (int i = 0 ; data_root_tmp[i] ; ++i) {
    if (',' == data_root_tmp[i]) {
      ++root_count;
    }
  }

  out = calloc (root_count + 1, sizeof (char *));
  if (NULL == out) {
    free (data_root_tmp);
    return NULL;
  }

  data_root = strtok_r (data_root_tmp, ",", &last);
  for (int i = 0 ; i < root_count ; ++i) {
    out[i] = strdup (data_root);
    data_root = strtok_r (NULL, ",", &last);
  }

  free (data_root_tmp);

  return out;
}

void hioi_context_data_root_array_release (char **array) {
  for (int i = 0 ; array[i] ; ++i) {
    free (array[i]);
  }

  free (array);
}

int hioi_context_create_modules (hio_context_t context) {
  hio_module_t *module = NULL;
  int num_modules = 0;
  char **roots;
  int rc;

  rc = hioi_context_scatter (context);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  roots = hioi_context_data_root_to_array (context->c_droots);
  if (NULL == roots) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  for (int i = 0 ; roots[i] ; ++i) {
    rc = hioi_component_query (context, roots[i], roots + i + 1, &module);
    if (HIO_SUCCESS != rc) {
      hioi_log (context, HIO_VERBOSE_WARN, "Could not find an hio io module for data root %s", roots[i]);
      continue;
    }

    /* the module may be used in this context. see if the dataset size needs to
     * be increased for this module */
    if (context->c_ds_size < module->ds_object_size) {
      context->c_ds_size = module->ds_object_size;
    }

    context->c_modules[num_modules++] = module;
    if (HIO_MAX_DATA_ROOTS <= num_modules) {
      hioi_log (context, HIO_VERBOSE_WARN, "Maximum number of IO modules (%d) reached for this context",
                HIO_MAX_DATA_ROOTS);
      break;
    }
  }

  hioi_context_data_root_array_release (roots);

  context->c_mcount = num_modules;
  context->c_cur_module = 0;

  return rc;
}

static int hioi_context_set_default_droot (hio_context_t context) {
  bool persistent_seen = false, non_persistent_seen = false;
  char cwd_buffer[MAXPATHLEN] = "";
  int datawarp_mount_count = 0;
  const char *last_mount_seen = NULL;
  const int dw_persistent_striped_length = 21;
  char *data_root_tmp = NULL, *tmp;
  int rc;

  /* default data root is the current working directory */
  if (NULL == getcwd (cwd_buffer, MAXPATHLEN)) {
    return HIO_ERROR;
  }

  data_root_tmp = getenv ("DW_JOB_STRIPED");
  if (NULL != data_root_tmp) {
    /* make sure the jobdw root is first */
    data_root_tmp = strdup ("datawarp,");
  }

  /* add all available datawarp mounts to the default data root list */
  for (int i = 0 ; environ[i] ; ++i) {
    if (0 == strncmp (environ[i], "DW_PERSISTENT_STRIPED", dw_persistent_striped_length)) {
      char *name = strdup (environ[i] + dw_persistent_striped_length + 1);

      tmp = strchr (name, '=');
      *tmp = '\0';

      tmp = data_root_tmp;

      /* named persistent datawarp mount */
      persistent_seen = true;
      rc = asprintf (&tmp, "%sdatawarp-%s,", data_root_tmp ? data_root_tmp : "",
                     name);
      free (name);
      free (data_root_tmp);

      data_root_tmp = tmp;

      if (0 > rc) {
        return HIO_ERR_OUT_OF_RESOURCE;
      }
    }
  }

  rc = asprintf (&context->c_droots, "%sposix:%s",  data_root_tmp ? data_root_tmp : "", cwd_buffer);
  free (data_root_tmp);

  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "default data roots: %s", context->c_droots);

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

  hioi_config_add (context, &context->c_object, &context->c_enable_tracing,
                   "enable_tracing", NULL, HIO_CONFIG_TYPE_BOOL, NULL, "Enable full tracing", 0);

  hioi_config_add (context, &context->c_object, &context->c_verbose,
                   "verbose", NULL, HIO_CONFIG_TYPE_UINT32, NULL, "Debug level", 0);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Set context verbosity to %d", context->c_verbose);

  hioi_config_add (context, &context->c_object, &context->c_droots,
                   "data_roots", NULL, HIO_CONFIG_TYPE_STRING, NULL,
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
                   "print_statistics", NULL, HIO_CONFIG_TYPE_BOOL, NULL, "Print statistics "
                   "to stdout when the context is closed (default: 0)", 0);

#if HIO_USE_DATAWARP
  context->c_dw_root = strdup ("auto");
  hioi_config_add (context, &context->c_object, &context->c_dw_root,
                   "datawarp_root", NULL, HIO_CONFIG_TYPE_STRING, NULL, "Mount path "
                   "for datawarp (burst-buffer) (default: auto-detect)", HIO_VAR_FLAG_DEFAULT);
  #ifdef HIO_DATAWARP_DEBUG_LOG
    context->c_dw_debug_mask = 0;
    context->c_dw_debug_installed = false;
    hioi_config_add (context, &context->c_object, &context->c_dw_debug_mask,
                     "datawarp_debug_mask", NULL, HIO_CONFIG_TYPE_UINT64, NULL, "Mask for "
                     "datawarp debug log messages (default: 0)", HIO_VAR_FLAG_DEFAULT);
  #endif
#endif

  /* mtti configuration */
  context->c_job_sys_int_rate = 138889;
  hioi_config_add (context, &context->c_object, &context->c_job_sys_int_rate,
                   "job_sys_int_rate", NULL, HIO_CONFIG_TYPE_UINT64, NULL, "Total "
                   "system interrupt rate in failures per 10^9 hours (default: "
                   "138889)", 0);

  context->c_job_node_int_rate = 5758;
  hioi_config_add (context, &context->c_object, &context->c_job_node_int_rate,
                   "job_node_int_rate", NULL, HIO_CONFIG_TYPE_UINT64, NULL, "Node "
                   "interrupt rate in failures per 10^9 hours (default: 5758)", 0);

  context->c_job_node_sw_rate = 0;
  hioi_config_add (context, &context->c_object, &context->c_job_node_sw_rate,
                   "job_node_sw_rate", NULL, HIO_CONFIG_TYPE_UINT64, NULL, "Software "
                   "failure rate in failures per 10^9 hours (default: 0)", 0);

  context->c_end_time = 0;
  hioi_config_add (context, &context->c_object, &context->c_end_time,
                   "end_time", NULL, HIO_CONFIG_TYPE_UINT64, NULL, "Job end time "
                   "in UNIX time format (default: 0)", 0);

  context->c_job_sigusr1_warning_time = 1800;
  hioi_config_add (context, &context->c_object, &context->c_job_sigusr1_warning_time,
                   "sigusr1_warning_time", NULL, HIO_CONFIG_TYPE_UINT64, NULL,
                   "Time remaining in job when SIGUSR1 is caught", 0);

  context->c_start_time = time (NULL);

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

  if (!context_name || '\0' == context_name[0] || strlen (context_name) >= HIO_CONTEXT_NAME_MAX) {
    hioi_err_push (HIO_ERR_BAD_PARAM, NULL, "context_name NULL, zero length, or too long");
    return HIO_ERR_BAD_PARAM;
  }

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

#if HIO_MPI_HAVE(1)
#if HIO_MPI_HAVE(3)
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
#endif /* HIO_MPI_HAVE(3) */

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

  if (!context_name || '\0' == context_name[0] ||  strlen (context_name) >= HIO_CONTEXT_NAME_MAX) {
    hioi_err_push (HIO_ERR_BAD_PARAM, NULL, "context_name NULL, zero length, or too long");
    return HIO_ERR_BAD_PARAM;
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
  hioi_context_msg_id(context, 1); 

  rc = hio_init_common (context, config_file, config_file_prefix, context_name);
  if (HIO_SUCCESS != rc) {
    hioi_object_release (&context->c_object);
    return rc;
  }

#if HIO_MPI_HAVE(3)
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
