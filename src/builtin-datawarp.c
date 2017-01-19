/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

/* datawarp is just a posix+ interface */
#include "builtin-posix_component.h"
#include "hio_internal.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <assert.h>

#include <sys/stat.h>

#include <datawarp.h>

#ifdef HIO_DATAWARP_DEBUG_LOG
  #include <datawarpLogger.h>
  #include <stdarg.h>
#endif

#define HIO_DATAWARP_MAX_KEEP 128

/**
 * builtin datawarp module
 *
 * This module is a thin wrapper around the posix module. In addition to the
 * posix module members it keeps track of the original open/fini functions
 * and the parallel file system path that we will be staging data to.
 */
typedef struct builtin_datawarp_module_t {
  builtin_posix_module_t posix_module;
  hio_module_dataset_open_fn_t posix_open;
  hio_module_dataset_unlink_fn_t posix_unlink;
  hio_module_fini_fn_t posix_fini;
  char *pfs_path;
} builtin_datawarp_module_t;

typedef struct builtin_datawarp_module_dataset_t {
  builtin_posix_module_dataset_t posix_dataset;
  char *pfs_path;
  int stage_mode;
  int keep_last;

  hio_dataset_close_fn_t posix_ds_close;
} builtin_datawarp_module_dataset_t;

typedef struct builtin_datawarp_dataset_backend_data_t {
  hio_dataset_backend_data_t base;

  struct builtin_datawarp_scheduled_id_t {
    int64_t id;
    int stage_mode;
  } resident_ids[HIO_DATAWARP_MAX_KEEP];
  int32_t keep_last;
  int next_index;
  int32_t num_resident;
} builtin_datawarp_dataset_backend_data_t;

enum {
  HIO_DATAWARP_STAGE_MODE_DISABLE = -2,
  HIO_DATAWARP_STAGE_MODE_AUTO = -1,
};

static hio_var_enum_t builtin_datawarp_stage_modes = {
  .count = 3,
  .values = (hio_var_enum_value_t []) {
    {.string_value = "disable", .value = HIO_DATAWARP_STAGE_MODE_DISABLE},
    {.string_value = "auto", .value = HIO_DATAWARP_STAGE_MODE_AUTO},
    {.string_value = "immediate", .value = DW_STAGE_IMMEDIATE},
    {.string_value = "end_of_job", .value = DW_STAGE_AT_JOB_END},
  },
};

#ifdef HIO_DATAWARP_DEBUG_LOG
static void datawarp_debug_logger(uint64_t flags, void *data, char *file,
                                  const char *func, unsigned int line,
                                  const char *fmt, ...) {
  hio_context_t context = (hio_context_t) data;
  va_list args;
  char buf[512];
  
  va_start(args, fmt);
  vsnprintf(buf, sizeof(buf), fmt, args); 
  va_end(args);
  hioi_log (context, HIO_VERBOSE_ERROR, "DataWarp debug: %s/%s/%u %s",
            file, func, line, buf);
  return;
}
#endif

static builtin_datawarp_dataset_backend_data_t *builtin_datawarp_get_dbd (hio_dataset_data_t *ds_data) {
  builtin_datawarp_dataset_backend_data_t *be_data;

  be_data = (builtin_datawarp_dataset_backend_data_t *) hioi_dbd_lookup_backend_data (ds_data, "datawarp");
  if (NULL == be_data) {
    be_data = (builtin_datawarp_dataset_backend_data_t *) hioi_dbd_alloc (ds_data, "datawarp",
                                                                          sizeof (*be_data));
    if (NULL == be_data) {
      return NULL;
    }

    for (int i = 0 ; i < HIO_DATAWARP_MAX_KEEP ; ++i) {
      be_data->resident_ids[i].id = -1;
      be_data->resident_ids[i].stage_mode = HIO_DATAWARP_STAGE_MODE_DISABLE;
    }

    be_data->keep_last = 1;
    be_data->next_index = 0;
    be_data->num_resident = 0;
  }

  return be_data;
}

static int builtin_datawarp_revoke_stage (hio_module_t *module, const char *ds_name, int64_t ds_id) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_datawarp_dataset_backend_data_t *ds_data;
  hio_context_t context = module->context;
  const char *data_root = module->data_root;
  char *dataset_path, *pfs_path;
  int rc;

  rc = asprintf (&dataset_path, "%s/%s.hio/%s/%llu", data_root, hioi_object_identifier (context),
                 ds_name, ds_id);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: revoking end-of-job stage for datawarp dataset %s::%lld. "
            "burst-buffer directory: %s", ds_name, ds_id, dataset_path);

  /* revoke the end of job stage for the previous dataset */
  rc = dw_stage_directory_out (dataset_path, NULL, DW_REVOKE_STAGE_AT_JOB_END);
  if (0 != rc) {
    hioi_err_push (HIO_ERROR, NULL, "builtin-datawarp/dataset_close: error revoking prior "
                   "end-of-job stage of dataset %s::%lld. errno: %d", ds_name, ds_id, errno);

    rc = access(dataset_path, R_OK | W_OK | X_OK);
    hioi_log (context, HIO_VERBOSE_DEBUG_XLOW, "access(%s, R_OK|W_OK|X_OK) returns %d errno: %d",
              dataset_path, rc, errno);

    free (dataset_path);
    return HIO_ERROR;
  }

  free (dataset_path);

  /* remove the last end-of-job dataset from the burst buffer */
  (void) datawarp_module->posix_unlink (module, ds_name, ds_id);

  /* remove created directories on pfs */
  rc = asprintf (&pfs_path, "%s/%s.hio/%s/%llu", datawarp_module->pfs_path, hioi_object_identifier (context),
                 ds_name, ds_id);
  if (0 > rc) {
    free (dataset_path);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* ignore failure. if the directory is not empty then we shouldn't be removing it */
  (void) rmdir (pfs_path);
  free (pfs_path);
  errno = 0;

  return HIO_SUCCESS;
}

static int builtin_datawarp_module_dataset_unlink (struct hio_module_t *module, const char *name, int64_t set_id) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_datawarp_dataset_backend_data_t *be_data;
  hio_context_t context = module->context;
  hio_dataset_data_t *ds_data = NULL;
  int stage_mode, rc, stage_index;

  (void) hioi_dataset_data_lookup (context, name, &ds_data);
  if (NULL == ds_data) {
    return HIO_ERR_NOT_FOUND;
  }

  be_data = builtin_datawarp_get_dbd (ds_data);
  if (NULL == be_data) {
    return HIO_ERR_NOT_FOUND;
  }

  for (stage_index = 0 ; stage_index < HIO_DATAWARP_MAX_KEEP ; ++stage_index) {
    if (be_data->resident_ids[stage_index].id == set_id) {
      break;
    }
  }

  if (HIO_DATAWARP_MAX_KEEP == stage_index) {
    return HIO_ERR_NOT_FOUND;
  }

  stage_mode = be_data->resident_ids[stage_index].stage_mode;

  if (DW_STAGE_IMMEDIATE == stage_mode) {
    int complete = 0, pending, deferred, failed;
    char *dw_path;

    rc = asprintf (&dw_path, "%s/%s.hio/%s/%llu", module->data_root, hioi_object_identifier (context), name, set_id);
    do {
      rc = dw_query_directory_stage (dw_path, &complete, &pending, &deferred, &failed);
      if (!complete) {
        const struct timespec interval = {.tv_sec = 0, .tv_nsec = 1000000};
        nanosleep (&interval, NULL);
      }
    } while (!complete);

    rc = datawarp_module->posix_unlink (module, name, set_id);
  } else if (HIO_DATAWARP_STAGE_MODE_DISABLE == stage_mode) {
    /* this dataset was detected at init time */
    rc = datawarp_module->posix_unlink (module, name, set_id);
  } else {
    rc = builtin_datawarp_revoke_stage (module, name, set_id);
  }

  be_data->resident_ids[stage_index].id = -1;
  be_data->resident_ids[stage_index].stage_mode = HIO_DATAWARP_STAGE_MODE_DISABLE;
  --be_data->num_resident;

  return rc;
}


static int builtin_datawarp_module_dataset_close (hio_dataset_t dataset);

static int builtin_datawarp_module_dataset_open (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_posix_module_t *posix_module = &datawarp_module->posix_module;
  builtin_datawarp_module_dataset_t *datawarp_dataset = (builtin_datawarp_module_dataset_t *) dataset;
  builtin_datawarp_dataset_backend_data_t *ds_data;
  hio_context_t context = module->context;
  builtin_posix_module_dataset_t *posix_dataset;
  int rc = HIO_SUCCESS;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "builtin-datawarp/dataset_open: opening dataset %s:%lu",
            hioi_object_identifier (dataset), (unsigned long) dataset->ds_id);

  #ifdef HIO_DATAWARP_DEBUG_LOG
    /* If datawarp debug log mask non-zero, install debug message handler */
    if (context->c_dw_debug_mask) {
      context->c_dw_debug_installed = true;
      INSTALL_LOGGING_CALLBACK(DATAWARP_LOG_KEY, datawarp_debug_logger);
      INSTALL_LOGGING_DATA(DATAWARP_LOG_KEY, context);
      INSTALL_LOGGING_MASK(DATAWARP_LOG_KEY, context->c_dw_debug_mask);
    } else {
      context->c_dw_debug_installed = false;
    }
  #endif

  /* open the posix dataset */
  rc = datawarp_module->posix_open (&posix_module->base, dataset);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  dataset->ds_module = module;

  if (0 == context->c_rank) {
    ds_data = builtin_datawarp_get_dbd (dataset->ds_data);
    if (NULL == ds_data) {
        return HIO_ERR_OUT_OF_RESOURCE;
    }
  }

  hioi_perf_add (context, &dataset->ds_object, &ds_data->num_resident, "resident_id_count",
                 HIO_CONFIG_TYPE_INT32, NULL, "Total number of resident dataset ids for this "
                 "dataset kind", 0);

  if (0 == context->c_rank && dataset->ds_flags & HIO_FLAG_WRITE) {
    /* default to auto mode */
    datawarp_dataset->stage_mode = -1;
    hioi_config_add (context, &dataset->ds_object, &datawarp_dataset->stage_mode,
                     "datawarp_stage_mode", HIO_CONFIG_TYPE_INT32, &builtin_datawarp_stage_modes,
                     "Datawarp stage mode to use with this dataset instance", 0);

    datawarp_dataset->keep_last = ds_data->keep_last;
    hioi_config_add (context, &dataset->ds_object, &datawarp_dataset->keep_last,
                     "datawarp_keep_last", HIO_CONFIG_TYPE_INT32, NULL,
                     "Keep last n dataset ids written to datawarp (default: 1)", 0);
    if (datawarp_dataset->keep_last > HIO_DATAWARP_MAX_KEEP || datawarp_dataset->keep_last <= 0) {
      hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "builtin-datawarp: invalid value specified for datawarp_keep_last: %d. value must be in "
                     "the range [1, %d]", datawarp_dataset->keep_last, HIO_DATAWARP_MAX_KEEP);
      datawarp_dataset->keep_last = ds_data->keep_last;
    }

    if (datawarp_dataset->keep_last != ds_data->keep_last) {
      if (datawarp_dataset->keep_last > ds_data->keep_last) {
        int id_index = ds_data->next_index;
        for (int i = 0 ; i < (datawarp_dataset->keep_last - ds_data->keep_last) ; ++i) {
          int64_t ds_id = ds_data->resident_ids[id_index].id;

          if (ds_id < 0) {
            continue;
          }

          hioi_log (context, HIO_VERBOSE_DEBUG_MED, "deleting dataset %s::%lu from datawarp", hioi_object_identifier (dataset),
                    ds_id);

          builtin_datawarp_module_dataset_unlink (module, hioi_object_identifier (dataset), ds_id);

          id_index = (id_index + 1) % ds_data->keep_last;
        }

        if (id_index != 0) {
          for (int i = 0 ; i < datawarp_dataset->keep_last ; ++i) {
            ds_data->resident_ids[i] = ds_data->resident_ids[id_index];
            id_index = (id_index + 1) % ds_data->keep_last;
          }
        }
      } else {
        for (int i = 0 ; i < datawarp_dataset->keep_last ; ++i) {
          if (-1 == ds_data->resident_ids[i].id) {
            ds_data->next_index = i;
            break;
          }
        }
      }
      ds_data->keep_last = datawarp_dataset->keep_last;
    }
  }

  /* override posix dataset functions (keep copies) */
  datawarp_dataset->posix_ds_close = dataset->ds_close;
  dataset->ds_close = builtin_datawarp_module_dataset_close;

  return HIO_SUCCESS;
}

static int builtin_datawarp_module_dataset_close (hio_dataset_t dataset) {
  builtin_datawarp_module_dataset_t *datawarp_dataset = (builtin_datawarp_module_dataset_t *) dataset;
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) dataset->ds_module;
  builtin_posix_module_t *posix_module = &datawarp_module->posix_module;
  builtin_posix_module_dataset_t *posix_dataset = &datawarp_dataset->posix_dataset;
  mode_t pfs_mode = posix_module->access_mode;
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  char *dataset_path = NULL;
  int rc, stage_mode;

  if (0 == context->c_rank && (dataset->ds_flags & HIO_FLAG_WRITE)) {
    /* keep a copy of the base path used by the posix module */
    dataset_path = strdup (posix_dataset->base_path);
    if (NULL == dataset_path) {
      /* out of memory */
      return HIO_ERR_OUT_OF_RESOURCE;
    }
  }

  rc = datawarp_dataset->posix_ds_close (dataset);
  if (HIO_SUCCESS != rc) {
    free (dataset_path);
    return rc;
  }

  if (datawarp_module->pfs_path && 0 == context->c_rank && (dataset->ds_flags & HIO_FLAG_WRITE) &&
      HIO_DATAWARP_STAGE_MODE_DISABLE != datawarp_dataset->stage_mode) {
    char *pfs_path;

    rc = asprintf (&pfs_path, "%s/%s.hio/%s/%llu", datawarp_module->pfs_path, hioi_object_identifier(context),
                   hioi_object_identifier(dataset), dataset->ds_id);
    if (0 > rc) {
      free (dataset_path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: staging datawarp dataset %s::%lld. "
              "burst-buffer directory: %s lustre dir: %s DW stage mode: %d",  hioi_object_identifier(dataset),
              dataset->ds_id, dataset_path, pfs_path, datawarp_dataset->stage_mode);

    if (HIO_DATAWARP_STAGE_MODE_AUTO == datawarp_dataset->stage_mode) {
      stage_mode = DW_STAGE_AT_JOB_END;
    } else {
      stage_mode = datawarp_dataset->stage_mode;
    }

    rc = hioi_mkpath (context, pfs_path, pfs_mode);
    if (HIO_SUCCESS != rc) {
      free (dataset_path);
      free (pfs_path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    rc = dw_stage_directory_out (dataset_path, pfs_path, stage_mode);
    free (pfs_path);
    free (dataset_path);
    if (0 != rc) {
      hioi_err_push (HIO_ERROR, &dataset->ds_object, "builtin-datawarp/dataset_close: error starting "
                    "data stage on dataset %s::%lld. DWRC: %d", hioi_object_identifier (dataset), dataset->ds_id, rc);
      
      hioi_err_push (HIO_ERROR, &dataset->ds_object, "dw_stage_directory_out(%s, %s, %d) "
                     "rc: %d errno: %d", dataset_path, pfs_path, stage_mode, rc, errno); 

      rc = access(dataset_path, R_OK | W_OK | X_OK);
      hioi_log (context, HIO_VERBOSE_DEBUG_XLOW, "access(%s, R_OK|W_OK|X_OK) returns %d errno: %d",
                dataset_path, rc, errno);    
      rc = access(pfs_path, R_OK | W_OK | X_OK);
      hioi_log (context, HIO_VERBOSE_DEBUG_XLOW, "access(%s, R_OK|W_OK|X_OK) returns %d errno: %d",
                pfs_path, rc, errno);    
 
      free (pfs_path);
      free (dataset_path);

      return HIO_ERROR;
    }

    if (DW_STAGE_AT_JOB_END == stage_mode) {
      builtin_datawarp_dataset_backend_data_t *be_data;
      int next_index;

      be_data = builtin_datawarp_get_dbd (dataset->ds_data);
      /* backend data should have created when this dataset was opened */
      assert (NULL != be_data);

      next_index = be_data->next_index;

      if (be_data->resident_ids[next_index].id >= 0) {
        builtin_datawarp_module_dataset_unlink (&posix_module->base, hioi_object_identifier (&dataset->ds_object),
                                                be_data->resident_ids[next_index].id);
      }
      ++be_data->num_resident;

      be_data->next_index = (next_index + 1) & be_data->keep_last;
    }
  }

  return rc;
}

static int builtin_datawarp_module_fini (struct hio_module_t *module) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  hio_context_t context = module->context;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Finalizing datawarp filesystem module for data root %s",
	    module->data_root);

  free (datawarp_module->pfs_path);

  if (datawarp_module->posix_fini) {
    /* posix fini will free the module so call this last */
    datawarp_module->posix_fini (&datawarp_module->posix_module.base);
  } else {
    free (module->data_root);
    free (module);
  }

  #ifdef HIO_DATAWARP_DEBUG_LOG
    /* If datawarp debug logger previously installed, remove it */
    if (context->c_dw_debug_installed) {
      INSTALL_LOGGING_MASK(DATAWARP_LOG_KEY, 0);
      INSTALL_LOGGING_CALLBACK(DATAWARP_LOG_KEY, NULL);
      context->c_dw_debug_installed = false;
    }
  #endif
  return HIO_SUCCESS;
}

static int builtin_datawarp_component_init (hio_context_t context) {
  /* nothing to do */
  return HIO_SUCCESS;
}

static int builtin_datawarp_component_fini (void) {
  /* nothing to do */
  return HIO_SUCCESS;
}

/**
 * @brief Scan the module's data root looking for existing datasets. If any are found
 *        add them to the dataset's backend data. This will allow us to include these
 *        datasets as part of the component's space management.
 */
static int builtin_datawarp_scan_datasets (builtin_datawarp_module_t *datawarp_module) {
  hio_context_t context = datawarp_module->posix_module.base.context;
  builtin_datawarp_dataset_backend_data_t *be_data;
  struct dirent context_entry, *tmp = NULL;
  hio_dataset_header_t *headers = NULL;
  hio_dataset_data_t *ds_data;
  char *context_path;
  DIR *context_dir;
  int rc, count = 0;

  if (0 != context->c_rank) {
    /* nothing to do */
    return HIO_SUCCESS;
  }

  rc = asprintf (&context_path, "%s/%s.hio", datawarp_module->posix_module.base.data_root,
                 hioi_object_identifier (context));
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  context_dir = opendir (context_path);
  if (NULL == context_dir) {
    /* no context directory == no datasets */
    free (context_path);
    return HIO_SUCCESS;
  }

  while (!readdir_r (context_dir, &context_entry, &tmp) && NULL != tmp) {
    if ('.' == context_entry.d_name[0]) {
      continue;
    }

    rc = hioi_dataset_data_lookup (context, context_entry.d_name, &ds_data);
    if (HIO_SUCCESS != rc) {
      /* should not happen */
      free (context_path);
      return rc;
    }

    be_data = builtin_datawarp_get_dbd (ds_data);
    if (NULL == be_data) {
      free (context_path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    /* use the internal version of the dataset list function since only the 0th rank
     * is processing the list */
    rc = builtin_posix_module_dataset_list_internal (&datawarp_module->posix_module.base, context_entry.d_name,
                                                     &headers, &count);
    if (HIO_SUCCESS != rc) {
      free (context_path);
      return rc;
    }

    if (0 == count) {
      /* no datasets in this directory */
      continue;
    }

    if (HIO_DATAWARP_MAX_KEEP < count) {
      hioi_err_push(HIO_ERR_BAD_PARAM, NULL, "builtin-datawarp: too many datasets in data root. %d > %d",
                    count, HIO_DATAWARP_MAX_KEEP);
      count = HIO_DATAWARP_MAX_KEEP;
    }

    hioi_dataset_headers_sort (headers, count, HIO_DATASET_ID_NEWEST);

    for (int i = 0 ; i < count ; ++i) {
      int complete = 0, pending = 0, deferred = 0, failed = 0;
      char *ds_path;

      be_data->resident_ids[i].id = headers[i].ds_id;

      rc = asprintf (&ds_path, "%s/%s/%ld", context_path, context_entry.d_name, headers[i].ds_id);
      if (0 > rc) {
        free (headers);
        free (context_path);
        return HIO_ERR_OUT_OF_RESOURCE;
      }

      rc = dw_query_directory_stage (ds_path, &complete, &pending, &deferred, &failed);
      free (ds_path);

      if (0 == rc) {
        /* end of job stages will have all files in the deferred stage. anything else is
         * an immediate stage */
        be_data->resident_ids[i].stage_mode = (deferred > 0) ? DW_STAGE_AT_JOB_END : DW_STAGE_IMMEDIATE;
      } else {
        /* no stage active. we do not need to cancel the stage on this dataset */
        be_data->resident_ids[i].stage_mode = HIO_DATAWARP_STAGE_MODE_DISABLE;
      }

      hioi_log (context, HIO_VERBOSE_DEBUG_MED, "builtin-datawarp: found resident dataset %s::%lu with status %d. stage mode %d",
                context_entry.d_name, headers[i].ds_id, headers[i].ds_status, be_data->resident_ids[i].stage_mode);
    }

    be_data->next_index = 0;
    be_data->keep_last = count;
    be_data->num_resident = count;

    free (headers);
    headers = NULL;
    count = 0;
  }

  closedir (context_dir);

  return HIO_SUCCESS;
}

static int builtin_datawarp_component_query (hio_context_t context, const char *data_root,
                                             const char *next_data_root, hio_module_t **module) {
  builtin_datawarp_module_t *new_module;
  hio_module_t *posix_module;
  char *posix_data_root;
  int rc;

  if (strncasecmp("datawarp", data_root, 8) && strncasecmp("dw", data_root, 2)) {
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/query: module datawarp does not match for data "
              "root %s", data_root);
    return HIO_ERR_NOT_AVAILABLE;
  }

  if (NULL != next_data_root && (strncasecmp (next_data_root, "posix:", 6) || access (next_data_root + 6, F_OK))) {
    hioi_log (context, HIO_VERBOSE_ERROR, "builtin-datawarp/query: attempting to use datawarp but PFS stage out "
              "path %s is not accessible", next_data_root);
    return HIO_ERR_NOT_AVAILABLE;
  }

  if (NULL == next_data_root) {
    hioi_log (context, HIO_VERBOSE_WARN, "builtin-datawarp/query: using datawarp without file staging support%s", "");
  }

  if (NULL == context->c_dw_root) {
    hioi_log (context, HIO_VERBOSE_ERROR, "builtin-datawarp/query: attempted to use datawarp without specifying "
              "the mount point of the datawarp file system%s", "");
    return HIO_ERR_NOT_AVAILABLE;
  }

  /* get a builtin-posix module for interfacing with the burst buffer file system */
  if (0 == strcmp (context->c_dw_root, "auto")) {
    char *datawarp_tmp = getenv ("DW_JOB_STRIPED");

    if (NULL == datawarp_tmp) {
      hioi_log (context, HIO_VERBOSE_WARN, "builtin-datawarp/query: neither DW_JOB_STRIPED nor HIO_datawarp_root "
                "set. disabling datawarp support%s", "");
      return HIO_ERR_NOT_AVAILABLE;
    }

    posix_data_root = strdup (datawarp_tmp);
    if (NULL == posix_data_root) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }
  } else {
    rc = asprintf (&posix_data_root, "%s", context->c_dw_root);

    if (0 > rc) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/query: using datawarp root: %s, pfs backing store: %s",
            posix_data_root, next_data_root);

  rc = builtin_posix_component.query (context, posix_data_root, NULL, &posix_module);
  free (posix_data_root);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  /* allocate a new datawarp I/O module */
  new_module = calloc (1, sizeof (builtin_datawarp_module_t));
  if (NULL == new_module) {
    posix_module->fini (posix_module);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  memcpy (&new_module->posix_module, posix_module, sizeof (new_module->posix_module));

  new_module->posix_open = posix_module->dataset_open;
  new_module->posix_unlink = posix_module->dataset_unlink;
  new_module->posix_fini = posix_module->fini;

  new_module->posix_module.base.dataset_open = builtin_datawarp_module_dataset_open;
  new_module->posix_module.base.dataset_unlink = builtin_datawarp_module_dataset_unlink;
  new_module->posix_module.base.ds_object_size = sizeof (builtin_datawarp_module_dataset_t);
  new_module->posix_module.base.fini = builtin_datawarp_module_fini;

  free (posix_module);

  if (next_data_root) {
    new_module->pfs_path = strdup (next_data_root + 6);
    if (NULL == new_module->pfs_path) {
      new_module->posix_module.base.fini ((hio_module_t *) new_module);
      return HIO_ERR_OUT_OF_RESOURCE;
    }
 }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/query: created datawarp filesystem module "
            "for data root %s", new_module->posix_module.base.data_root);

  *module = &new_module->posix_module.base;

  /* scan for existing dataset(s) for space management */
  (void) builtin_datawarp_scan_datasets (new_module);

  return HIO_SUCCESS;
}

hio_component_t builtin_datawarp_component = {
  .init = builtin_datawarp_component_init,
  .fini = builtin_datawarp_component_fini,

  .query = builtin_datawarp_component_query,
  .flags = 0,
  .priority = 10,
  .version = HIO_MODULE_VERSION_1,
};
