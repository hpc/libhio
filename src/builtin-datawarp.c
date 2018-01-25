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
  hio_module_compare_fn_t posix_compare;
  char *pfs_path;
} builtin_datawarp_module_t;

typedef struct builtin_datawarp_module_dataset_t {
  builtin_posix_module_dataset_t posix_dataset;
  char *pfs_path;
  int stage_mode;
  int keep_last;

  hio_dataset_close_fn_t posix_ds_close;
  int64_t stage_out_stripe_size;
  int64_t stage_out_stripe_count;
} builtin_datawarp_module_dataset_t;

typedef struct builtin_datawarp_resident_id_t {
  hio_list_t dwrid_list;
  /** dataset identifier */
  int64_t dwrid_id;
  /** active stage mode */
  int dwrid_stage_mode;
} builtin_datawarp_resident_id_t;

typedef struct builtin_datawarp_dataset_backend_data_t {
  hio_dataset_backend_data_t base;

  /** datset ids resident on the datawarp mount */
  hio_list_t resident_ids;
  /** maximum number of dataset ids to keep resident on datawarp */
  int32_t keep_last;
  /** current number of resident datasets. this is equal to the
   * length of the resident_ids list. this field exists to
   * expose the length as a performance variable. */
  int32_t num_resident;
} builtin_datawarp_dataset_backend_data_t;

enum {
  /** disable/no stage */
  HIO_DATAWARP_STAGE_MODE_DISABLE = -2,
  /** automatically choose the stage mode */
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

static void builtin_datawarp_add_resident (builtin_datawarp_dataset_backend_data_t *be_data, int64_t id, int stage_mode, bool append) {
  builtin_datawarp_resident_id_t *rid = calloc (1, sizeof (*rid));

  if (NULL == rid) {
    return;
  }

  hioi_list_init (rid->dwrid_list);
  rid->dwrid_id = id;
  rid->dwrid_stage_mode = stage_mode;

  if (append) {
    hioi_list_append (rid, be_data->resident_ids, dwrid_list);
  } else {
    hioi_list_prepend (rid, be_data->resident_ids, dwrid_list);
  }

  ++be_data->num_resident;
}

static void builtin_datawarp_bed_release (hio_dataset_backend_data_t *data) {
  builtin_datawarp_dataset_backend_data_t *be_data = (builtin_datawarp_dataset_backend_data_t *) data;
  builtin_datawarp_resident_id_t *resident_id, *next;

  hioi_list_foreach_safe (resident_id, next, be_data->resident_ids, builtin_datawarp_resident_id_t, dwrid_list) {
    hioi_list_remove(resident_id, dwrid_list);
    free (resident_id);
  }
}

static builtin_datawarp_dataset_backend_data_t *builtin_datawarp_get_dbd (hio_dataset_data_t *ds_data) {
  builtin_datawarp_dataset_backend_data_t *be_data;

  be_data = (builtin_datawarp_dataset_backend_data_t *) hioi_dbd_lookup_backend_data (ds_data, "datawarp");
  if (NULL == be_data) {
    be_data = (builtin_datawarp_dataset_backend_data_t *) hioi_dbd_alloc (ds_data, "datawarp",
                                                                          sizeof (*be_data));
    if (NULL == be_data) {
      return NULL;
    }

    hioi_list_init(be_data->resident_ids);

    be_data->keep_last = 1;
    be_data->num_resident = 0;
    be_data->base.dbd_release_fn = builtin_datawarp_bed_release;
  }

  return be_data;
}

static int builtin_datawarp_revoke_stage (hio_module_t *module, hio_dataset_header_t *header) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_datawarp_dataset_backend_data_t *ds_data;
  hio_context_t context = module->context;
  char *pfs_path, *last_slash, *tmp = header->ds_path;
  int rc;

  last_slash = strrchr (header->ds_path, '/');
  if (NULL == last_slash) {
    /* should not happen */
    return HIO_ERROR;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: revoking end-of-job stage for datawarp "
            "dataset %s::%lld. burst-buffer directory: %s, pfs directory: %s/%s", header->ds_name, header->ds_id,
            header->ds_path, datawarp_module->pfs_path, last_slash + 1);

  /* revoke the end of job stage for the previous dataset */
  rc = dw_stage_directory_out (header->ds_path, NULL, DW_REVOKE_STAGE_AT_JOB_END);
  if (0 != rc) {
    hioi_err_push (HIO_ERROR, NULL, "builtin-datawarp/dataset_close: error revoking prior "
                   "end-of-job stage of dataset %s::%lld. errno: %d", header->ds_name, header->ds_id, errno);

    rc = access (header->ds_path, R_OK | W_OK | X_OK);
    hioi_log (context, HIO_VERBOSE_DEBUG_XLOW, "access(%s, R_OK|W_OK|X_OK) returns %d errno: %d",
              header->ds_path, rc, errno);

    return HIO_ERROR;
  }

  /* remove the last end-of-job dataset from the burst buffer */
  (void) builtin_posix_unlink_dir (context, header);

  rc = asprintf (&pfs_path, "%s/%s", datawarp_module->pfs_path, last_slash + 1);
  if (0 >= rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  header->ds_path = pfs_path;

  /* ignore error when removing a stage-out directory from the parallel file system */
  (void) builtin_posix_unlink_dir (module->context, header);
  header->ds_path = tmp;

  return HIO_SUCCESS;
}

static int buildin_datawarp_module_dataset_unlink_dir (struct hio_module_t *module, hio_dataset_header_t *header, int stage_mode) {
  int complete = 0, pending, deferred, failed, rc;

  switch (stage_mode) {
  case DW_STAGE_IMMEDIATE:
      do {
        rc = dw_query_directory_stage (header->ds_path, &complete, &pending, &deferred, &failed);
        if (0 != rc) {
          hioi_err_push (HIO_ERROR, &module->context->c_object, "error querying directory stage. got %d\n", rc);
          return HIO_ERROR;
        }

        if (!complete) {
          nanosleep (&(struct timespec) {.tv_sec = 0, .tv_nsec = 1000000}, NULL);
        }
      } while (!complete);

      /* fall through -- silence static analysis warnings */
  case HIO_DATAWARP_STAGE_MODE_DISABLE:
    return builtin_posix_unlink_dir (module->context, header);
  default:
    return builtin_datawarp_revoke_stage (module, header);
  }
}

static int builtin_datawarp_module_dataset_unlink (struct hio_module_t *module, const char *name, int64_t set_id) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_datawarp_dataset_backend_data_t *be_data;
  builtin_datawarp_resident_id_t *rid = NULL, *next;
  hio_context_t context = module->context;
  hio_dataset_data_t *ds_data = NULL;
  hio_dataset_list_t *list;
  int stage_mode, rc;
  bool found = false;

  if (module->context->c_rank) {
    return HIO_ERR_NOT_AVAILABLE;
  }

  list = hioi_dataset_list_alloc ();
  if (NULL == list) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  do {
    rc = builtin_posix_module_dataset_list_internal (&datawarp_module->posix_module.base, name, NULL, 0, list);
    if (HIO_SUCCESS != rc) {
      break;
    }

    rc = HIO_ERR_NOT_FOUND;

    (void) hioi_dataset_data_lookup (context, name, &ds_data);
    if (NULL == ds_data) {
      break;
    }

    be_data = builtin_datawarp_get_dbd (ds_data);
    if (NULL == be_data) {
      break;
    }

    hioi_list_foreach_safe (rid, next, be_data->resident_ids, builtin_datawarp_resident_id_t, dwrid_list) {
      if (set_id == rid->dwrid_id) {
        hioi_list_remove (rid, dwrid_list);
        found = true;
        break;
      }
    }

    if (!found) {
      rid = NULL;
      break;
    }

    for (size_t i = 0 ; i < list->header_count ; ++i) {
      if (list->headers[i].ds_id == set_id) {
        rc = buildin_datawarp_module_dataset_unlink_dir (module, list->headers + i, rid->dwrid_stage_mode);
      }
    }

    free (rid);
    --be_data->num_resident;
  } while (0);

  hioi_dataset_list_release (list);

  return rc;
}

static void builtin_datawarp_cleanup (hio_dataset_t dataset, builtin_datawarp_dataset_backend_data_t *be_data) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  builtin_datawarp_resident_id_t *resident_id, *next;
  hio_module_t *module = dataset->ds_module;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "builtin_datawarp_cleanup: there are %d resident dataset ids. keeping at most %d",
            be_data->num_resident, be_data->keep_last);

  hioi_list_foreach_safe (resident_id, next, be_data->resident_ids, builtin_datawarp_resident_id_t, dwrid_list) {
    int64_t ds_id = resident_id->dwrid_id;

    if (be_data->num_resident <= be_data->keep_last) {
      break;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_MED, "deleting dataset %s::%"PRIi64" from datawarp", hioi_object_identifier (dataset),
              ds_id);

    builtin_datawarp_module_dataset_unlink (module, hioi_object_identifier (dataset), ds_id);
  }
}

static void builtin_datawarp_keep_last_set_cb (hio_object_t object, struct hio_var_t *variable) {
  builtin_datawarp_module_dataset_t *datawarp_dataset = (builtin_datawarp_module_dataset_t *) object;
  hio_context_t context = hioi_object_context (object);
  builtin_datawarp_dataset_backend_data_t *be_data;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "user set keep_last variable to %d", datawarp_dataset->keep_last);

  be_data = (builtin_datawarp_dataset_backend_data_t *) hioi_dbd_lookup_backend_data (datawarp_dataset->posix_dataset.base.ds_data,
                                                                                      "datawarp");
  if (NULL != be_data) {
    be_data->keep_last = datawarp_dataset->keep_last;
    builtin_datawarp_cleanup (&datawarp_dataset->posix_dataset.base, be_data);
  }
}


static int builtin_datawarp_module_dataset_close (hio_dataset_t dataset);

static int builtin_datawarp_module_dataset_open (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_posix_module_t *posix_module = &datawarp_module->posix_module;
  builtin_datawarp_module_dataset_t *datawarp_dataset = (builtin_datawarp_module_dataset_t *) dataset;
  builtin_datawarp_resident_id_t *resident_id, *next;
  builtin_datawarp_dataset_backend_data_t *be_data;
  hio_context_t context = module->context;
  builtin_posix_module_dataset_t *posix_dataset;
  int rc = HIO_SUCCESS, num_resident;

  if (0 != strcmp (module->data_root, dataset->ds_data_root)) {
    return HIO_ERR_NOT_FOUND;
  }

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
    be_data = builtin_datawarp_get_dbd (dataset->ds_data);
    if (NULL == be_data) {
        return HIO_ERR_OUT_OF_RESOURCE;
    }

    if (dataset->ds_flags & HIO_FLAG_WRITE) {
      hioi_perf_add (context, &dataset->ds_object, &be_data->num_resident, "resident_id_count",
                     HIO_CONFIG_TYPE_INT32, NULL, "Total number of resident dataset ids for this "
                     "dataset kind", 0);

      /* default to auto mode */
      datawarp_dataset->stage_mode = HIO_DATAWARP_STAGE_MODE_AUTO;
      hioi_config_add (context, &dataset->ds_object, &datawarp_dataset->stage_mode,
                       "datawarp_stage_mode", NULL, HIO_CONFIG_TYPE_INT32, &builtin_datawarp_stage_modes,
                       "Datawarp stage mode to use with this dataset instance", 0);

      hioi_config_add (context, &dataset->ds_object, &datawarp_dataset->pfs_path,
                       "datawarp_stage_out_destination", NULL, HIO_CONFIG_TYPE_STRING, NULL,
                       "Target directory for stage-out operations", 0);

      datawarp_dataset->keep_last = be_data->keep_last;
      hioi_config_add (context, &dataset->ds_object, &datawarp_dataset->keep_last,
                       "datawarp_keep_last", builtin_datawarp_keep_last_set_cb, HIO_CONFIG_TYPE_INT32, NULL,
                       "Keep last n dataset ids written to datawarp (default: 1)", 0);
      if (datawarp_dataset->keep_last >= HIO_DATAWARP_MAX_KEEP || datawarp_dataset->keep_last <= 0) {
        hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "builtin-datawarp: invalid value specified for datawarp_keep_last: %d. value must be in "
                       "the range [1, %d]", datawarp_dataset->keep_last, HIO_DATAWARP_MAX_KEEP);
        datawarp_dataset->keep_last = be_data->keep_last;
      } else {
        be_data->keep_last = datawarp_dataset->keep_last;
      }

      datawarp_dataset->stage_out_stripe_count = -1;
      hioi_config_add (context, &dataset->ds_object, &datawarp_dataset->stage_out_stripe_count,
                       "datawarp_stage_out_stripe_count", builtin_datawarp_keep_last_set_cb, HIO_CONFIG_TYPE_INT64, NULL,
                       "Stripe count of data directory when staged out to lustre. Only applies to datasets with unique "
                       "address spaces.(default: -1 (auto)) ", 0);

      datawarp_dataset->stage_out_stripe_size = -1;
      hioi_config_add (context, &dataset->ds_object, &datawarp_dataset->stage_out_stripe_size,
                       "datawarp_stage_out_stripe_count", builtin_datawarp_keep_last_set_cb, HIO_CONFIG_TYPE_INT64, NULL,
                       "Stripe size of data directory when staged out to lustre. Only applies to datasets with unique "
                       "address spaces. (default: -1 (auto))", 0);

      builtin_datawarp_cleanup (dataset, be_data);
    }
  }

  /* override posix dataset functions (keep copies) */
  datawarp_dataset->posix_ds_close = dataset->ds_close;
  dataset->ds_close = builtin_datawarp_module_dataset_close;

  return HIO_SUCCESS;
}

static inline int builtin_datawarp_set_output_striping (builtin_datawarp_module_dataset_t *datawarp_dataset,
                                                        const char *dw_path, const char *pfs_path, mode_t pfs_mode)
{
  hio_context_t context = hioi_object_context (&datawarp_dataset->posix_dataset.base.ds_object);
  size_t total_size = 0, count = 0, average_size;
  struct dirent dir_entry, *result;
  hio_fs_attr_t fs_attr;
  struct stat statinfo;
  char *data_path;
  DIR *dh;
  int rc;

  if (HIO_SET_ELEMENT_UNIQUE == datawarp_dataset->posix_dataset.base.ds_mode) {
    /* nothing to do */
    return HIO_SUCCESS;
  }

  /* find out the default stripe information */
  rc = hioi_fs_query_single (context, pfs_path, &fs_attr);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = asprintf (&data_path, "%s/data", dw_path);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* see how large the typical file is. we will set the default stripe size
   * and count based on this information. */
  dh = opendir (data_path);
  free (data_path);
  if (NULL == dh) {
    return hioi_err_errno (errno);
  }

  while (0 == readdir_r (dh, &dir_entry, &result)) {
    if (NULL == result) {
      break;
    }
    if ('.' == result->d_name[0]) {
      continue;
    }

    rc = fstatat (dirfd (dh), result->d_name, &statinfo, 0);
    if (0 == rc && !S_ISDIR(statinfo.st_mode)) {
      total_size += statinfo.st_size;
      count++;
    }
  }

  closedir (dh);

  /* this should never happen. silence static analysis error */
  if (0 == count) {
    assert (0);

    return HIO_ERROR;
  }

  average_size = total_size / count;

  if (-1 != datawarp_dataset->stage_out_stripe_count) {
    fs_attr.fs_scount = datawarp_dataset->stage_out_stripe_count;
  } else {
    /* by default use 90% of the available IO resources */
    fs_attr.fs_scount = (fs_attr.fs_smax_count * 9) / 10;
  }

  if (-1 != datawarp_dataset->stage_out_stripe_size) {
    fs_attr.fs_ssize =  datawarp_dataset->stage_out_stripe_size;
  } else if (average_size < 0x1000000ul) {
    fs_attr.fs_ssize = 0x100000ul;
  } else {
    fs_attr.fs_ssize = 0x1000000ul;
  }

  /* ensure we don't exceed the maximum values */
  if (fs_attr.fs_ssize > fs_attr.fs_smax_size) {
    hioi_log (context, HIO_VERBOSE_WARN, "requested stripe size exceeeds the maximum: requested = %"
              PRIu64 ", max = %" PRIu64, fs_attr.fs_ssize, fs_attr.fs_smax_size);
    fs_attr.fs_ssize = fs_attr.fs_smax_size;
  }

  if (fs_attr.fs_scount > fs_attr.fs_smax_count) {
    hioi_log (context, HIO_VERBOSE_WARN, "requested stripe count exceeeds the maximum: requested = %u"
              ", max = %u", fs_attr.fs_scount, fs_attr.fs_smax_count);
    fs_attr.fs_scount = fs_attr.fs_smax_count;
  }

  rc = asprintf (&data_path, "%s/data", pfs_path);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* need to make sure the directory is created before we can set striping parameters */
  if (0 != access (data_path, R_OK)) {
    rc = hioi_mkpath (context, data_path, pfs_mode);
    if (HIO_SUCCESS != rc) {
      free (data_path);
      return rc;
    }
  }

  rc = hioi_fs_set_stripe (data_path, &fs_attr);
  if (HIO_SUCCESS != rc) {
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "could not set file system striping on stage out target %s", data_path);
  }
  free (data_path);

  return rc;
}

static int builtin_datawarp_module_dataset_close (hio_dataset_t dataset) {
  builtin_datawarp_module_dataset_t *datawarp_dataset = (builtin_datawarp_module_dataset_t *) dataset;
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) dataset->ds_module;
  builtin_posix_module_t *posix_module = &datawarp_module->posix_module;
  builtin_posix_module_dataset_t *posix_dataset = &datawarp_dataset->posix_dataset;
  builtin_datawarp_dataset_backend_data_t *be_data;
  mode_t pfs_mode = posix_module->access_mode;
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  char *dataset_path = NULL, *pfs_path = NULL;
  int rc, stage_mode, num_resident;

  if (0 == context->c_rank && (dataset->ds_flags & HIO_FLAG_WRITE)) {
    /* keep a copy of the base path used by the posix module */
    dataset_path = strdup (posix_dataset->base_path);
    if (NULL == dataset_path) {
      /* out of memory */
      return HIO_ERR_OUT_OF_RESOURCE;
    }
  }


  if (datawarp_module->pfs_path && 0 == context->c_rank && (dataset->ds_flags & HIO_FLAG_WRITE) &&
      HIO_DATAWARP_STAGE_MODE_DISABLE != datawarp_dataset->stage_mode) {
    rc = builtin_posix_dataset_path_data_root (posix_dataset, &pfs_path, datawarp_module->pfs_path);
    assert (HIO_SUCCESS == rc);
  }

  /* close the dataset with the underlying posix module */
  rc = datawarp_dataset->posix_ds_close (dataset);
  if (HIO_SUCCESS != rc) {
    free (dataset_path);
    free (pfs_path);
    return rc;
  }

  if (datawarp_module->pfs_path && 0 == context->c_rank && (dataset->ds_flags & HIO_FLAG_WRITE) &&
      HIO_DATAWARP_STAGE_MODE_DISABLE != datawarp_dataset->stage_mode) {
    /* data write is complete. start staging the dataset out to the parallel file system */
    if (HIO_DATAWARP_STAGE_MODE_AUTO == datawarp_dataset->stage_mode) {
      stage_mode = DW_STAGE_AT_JOB_END;
    } else {
      stage_mode = datawarp_dataset->stage_mode;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: staging datawarp dataset %s::%lld. "
              "burst-buffer directory: %s lustre dir: %s DW stage mode: %d",  hioi_object_identifier(dataset),
              dataset->ds_id, dataset_path, pfs_path, stage_mode);

    rc = hioi_mkpath (context, pfs_path, pfs_mode);
    if (HIO_SUCCESS != rc) {
      free (dataset_path);
      free (pfs_path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    /* set the striping on the output directory based on what we have in datawarp */
    rc = builtin_datawarp_set_output_striping (datawarp_dataset, dataset_path, pfs_path, pfs_mode);
    if (HIO_SUCCESS != rc) {
      hioi_log (context, HIO_VERBOSE_WARN, "error setting striping on stage-out directory. code = %d", rc);
    }

    rc = dw_stage_directory_out (dataset_path, pfs_path, stage_mode);
    if (0 != rc) {
      hioi_err_push (HIO_ERROR, &dataset->ds_object, "builtin-datawarp/dataset_close: error starting "
                    "data stage on dataset %s::%lld. DWRC: %d", hioi_object_identifier (dataset), dataset->ds_id, rc);
      
      hioi_err_push (HIO_ERROR, &dataset->ds_object, "dw_stage_directory_out(%s, %s, %d) "
                     "rc: %d errno: %d", dataset_path, pfs_path, stage_mode, rc, errno); 

      errno = 0;
      rc = access(dataset_path, R_OK | W_OK | X_OK);
      hioi_log (context, HIO_VERBOSE_DEBUG_XLOW, "access(%s, R_OK|W_OK|X_OK) returns %d errno: %d",
                dataset_path, rc, errno);    
      errno = 0;
      rc = access(pfs_path, R_OK | W_OK | X_OK);
      hioi_log (context, HIO_VERBOSE_DEBUG_XLOW, "access(%s, R_OK|W_OK|X_OK) returns %d errno: %d",
                pfs_path, rc, errno);    
 
      free (pfs_path);
      free (dataset_path);

      return HIO_ERROR;
    }
    free (pfs_path);

    be_data = builtin_datawarp_get_dbd (dataset->ds_data);
    /* backend data should have created when this dataset was opened */
    assert (NULL != be_data);

    builtin_datawarp_add_resident (be_data, dataset->ds_id, stage_mode, true);

    num_resident = be_data->num_resident;

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: resident datasets: %d, keep: %d", num_resident,
              be_data->keep_last);

    if (num_resident > be_data->keep_last) {
      builtin_datawarp_resident_id_t *rid = (builtin_datawarp_resident_id_t *) be_data->resident_ids.next;

      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: removing resident dataset with id %"PRIi64,
                rid->dwrid_id);

      rc = builtin_datawarp_module_dataset_unlink (&posix_module->base, hioi_object_identifier (&dataset->ds_object),
                                                   rid->dwrid_id);
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: builtin_datawarp_module_dataset_unlink returned %d. "
                "resident ids %d", rc, num_resident - 1);
    }
  }

  free (dataset_path);

  return rc;
}

static int builtin_datawarp_module_fini (struct hio_module_t *module) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  hio_context_t context = module->context;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Finalizing datawarp filesystem module for data root %s",
	    module->data_root);

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
  hio_dataset_list_t *list = NULL;
  hio_dataset_data_t *ds_data;
  char *context_path;
  int rc;

  if (0 != context->c_rank) {
    /* nothing to do */
    return HIO_SUCCESS;
  }

  list = hioi_dataset_list_alloc ();
  if (NULL == list) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* use the internal version of the dataset list function since only the 0th rank
   * is processing the list */
  rc = builtin_posix_module_dataset_list_internal (&datawarp_module->posix_module.base, NULL, NULL, 0, list);
  if (HIO_SUCCESS != rc || 0 == list->header_count) {
    hioi_dataset_list_release (list);
    return rc;
  }

  hioi_dataset_list_sort (list, HIO_DATASET_ID_NEWEST);

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "builtin-datawarp: found %lu resident datasets",
            (unsigned long) list->header_count);

  for (int i = 0 ; i < list->header_count ; ++i) {
    int complete = 0, pending = 0, deferred = 0, failed = 0, stage_mode = HIO_DATAWARP_STAGE_MODE_DISABLE;
    hio_dataset_header_t *header = list->headers + i;

    rc = hioi_dataset_data_lookup (context, header->ds_name, &ds_data);
    if (HIO_SUCCESS != rc) {
      /* should not happen */
      break;
    }

    be_data = builtin_datawarp_get_dbd (ds_data);
    if (NULL == be_data) {
      rc = HIO_ERR_OUT_OF_RESOURCE;
      break;
    }

    rc = dw_query_directory_stage (header->ds_path, &complete, &pending, &deferred, &failed);
    if (0 == rc) {
      /* end of job stages will have all files in the deferred stage. anything else will be
       * treated as an immediate stage */
      stage_mode = (deferred > 0) ? DW_STAGE_AT_JOB_END : DW_STAGE_IMMEDIATE;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_MED, "builtin-datawarp: found resident dataset %s::%" PRIi64 " with status "
              "%d. stage mode %d", header->ds_name, header->ds_id, header->ds_status, stage_mode);

    builtin_datawarp_add_resident (be_data, header->ds_id, stage_mode, true);
    rc = HIO_SUCCESS;
  }

  hioi_dataset_list_release (list);

  return rc;
}

bool builtin_datawarp_module_compare (hio_module_t *module, const char *data_root) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  if (0 == strncasecmp("datawarp", data_root, 8) || 0 == strncasecmp("dw", data_root, 2)) {
    return true;
  }

  return datawarp_module->posix_compare (module, data_root);
}

static int builtin_datawarp_component_query (hio_context_t context, const char *data_root,
                                             const char *next_data_root, hio_module_t **module) {
  const char *dw_root;
  builtin_datawarp_module_t *new_module;
  hio_module_t *posix_module;
  int rc;

  if (0 == strcmp (context->c_dw_root, "auto")) {
    dw_root = getenv ("DW_JOB_STRIPED");
    if (NULL == dw_root) {
      hioi_log (context, HIO_VERBOSE_WARN, "builtin-datawarp/query: neither DW_JOB_STRIPED nor HIO_datawarp_root "
                "set. disabling datawarp support%s", "");
      return HIO_ERR_NOT_AVAILABLE;
    }
  } else {
    dw_root = context->c_dw_root;
  }

  if (NULL == dw_root || (strncasecmp("datawarp", data_root, 8) && strncasecmp("dw", data_root, 2) && strcmp (dw_root, data_root))) {
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

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/query: using datawarp root: %s, pfs backing store: %s",
            dw_root, next_data_root);

  rc = builtin_posix_component.query (context, dw_root, NULL, &posix_module);
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
  new_module->posix_compare = posix_module->compare;

  new_module->posix_module.base.dataset_open = builtin_datawarp_module_dataset_open;
  new_module->posix_module.base.dataset_unlink = builtin_datawarp_module_dataset_unlink;
  new_module->posix_module.base.compare = builtin_datawarp_module_compare;
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
