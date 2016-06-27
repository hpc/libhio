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

/* datawarp is just a posix+ interface */
#include "builtin-posix_component.h"
#include "hio_internal.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>

#include <sys/stat.h>

#include <datawarp.h>

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
  hio_module_fini_fn_t posix_fini;
  char *pfs_path;
} builtin_datawarp_module_t;

typedef struct builtin_datawarp_module_dataset_t {
  builtin_posix_module_dataset_t posix_dataset;
  char *pfs_path;
  int stage_mode;

  hio_dataset_close_fn_t posix_ds_close;
} builtin_datawarp_module_dataset_t;

typedef struct builtin_datawarp_dataset_backend_data_t {
  hio_dataset_backend_data_t base;

  int64_t last_scheduled_stage_id;
  uint64_t last_immediate_stage;
} builtin_datawarp_dataset_backend_data_t;

static hio_var_enum_value_t builtin_datawarp_stage_mode_values[] = {
  {.string_value = "disable", .value = -2},
  {.string_value = "auto", .value = -1},
  {.string_value = "immediate", .value = DW_STAGE_IMMEDIATE},
  {.string_value = "end_of_job", .value = DW_STAGE_AT_JOB_END},
};

static hio_var_enum_t builtin_datawarp_stage_modes = {
  .count = 3,
  .values = builtin_datawarp_stage_mode_values,
};

static int builtin_datawarp_module_dataset_close (hio_dataset_t dataset);

static int builtin_datawarp_module_dataset_open (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_posix_module_t *posix_module = &datawarp_module->posix_module;
  builtin_datawarp_module_dataset_t *datawarp_dataset = (builtin_datawarp_module_dataset_t *) dataset;
  hio_context_t context = module->context;
  builtin_posix_module_dataset_t *posix_dataset;
  int rc = HIO_SUCCESS;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "builtin-datawarp/dataset_open: opening dataset %s:%lu",
            hioi_object_identifier (dataset), (unsigned long) dataset->ds_id);

  /* open the posix dataset */
  rc = datawarp_module->posix_open (&posix_module->base, dataset);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  dataset->ds_module = module;

  if (dataset->ds_flags & HIO_FLAG_WRITE) {
    /* default to auto mode */
    datawarp_dataset->stage_mode = -1;
    hioi_config_add (context, &dataset->ds_object, &datawarp_dataset->stage_mode,
                     "datawarp_stage_mode", HIO_CONFIG_TYPE_INT32, &builtin_datawarp_stage_modes,
                     "Datawarp stage mode to use with this dataset instance", 0);
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

  if (datawarp_module->pfs_path && 0 == context->c_rank && (dataset->ds_flags & HIO_FLAG_WRITE) && -2 != datawarp_dataset->stage_mode) {
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

    stage_mode = datawarp_dataset->stage_mode;
    if (-1 == datawarp_dataset->stage_mode) {
      stage_mode = DW_STAGE_AT_JOB_END;
    }

    rc = hio_mkpath (context, pfs_path, pfs_mode);
    if (HIO_SUCCESS != rc) {
      free (dataset_path);
      free (pfs_path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    rc = dw_stage_directory_out (dataset_path, pfs_path, stage_mode);

    if (0 != rc) {
      hioi_err_push (HIO_ERROR, &dataset->ds_object, "builtin-datawarp/dataset_close: error starting "
                    "data stage on dataset %s::%lld. DWRC: %d", hioi_object_identifier (dataset), dataset->ds_id, rc);
      
      hioi_log (context, HIO_VERBOSE_DEBUG_XLOW, "dw_stage_directory_out(%s, %s, %d) returns %d errno: %d",
                dataset_path, pfs_path, stage_mode, rc, errno); 

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

    free (pfs_path);
    free (dataset_path);


    if (DW_STAGE_AT_JOB_END == stage_mode) {
      builtin_datawarp_dataset_backend_data_t *ds_data;
      int64_t last_stage_id;

      ds_data = (builtin_datawarp_dataset_backend_data_t *) hioi_dbd_lookup_backend_data (dataset->ds_data, "datawarp");
      if (NULL == ds_data) {
        ds_data = (builtin_datawarp_dataset_backend_data_t *) hioi_dbd_alloc (dataset->ds_data, "datawarp",
                                                                              sizeof (*ds_data));
        if (NULL == ds_data) {
          return HIO_ERR_OUT_OF_RESOURCE;
        }

        ds_data->last_scheduled_stage_id = dataset->ds_id;
      }

      if (ds_data->last_scheduled_stage_id == dataset->ds_id) {
        /* this dataset has the same identifier as the last know good one or this is the first successful stage
         * of this dataset in this context. destaging the dataset will undo the stage that was just performed so
         * just return */
        return HIO_SUCCESS;
      }

      last_stage_id = ds_data->last_scheduled_stage_id;

      rc = asprintf (&dataset_path, "%s/%s.hio/%s/%llu", posix_module->base.data_root, hioi_object_identifier (context),
                     hioi_object_identifier (dataset), last_stage_id);
      if (0 > rc) {
        return HIO_ERR_OUT_OF_RESOURCE;
      }

      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: revoking end-of-job stage for datawarp dataset %s::%lld. "
                "burst-buffer directory: %s", hioi_object_identifier( dataset), last_stage_id, dataset_path);

      /* revoke the end of job stage for the previous dataset */
      rc = dw_stage_directory_out (dataset_path, NULL, DW_REVOKE_STAGE_AT_JOB_END);
      free (dataset_path);
      if (0 != rc) {
        hioi_err_push (HIO_ERROR, &dataset->ds_object, "builtin-datawarp/dataset_close: error revoking prior "
                      "end-of-job stage of dataset %s::%lld. errno: %d", hioi_object_identifier (dataset),
                      last_stage_id, errno);
        return HIO_ERROR;
      }

      ds_data->last_scheduled_stage_id = dataset->ds_id;

      /* remove the last end-of-job dataset from the burst buffer */
      (void) posix_module->base.dataset_unlink (&posix_module->base, hioi_object_identifier (dataset), last_stage_id);

      /* remove created directories on pfs */
      rc = asprintf (&pfs_path, "%s/%s.hio/%s/%llu", datawarp_module->pfs_path, hioi_object_identifier (context),
                     hioi_object_identifier (dataset), last_stage_id);
      if (0 > rc) {
        free (dataset_path);
        return HIO_ERR_OUT_OF_RESOURCE;
      }

      /* ignore failure. if the directory is not empty then we shouldn't be removing it */
      (void) rmdir (pfs_path);
      errno = 0;

      free (pfs_path);
    }
  }

  return HIO_SUCCESS;
}

static int builtin_datawarp_module_fini (struct hio_module_t *module) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;

  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "Finalizing datawarp filesystem module for data root %s",
	    module->data_root);

  free (datawarp_module->pfs_path);

  if (datawarp_module->posix_fini) {
    /* posix fini will free the module so call this last */
    datawarp_module->posix_fini (&datawarp_module->posix_module.base);
  } else {
    free (module->data_root);
    free (module);
  }

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
  new_module->posix_fini = posix_module->fini;
  new_module->posix_module.base.dataset_open = builtin_datawarp_module_dataset_open;
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

  return HIO_SUCCESS;
}

hio_component_t builtin_datawarp_component = {
  .init = builtin_datawarp_component_init,
  .fini = builtin_datawarp_component_fini,

  .query = builtin_datawarp_component_query,
  .flags = 0,
  .priority = 10,
};
