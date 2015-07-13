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

typedef struct builtin_datawarp_module_t {
  hio_module_t base;
  char *pfs_path;
  builtin_posix_module_t *posix_module;
} builtin_datawarp_module_t;

typedef struct builtin_datawarp_module_dataset_t {
  struct hio_dataset base;
  char *pfs_path;
  int stage_mode;
  builtin_posix_module_dataset_t *posix_dataset;
} builtin_datawarp_module_dataset_t;

typedef struct builtin_datawarp_dataset_backend_data_t {
  hio_dataset_backend_data_t base;

  int64_t last_scheduled_stage_id;
} builtin_datawarp_dataset_backend_data_t;

static hio_var_enum_value_t builtin_datawarp_stage_mode_values[] = {
  {.string_value = "immediate", .value = DW_STAGE_IMMEDIATE},
  {.string_value = "end_of_job", .value = DW_STAGE_AT_JOB_END},
};

static hio_var_enum_t builtin_datawarp_stage_modes = {
  .count = 2,
  .values = builtin_datawarp_stage_mode_values,
};

static int builtin_datawarp_module_dataset_list (struct hio_module_t *module, const char *name,
                                                 hio_dataset_header_t **headers, int *count) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;

  return datawarp_module->posix_module->base.dataset_list (&datawarp_module->posix_module->base, name, headers,
                                                           count);
}

static int builtin_datawarp_module_dataset_open (struct hio_module_t *module,
                                                 hio_dataset_t *set_out, const char *name,
                                                 int64_t set_id, int flags,
                                                 hio_dataset_mode_t mode) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) datawarp_module->posix_module;
  builtin_datawarp_module_dataset_t *datawarp_dataset;
  hio_context_t context = module->context;
  builtin_posix_module_dataset_t *posix_dataset;
  int rc = HIO_SUCCESS;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "builtin-datawarp/dataset_open: opening dataset %s:%lu",
	    name, (unsigned long) set_id);

  datawarp_dataset = (builtin_datawarp_module_dataset_t *)
    hioi_dataset_alloc (context, name, set_id, flags, mode,
			sizeof (builtin_datawarp_module_dataset_t));
  if (NULL == datawarp_dataset) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* open the posix dataset */
  rc = posix_module->base.dataset_open (&posix_module->base, (hio_dataset_t *) &posix_dataset, name, set_id,
                                        flags, mode);
  if (HIO_SUCCESS != rc) {
    free (datawarp_dataset);
    return rc;
  }

  /* NTH -- TODO -- Need a way to expose the variable associated with the posix dataset up to the user */
  datawarp_dataset->posix_dataset = posix_dataset;
  datawarp_dataset->base.dataset_module = module;

  if (flags & HIO_FLAG_WRITE) {
    hioi_config_add (context, &datawarp_dataset->base.dataset_object, &datawarp_dataset->stage_mode,
                     "datawarp_stage_mode", HIO_CONFIG_TYPE_INT32, &builtin_datawarp_stage_modes,
                     "Datawarp stage mode to use with this dataset instance", 0);
  }

  *set_out = &datawarp_dataset->base;

  return HIO_SUCCESS;
}

static int builtin_datawarp_module_dataset_close (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_datawarp_module_dataset_t *datawarp_dataset = (builtin_datawarp_module_dataset_t *) dataset;
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) datawarp_module->posix_module;
  builtin_posix_module_dataset_t *posix_dataset = datawarp_dataset->posix_dataset;
  mode_t pfs_mode = posix_module->access_mode;
  hio_context_t context = module->context;
  char *dataset_path = NULL;
  int rc;

  if (0 == context->context_rank && (dataset->dataset_flags & HIO_FLAG_WRITE)) {
    /* keep a copy of the base path used by the posix module */
    dataset_path = strdup (posix_dataset->base_path);
    if (NULL == dataset_path) {
      /* out of memory */
      return HIO_ERR_OUT_OF_RESOURCE;
    }
  }

  rc = posix_module->base.dataset_close (&posix_module->base, (hio_dataset_t) posix_dataset);
  if (HIO_SUCCESS != rc) {
    free (dataset_path);
    return rc;
  }

  hioi_dataset_release ((hio_dataset_t *) &posix_dataset);

  if (0 == context->context_rank && (dataset->dataset_flags & HIO_FLAG_WRITE)) {
    char *pfs_path;

    rc = asprintf (&pfs_path, "%s/%s.hio/%s/%llu", datawarp_module->pfs_path, context->context_object.identifier,
                   dataset->dataset_object.identifier, dataset->dataset_id);
    if (0 > rc) {
      free (dataset_path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: staging datawarp dataset %s::%lld. "
              "burst-buffer directory: %s lustre dir: %s DW stage mode: %d",  dataset->dataset_object.identifier,
              dataset->dataset_id, dataset_path, pfs_path, datawarp_dataset->stage_mode);

    rc = hio_mkpath (pfs_path, pfs_mode);
    if (HIO_SUCCESS != rc) {
      free (dataset_path);
      free (pfs_path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    rc = dw_stage_directory_out (dataset_path, pfs_path, datawarp_dataset->stage_mode);
    free (pfs_path);
    free (dataset_path);
    if (0 != rc) {
      hio_err_push (HIO_ERROR, context, &dataset->dataset_object, "builtin-datawarp/dataset_close: error starting "
                    "data stage on dataset %s::%lld. DWRC: %d", dataset->dataset_object.identifier, dataset->dataset_id, rc);
      return HIO_ERROR;
    }

    if (DW_STAGE_AT_JOB_END == datawarp_dataset->stage_mode) {
      builtin_datawarp_dataset_backend_data_t *ds_data;

      ds_data = (builtin_datawarp_dataset_backend_data_t *) hioi_dbd_lookup_backend_data (dataset->dataset_data, "datawarp");
      if (NULL == ds_data) {
        ds_data = (builtin_datawarp_dataset_backend_data_t *) hioi_dbd_alloc (dataset->dataset_data, "datawarp",
                                                                              sizeof (*ds_data));
        if (NULL == ds_data) {
          return HIO_ERR_OUT_OF_RESOURCE;
        }

        ds_data->last_scheduled_stage_id = dataset->dataset_id;
      }

      if (ds_data->last_scheduled_stage_id == dataset->dataset_id) {
        /* this dataset has the same identifier as the last know good one or this is the first successfull stage
         * of this dataset in this context. destaging the dataset will undo the stage that was just performed so
         * just return */
        return HIO_SUCCESS;
      }

      rc = asprintf (&dataset_path, "%s/%s.hio/%s/%llu", module->data_root, context->context_object.identifier,
                     dataset->dataset_object.identifier, ds_data->last_scheduled_stage_id);
      if (0 > rc) {
        return HIO_ERR_OUT_OF_RESOURCE;
      }

      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/dataset_close: revoking end-of-job stage for datawarp dataset %s::%lld. "
                "burst-buffer directory: %s",  dataset->dataset_object.identifier, ds_data->last_scheduled_stage_id,
                dataset_path);

      /* revoke the end of job stage for the previous dataset */
      rc = dw_stage_directory_out (dataset_path, NULL, DW_REVOKE_STAGE_AT_JOB_END);
      free (dataset_path);
      if (0 != rc) {
        hio_err_push (HIO_ERROR, context, &dataset->dataset_object, "builtin-datawarp/dataset_close: error revoking prior "
                      "end-of-job stage of dataset %s::%lld. errno: %d", dataset->dataset_object.identifier,
                      ds_data->last_scheduled_stage_id, errno);
        return HIO_ERROR;
      }

      ds_data->last_scheduled_stage_id = dataset->dataset_id;

      /* remove the last end-of-job dataset from the burst buffer */
      (void) posix_module->base.dataset_unlink (&posix_module->base, dataset->dataset_object.identifier,
                                                ds_data->last_scheduled_stage_id);
    }
  }

  return HIO_SUCCESS;
}

static int builtin_datawarp_module_dataset_unlink (struct hio_module_t *module, const char *name, int64_t set_id) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  return datawarp_module->posix_module->base.dataset_unlink (&datawarp_module->posix_module->base, name, set_id);
}

static int builtin_datawarp_module_element_open (struct hio_module_t *module, hio_dataset_t dataset,
                                                 hio_element_t *element_out, const char *element_name,
                                                 int flags) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_datawarp_module_dataset_t *datawarp_dataset = (builtin_datawarp_module_dataset_t *) dataset;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) datawarp_module->posix_module;
  builtin_posix_module_dataset_t *posix_dataset = datawarp_dataset->posix_dataset;

  return posix_module->base.element_open (&posix_module->base, &posix_dataset->base, element_out,
                                          element_name, flags);
}

static int builtin_datawarp_module_element_close (struct hio_module_t *module, hio_element_t element) {

  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  return datawarp_module->posix_module->base.element_close (&datawarp_module->posix_module->base, element);
}

static int builtin_datawarp_module_element_write_strided_nb (struct hio_module_t *module, hio_element_t element,
                                                          hio_request_t *request, off_t offset, const void *ptr,
                                                          size_t count, size_t size, size_t stride) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  return datawarp_module->posix_module->base.element_write_strided_nb (&datawarp_module->posix_module->base,
                                                                       element, request, offset, ptr, count,
                                                                       size, stride);
}

static int builtin_datawarp_module_element_read_strided_nb (struct hio_module_t *module, hio_element_t element,
                                                         hio_request_t *request, off_t offset, void *ptr,
                                                         size_t count, size_t size, size_t stride) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  return datawarp_module->posix_module->base.element_read_strided_nb (&datawarp_module->posix_module->base,
                                                                      element, request, offset, ptr, count,
                                                                      size, stride);
}

static int builtin_datawarp_module_element_flush (struct hio_module_t *module, hio_element_t element,
                                               hio_flush_mode_t mode) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  return datawarp_module->posix_module->base.element_flush (&datawarp_module->posix_module->base, element, mode);
}

static int builtin_datawarp_module_element_complete (struct hio_module_t *module, hio_element_t element) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  return datawarp_module->posix_module->base.element_complete (&datawarp_module->posix_module->base, element);
}

static int builtin_datawarp_module_fini (struct hio_module_t *module) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;

  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "Finalizing datawarp filesystem module for data root %s",
	    module->data_root);

  if (datawarp_module->posix_module) {
    datawarp_module->posix_module->base.fini (&datawarp_module->posix_module->base);
    datawarp_module->posix_module = NULL;
  }

  free (datawarp_module->pfs_path);
  free (module->data_root);
  free (module);

  return HIO_SUCCESS;
}

hio_module_t builtin_datawarp_module_template = {
  .dataset_open     = builtin_datawarp_module_dataset_open,
  .dataset_close    = builtin_datawarp_module_dataset_close,
  .dataset_unlink   = builtin_datawarp_module_dataset_unlink,

  .element_open     = builtin_datawarp_module_element_open,
  .element_close    = builtin_datawarp_module_element_close,

  .element_write_strided_nb = builtin_datawarp_module_element_write_strided_nb,
  .element_read_strided_nb  = builtin_datawarp_module_element_read_strided_nb,

  .element_flush    = builtin_datawarp_module_element_flush,
  .element_complete = builtin_datawarp_module_element_complete,

  .dataset_list     = builtin_datawarp_module_dataset_list,

  .fini             = builtin_datawarp_module_fini,
};

static int builtin_datawarp_component_init (void) {
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

  if (NULL == context->context_datawarp_root) {
    hioi_log (context, HIO_VERBOSE_ERROR, "builtin-datawarp/query: attempted to use datawarp without specifying "
              "the mount point of the datawarp file system");
    return HIO_ERR_NOT_AVAILABLE;
  }

  if (NULL == next_data_root || strncasecmp (next_data_root, "posix:", 6) || access (next_data_root + 6, F_OK)) {
    hioi_log (context, HIO_VERBOSE_ERROR, "builtin-datawarp/query: attempting to use datawarp but PFS stage out "
              "path %s is not accessible", next_data_root);
    return HIO_ERR_NOT_AVAILABLE;
  }

  /* get a builtin-posix module for interfacing with the burst buffer file system */
  if (0 == strcmp (context->context_datawarp_root, "auto")) {
    /* NTH: This will have to be updated or changed to a system parameter in the future */
    rc = asprintf (&posix_data_root, "posix:/dwphase1/%s", getenv ("USER"));
  } else {
    rc = asprintf (&posix_data_root, "posix:%s", context->context_datawarp_root);
  }

  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
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

  memcpy (new_module, &builtin_datawarp_module_template, sizeof (builtin_datawarp_module_template));

  new_module->posix_module = (builtin_posix_module_t *) posix_module;

  new_module->pfs_path = strdup (next_data_root + 6);
  if (NULL == new_module->pfs_path) {
    posix_module->fini (posix_module);
    free (new_module);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  rc = asprintf (&new_module->base.data_root, "datawarp:%s", context->context_datawarp_root);
  if (0 > rc) {
    posix_module->fini (posix_module);
    free (new_module);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  new_module->base.context = context;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin-datawarp/query: created datawarp filesystem module "
            "for data root %s", new_module->base.data_root);

  *module = &new_module->base;

  return HIO_SUCCESS;
}

hio_component_t builtin_datawarp_component = {
  .init = builtin_datawarp_component_init,
  .fini = builtin_datawarp_component_fini,

  .query = builtin_datawarp_component_query,
  .flags = 0,
  .priority = 10,
};
