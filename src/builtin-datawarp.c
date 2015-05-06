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
#include <datawarp.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>

typedef struct builtin_datawarp_module_t {
  hio_module_t base;
  char *pfs_path;
  builtin_posix_module_t *posix_module;
} builtin_datawarp_module_t;

typedef struct builtin_datawarp_module_dataset_t {
  struct hio_dataset_t base;
  char *pfs_path;
  int stage_mode;
  builtin_posix_module_dataset_t *posix_dataset;
} builtin_datawarp_module_dataset_t;

static hio_var_enum_t builtin_datawarp_stage_modes = {
  .count = 2,
  .values = {{.string_value = "immediate", .value = DW_STAGE_IMMEDIATE},
             {.string_value = "end_of_job", .value = DW_STAGE_AT_JOB_END}},
};

static int builtin_datawarp_module_dataset_list (struct hio_module_t *module, const char *name,
                                                 int64_t **set_ids, int *set_id_count) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;

  return datawarp_module->posix_module->base.dataset_list (&datawarp_module->posix_module->base, name, set_ids,
                                                           set_id_count);
}

static int builtin_datawarp_module_dataset_open (struct hio_module_t *module,
                                                 hio_dataset_t *set_out, const char *name,
                                                 int64_t set_id, hio_flags_t flags,
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

  if (flags & HIO_FLAG_WRONLY) {
    hioi_config_add (context, &datawarp_dataset->base.dataset_object, &datawarp_dataset->stage_mode,
                     "datawarp_stage_mode", HIO_CONFIG_TYPE_INT32, &builtin_datawarp_stage_modes,
                     "Datawarp stage mode to use with this dataset instance", 0);
  }

  *set_out = &datawarp_dataset->base;

  return HIO_SUCCESS;
}

/* remove once implemented in datawarp */
static int temp_dw_stage_directory_out (const char *dataset_path, const char *pfs_path, int mode) {
  DIR *dw_dir = opendir (dataset_path);
  struct dirent *dw_entry;
  int rc = 0;

  if (NULL == dw_dir) {
    errno = ENOENT;
    return -1;
  }

  while (NULL != (dw_entry = readdir (dw_dir))) {
    char *dw_tmp, *pfs_tmp;
    if ('.' == dw_entry->d_name[0]) {
      continue;
    }

    rc = asprintf (&dw_tmp, "%s/%s", dataset_path, dw_entry->d_name);
    if (0 > rc) {
      break;
    }

    rc = asprintf (&pfs_tmp, "%s/%s", pfs_path, dw_entry->d_name);
    if (0 > rc) {
      free (dw_tmp);
      break;
    }

    if (DT_DIR == dw_entry->d_type) {
      mkdir (pfs_tmp);
      rc = temp_dw_stage_directory_out (dw_tmp, pfs_tmp, mode);
    } else {
      rc = dw_stage_file_out (dw_tmp, pfs_tmp, mode);
    }

    free (dw_tmp);
    free (pfs_tmp);

    if (0 != rc) {
      break;
    }
  }

  closedir (dw_dir);
  return rc;
}

static int builtin_datawarp_module_dataset_close (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_datawarp_module_dataset_t *datawarp_dataset = (builtin_datawarp_module_dataset_t *) dataset;
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) datawarp_module->posix_module;
  builtin_posix_module_dataset_t *posix_dataset = datawarp_dataset->posix_dataset;
  hio_context_t context = module->context;
  char *dataset_path = NULL;
  int rc;

  if (0 == context->context_rank) {
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

  if (0 == context->context_rank && (dataset->dataset_flags & HIO_FLAG_WRONLY)) {
    char *pfs_path;

    rc = asprintf (&pfs_path, "%s/%s.hio/%s/%llu", datawarp_module->pfs_path, context->context_object.identifier,
                   dataset->dataset_object.identifier, dataset->dataset_id);
    if (0 > rc) {
      free (dataset_path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Staging datawarp directory %s to %s with mode %d",
              dataset_path, pfs_path, datawarp_dataset->stage_mode);

    rc = hio_mkpath (pfs_path, datawarp_module->posix_module->access_mode);
    if (HIO_SUCCESS != rc) {
      free (dataset_path);
      free (pfs_path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    rc = temp_dw_stage_directory_out (dataset_path, pfs_path, datawarp_dataset->stage_mode);
    free (pfs_path);
    if (0 != rc) {
      free (dataset_path);
      hio_err_push (HIO_ERROR, context, &dataset->dataset_object, "Error starting data stage. DWRC: %d", rc);
      return HIO_ERROR;
    }
    /* TODO -- remove previous stage that was specified as end of job */
  }

  return HIO_SUCCESS;
}

static int builtin_datawarp_module_dataset_unlink (struct hio_module_t *module, const char *name, int64_t set_id) {
  builtin_datawarp_module_t *datawarp_module = (builtin_datawarp_module_t *) module;
  return datawarp_module->posix_module->base.dataset_unlink (&datawarp_module->posix_module->base, name, set_id);
}

static int builtin_datawarp_module_element_open (struct hio_module_t *module, hio_dataset_t dataset,
                                                 hio_element_t *element_out, const char *element_name,
                                                 hio_flags_t flags) {
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
                                                          hio_request_t *request, off_t offset, void *ptr,
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

  if (strncasecmp("datawarp", data_root, 8) && strncasecmp("dw", "dataroot", 2)) {
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Module datawarp does not match for data root %s",
	      data_root);
    return HIO_ERR_NOT_AVAILABLE;
  }

  if (NULL == context->context_datawarp_root) {
    hioi_log (context, HIO_VERBOSE_ERROR, "Attempted to use datawarp without specifying the mount point "
              "of the datawarp file system");
    return HIO_ERR_NOT_AVAILABLE;
  }

  if (NULL == next_data_root || strncasecmp (next_data_root, "posix:", 6) || access (next_data_root + 6, F_OK)) {
    hioi_log (context, HIO_VERBOSE_ERROR, "Attempting to use datawarp but PFS stage out path %s is not "
              "accessible", next_data_root);
    return HIO_ERR_NOT_AVAILABLE;
  }

  if (0 == strcmp (context->context_datawarp_root, "auto")) {
    /* NTH: This will have to be updated or changed to a system parameter in the future */
    rc = asprintf (&posix_data_root, "posix:/dwphase1/%s", getenv ("USER"));
  } else {
    rc = asprintf (&posix_data_root, "posix:%s", context->context_datawarp_root);
  }

  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Datawarp in use with dw root: %s, backing store: %s",
            posix_data_root, next_data_root);

  rc = builtin_posix_component.query (context, posix_data_root, NULL, &posix_module);
  free (posix_data_root);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

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

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Created datawarp filesystem module for data root %s",
	    new_module->base.data_root);

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
