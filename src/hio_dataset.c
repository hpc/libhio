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

#include "hio_types.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

static hio_var_enum_t hioi_dataset_file_modes = {
  .count = 2,
  .values = {{.string_value = "basic", .value = HIO_FILE_MODE_BASIC},
             {.string_value = "optimized", .value = HIO_FILE_MODE_OPTIMIZED}},
};

hio_element_t hioi_element_alloc (hio_dataset_t dataset, const char *name) {
  hio_element_t element;

  element = (hio_element_t) calloc (1, sizeof (*element));
  if (NULL == element) {
    return NULL;
  }

  element->element_object.identifier = strdup (name);
  if (NULL == element->element_object.identifier) {
    free (element);
    return NULL;
  }

  element->element_object.type = HIO_OBJECT_TYPE_ELEMENT;

  element->element_dataset = dataset;

  hioi_list_init (element->element_segment_list);

  return element;
}

void hioi_element_release (hio_element_t element) {
  if (NULL != element) {
    if (NULL != element->element_object.identifier) {
      free (element->element_object.identifier);
    }

    if (NULL != element->element_backing_file) {
      free (element->element_backing_file);
    }

    free (element);
  }
}

static int hioi_dataset_data_lookup (hio_context_t context, const char *name, hio_dataset_data_t **data) {
  hio_dataset_data_t *ds_data;

  /* look for existing persistent data */
  pthread_mutex_lock (&context->context_lock);
  hioi_list_foreach (ds_data, context->context_dataset_data, hio_dataset_data_t, dd_list) {
    if (0 == strcmp (ds_data->dd_name, name)) {
      pthread_mutex_unlock (&context->context_lock);
      *data = ds_data;
      return HIO_SUCCESS;
    }
  }

  /* allocate new persistent dataset data and add it to the context */
  ds_data = (hio_dataset_data_t *) calloc (1, sizeof (*ds_data));
  if (NULL == ds_data) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  ds_data->dd_name = strdup (name);
  if (NULL == ds_data->dd_name) {
    free (ds_data);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  ds_data->dd_last_id = -1;

  hioi_list_init (ds_data->dd_backend_data);

  hioi_list_append (ds_data, context->context_dataset_data, dd_list);

  *data = ds_data;
  pthread_mutex_unlock (&context->context_lock);

  return HIO_SUCCESS;
}

hio_dataset_t hioi_dataset_alloc (hio_context_t context, const char *name, int64_t id,
                                  hio_flags_t flags, hio_dataset_mode_t mode,
                                  size_t dataset_size) {
  hio_dataset_t new_dataset;
  int rc;

  /* bozo check for invalid dataset object size */
  assert (dataset_size >= sizeof (*new_dataset));

  /* allocate new dataset object */
  new_dataset = calloc (1, dataset_size);
  if (NULL == new_dataset) {
    return NULL;
  }

  /* lookup/allocate persistent dataset data. this data will keep track of per-dataset
   * statistics (average write time, last successfull checkpoint, etc) */
  rc = hioi_dataset_data_lookup (context, name, &new_dataset->dataset_data);
  if (HIO_SUCCESS != rc) {
    free (new_dataset);
    return NULL;
  }

  /* initialize new dataset object */
  new_dataset->dataset_object.identifier = strdup (name);
  if (NULL == new_dataset->dataset_object.identifier) {
    free (new_dataset);
    return NULL;
  }

  new_dataset->dataset_object.type = HIO_OBJECT_TYPE_DATASET;
  new_dataset->dataset_id = id;
  new_dataset->dataset_flags = flags;
  new_dataset->dataset_mode = mode;
  new_dataset->dataset_context = context;

  new_dataset->dataset_file_mode = HIO_FILE_MODE_BASIC;
  hioi_config_add (context, &new_dataset->dataset_object, &new_dataset->dataset_file_mode,
                   "dataset_file_mode", HIO_CONFIG_TYPE_INT32, &hioi_dataset_file_modes,
                   "Modes for writing dataset files. Valid values: (0: basic, 1: optimized)", 0);

  /* set up performance variables */
  hioi_perf_add (context, &new_dataset->dataset_object, &new_dataset->dataset_bytes_read, "total_bytes_read",
                 HIO_CONFIG_TYPE_UINT64, NULL, "Total number of bytes read in this dataset instance", 0);

  hioi_perf_add (context, &new_dataset->dataset_object, &new_dataset->dataset_bytes_written, "total_bytes_written",
                 HIO_CONFIG_TYPE_UINT64, NULL, "Total number of bytes written in this dataset instance", 0);


  hioi_list_init (new_dataset->dataset_element_list);

  return new_dataset;
}

void hioi_dataset_release (hio_dataset_t *set) {
  hio_element_t element, next;
  hio_context_t context;
  hio_module_t *module;

  if (!set || !*set) {
    return;
  }

  module = (*set)->dataset_module;
  context = (*set)->dataset_context;

  hioi_list_foreach_safe(element, next, (*set)->dataset_element_list, struct hio_element_t, element_list) {
    if (element->element_is_open) {
      hioi_log (context, HIO_VERBOSE_WARN, "element still open at dataset close");
      module->element_close (module, element);
    }

    hioi_list_remove(element, element_list);
    hioi_element_release (element);
  }

  if ((*set)->dataset_object.identifier) {
    free ((*set)->dataset_object.identifier);
  }

  free (*set);
  *set = NULL;
}

void hioi_dataset_add_element (hio_dataset_t dataset, hio_element_t element) {
  hioi_list_append (element, dataset->dataset_element_list, element_list);
}

int hioi_element_add_segment (hio_element_t element, off_t file_offset, uint64_t app_offset0,
                              uint64_t app_offset1, size_t segment_length) {
  hio_manifest_segment_t *segment = NULL;

  if (element->element_segment_list.prev != &element->element_segment_list) {
    unsigned long last_offset;

    segment = hioi_list_item(element->element_segment_list.prev, hio_manifest_segment_t, segment_list);

    last_offset = segment->segment_app_offset0 + segment->segment_length;

    if (last_offset == app_offset0) {
      segment->segment_length += segment_length;
      return HIO_SUCCESS;
    }
  }

  segment = (hio_manifest_segment_t *) malloc (sizeof (*segment));
  if (NULL == segment) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  segment->segment_file_offset = (uint64_t) file_offset;
  segment->segment_app_offset0 = app_offset0;
  segment->segment_app_offset1 = app_offset1;
  segment->segment_length      = segment_length;

  hioi_list_append (segment, element->element_segment_list, segment_list);

  return HIO_SUCCESS;
}

hio_dataset_backend_data_t *hioi_dbd_alloc (hio_dataset_data_t *data, const char *backend_name, size_t size) {
  hio_dataset_backend_data_t *new_backend_data;
  int rc;

  assert (size >= sizeof (*new_backend_data));

  new_backend_data = calloc (1, size);
  if (NULL == new_backend_data) {
    return NULL;
  }

  new_backend_data->dbd_backend_name = strdup (backend_name);
  if (NULL == new_backend_data->dbd_backend_name) {
    free (new_backend_data);
    return NULL;
  }

  hioi_list_append (new_backend_data, data->dd_backend_data, dbd_list);

  return new_backend_data;
}

/**
 * Reteive stored backend data
 *
 * @param[in] data         dataset persistent data structure
 * @param[in] backend_name name of the requesting backend
 */
hio_dataset_backend_data_t *hioi_dbd_lookup_backend_data (hio_dataset_data_t *data, const char *backend_name) {
  hio_dataset_backend_data_t *dbd_data;

  hioi_list_foreach (dbd_data, data->dd_backend_data, hio_dataset_backend_data_t, dbd_list) {
    if (0 == strcmp (dbd_data->dbd_backend_name, backend_name)) {
      return dbd_data;
    }
  }

  return NULL;
}
