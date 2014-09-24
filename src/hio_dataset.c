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

#include "hio_types.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

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

    free (element);
  }
}

hio_dataset_t hioi_dataset_alloc (hio_context_t context, const char *name, int64_t id,
                                  hio_flags_t flags, hio_dataset_mode_t mode,
                                  size_t dataset_size) {
  hio_dataset_t new_dataset;

  assert (dataset_size >= sizeof (*new_dataset));

  new_dataset = calloc (1, dataset_size);
  if (NULL == new_dataset) {
    return NULL;
  }

  /* initialize dataset members */
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
  hio_dataset_t dataset = element->element_dataset;
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
