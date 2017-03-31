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

#include "hio_manifest.h"

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <bzlib.h>

#if !defined(__STRICT_ANSI__)
/* silence pedantic error about extension usage in json-c */
#define __STRICT_ANSI__
#endif

#include <json.h>

#define HIO_MANIFEST_VERSION "3.0"
#define HIO_MANIFEST_COMPAT  "3.0"

/* manifest helper functions */
static void hioi_manifest_set_number (json_object *parent, const char *name, unsigned long value) {
  json_object *new_object = json_object_new_int64 ((int64_t) value);

  assert (NULL != new_object);
  json_object_object_add (parent, name, new_object);
}

static void hioi_manifest_set_signed_number (json_object *parent, const char *name, long value) {
  json_object *new_object = json_object_new_int64 (value);

  assert (NULL != new_object);
  json_object_object_add (parent, name, new_object);
}

static void hioi_manifest_set_string (json_object *parent, const char *name, const char *value) {
  json_object *new_object = json_object_new_string (value);

  assert (NULL != new_object);
  json_object_object_add (parent, name, new_object);
}

static void hioi_manifest_set_double (json_object *parent, const char *name, const double value) {
  json_object *new_object = json_object_new_double (value);

  assert (NULL != new_object);
  json_object_object_add (parent, name, new_object);
}

static json_object *hio_manifest_new_array (json_object *parent, const char *name) {
  json_object *new_object = json_object_new_array ();

  if (NULL == new_object) {
    return NULL;
  }

  json_object_object_add (parent, name, new_object);

  return new_object;
}

static json_object *hioi_manifest_find_object (const json_object *parent, const char *name) {
  json_object *object;

  if (json_object_object_get_ex ((json_object *) parent, name, &object)) {
    return object;
  }

  return NULL;
}

int hioi_manifest_get_string (const json_object *parent, const char *name, const char **string) {
  json_object *object;

  object = hioi_manifest_find_object (parent, name);
  if (NULL == object) {
    return HIO_ERR_NOT_FOUND;
  }

  *string = json_object_get_string (object);
  if (!*string) {
    return HIO_ERROR;
  }

  return HIO_SUCCESS;
}

int hioi_manifest_get_number (const json_object *parent, const char *name, unsigned long *value) {
  json_object *object;

  object = hioi_manifest_find_object (parent, name);
  if (NULL == object) {
    return HIO_ERR_NOT_FOUND;
  }

  *value = (unsigned long) json_object_get_int64 (object);

  return HIO_SUCCESS;
}

int hioi_manifest_get_signed_number (const json_object *parent, const char *name, long *value) {
  json_object *object;

  object = hioi_manifest_find_object (parent, name);
  if (NULL == object) {
    return HIO_ERR_NOT_FOUND;
  }

  *value = (long) json_object_get_int64 (object);

  return HIO_SUCCESS;
}

static json_object *hio_manifest_generate_empty_3_0 (void) {
  json_object *top;

  top = json_object_new_object ();
  if (NULL == top) {
    return NULL;
  }

  hioi_manifest_set_string (top, HIO_MANIFEST_KEY_VERSION, HIO_MANIFEST_VERSION);
  hioi_manifest_set_string (top, HIO_MANIFEST_KEY_COMPAT, HIO_MANIFEST_COMPAT);
  hioi_manifest_set_string (top, HIO_MANIFEST_KEY_HIO_VERSION, PACKAGE_VERSION);

  return top;
}

static int hioi_manifest_save_vars (json_object *top, const char *name, hio_var_array_t *var_array) {
  json_object *object;
  int var_count;

  object = json_object_new_object ();
  if (NULL == object) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  json_object_object_add (top, name, object);

  var_count = var_array->var_count;

  for (int i = 0 ; i < var_count ; ++i) {
    hio_var_t *var = var_array->vars + i;
    const char *name = var->var_name;

    switch (var->var_type) {
    case HIO_CONFIG_TYPE_INT32:
      hioi_manifest_set_signed_number (object, name, var->var_storage->int32val);
      break;
    case HIO_CONFIG_TYPE_INT64:
      hioi_manifest_set_signed_number (object, name, var->var_storage->int64val);
      break;
    case HIO_CONFIG_TYPE_UINT32:
      hioi_manifest_set_signed_number (object, name, var->var_storage->uint32val);
      break;
    case HIO_CONFIG_TYPE_UINT64:
      hioi_manifest_set_signed_number (object, name, var->var_storage->uint64val);
      break;
    case HIO_CONFIG_TYPE_BOOL:
      hioi_manifest_set_string (object, name, var->var_storage->boolval ? "true" : "false");
      break;
    case HIO_CONFIG_TYPE_STRING:
      hioi_manifest_set_string (object, name, var->var_storage->strval);
      break;
    case HIO_CONFIG_TYPE_FLOAT:
      hioi_manifest_set_double (object, name, var->var_storage->doubleval);
      break;
    case HIO_CONFIG_TYPE_DOUBLE:
      hioi_manifest_set_double (object, name, var->var_storage->floatval);
      break;
    }
  }

  return HIO_SUCCESS;
}

static json_object *hio_manifest_generate_simple_3_0 (hio_dataset_t dataset) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  hio_object_t hio_object = &dataset->ds_object;
  json_object *top;
  int rc;

  top = hio_manifest_generate_empty_3_0 ();
  if (NULL == top) {
    return NULL;
  }

  hioi_manifest_set_string (top, HIO_MANIFEST_KEY_IDENTIFIER, hio_object->identifier);
  hioi_manifest_set_number (top, HIO_MANIFEST_KEY_DATASET_ID, (unsigned long) dataset->ds_id);

  if (HIO_SET_ELEMENT_UNIQUE == dataset->ds_mode) {
    hioi_manifest_set_string (top, HIO_MANIFEST_KEY_DATASET_MODE, "unique");
  } else {
    hioi_manifest_set_string (top, HIO_MANIFEST_KEY_DATASET_MODE, "shared");
  }

  rc = hioi_manifest_save_vars (top, HIO_MANIFEST_KEY_CONFIG, &dataset->ds_object.configuration);
  if (HIO_SUCCESS != rc) {
    json_object_put (top);
    return NULL;
  }

  rc = hioi_manifest_save_vars (top, HIO_MANIFEST_KEY_PERF, &dataset->ds_object.performance);
  if (HIO_SUCCESS != rc) {
    json_object_put (top);
    return NULL;
  }

  hioi_manifest_set_number (top, HIO_MANIFEST_KEY_COMM_SIZE, (unsigned long) context->c_size);
  hioi_manifest_set_signed_number (top, HIO_MANIFEST_KEY_STATUS, (long) dataset->ds_status);
  hioi_manifest_set_number (top, HIO_MANIFEST_KEY_MTIME, (unsigned long) time (NULL));

  return top;
}

/**
 * @brief Generate a json manifest from an hio dataset
 *
 * @param[in] dataset hio dataset handle
 *
 * @returns json object representing the dataset's manifest
 */
static json_object *hio_manifest_generate_3_0 (hio_dataset_t dataset) {
  json_object *elements, *top;
  hio_element_t element;

  top = hio_manifest_generate_simple_3_0 (dataset);
  if (NULL == top || 0 == hioi_list_length (&dataset->ds_elist)) {
    /* nothing more to write */
    return top;
  }

  elements = hio_manifest_new_array (top, HIO_MANIFEST_KEY_ELEMENTS);
  if (NULL == elements) {
    json_object_put (top);
    return NULL;
  }

  hioi_list_foreach(element, dataset->ds_elist, struct hio_element, e_list) {
    json_object *element_object = json_object_new_object ();
    if (NULL == element_object) {
      json_object_put (top);
      return NULL;
    }

    hioi_manifest_set_string (element_object, HIO_MANIFEST_KEY_IDENTIFIER, element->e_object.identifier);
    hioi_manifest_set_number (element_object, HIO_MANIFEST_KEY_SIZE, (unsigned long) element->e_size);
    if (HIO_SET_ELEMENT_UNIQUE == dataset->ds_mode) {
      hioi_manifest_set_number (element_object, HIO_MANIFEST_KEY_RANK, (unsigned long) element->e_rank);
    }

    json_object_array_add (elements, element_object);

    if (element->e_scount) {
      json_object *segments_object = hio_manifest_new_array (element_object, HIO_MANIFEST_KEY_SEGMENTS);
      if (NULL == segments_object) {
        json_object_put (top);
        return NULL;
      }

      for (int i = 0 ; i < element->e_scount ; ++i) {
        json_object *segment_object = json_object_new_object ();
        hio_manifest_segment_t *segment = element->e_sarray + i;
        if (NULL == segment_object) {
          json_object_put (top);
          return NULL;
        }

        hioi_manifest_set_number (segment_object, HIO_SEGMENT_KEY_APP_OFFSET0,
                                  (unsigned long) segment->seg_offset);
        hioi_manifest_set_number (segment_object, HIO_SEGMENT_KEY_FILE_OFFSET,
                                  (unsigned long) segment->seg_foffset);
        hioi_manifest_set_number (segment_object, HIO_SEGMENT_KEY_LENGTH,
                                  (unsigned long) segment->seg_length);
        hioi_manifest_set_number (segment_object, HIO_SEGMENT_KEY_FILE_INDEX,
                                  (unsigned long) segment->seg_file_index);
        json_object_array_add (segments_object, segment_object);
      }
    }
  }

  return top;
}

/**
 * @brief Serialize a json object
 *
 * @param[in] json_object json object pointer
 * @param[out] data serialized data out
 * @param[out] data_size length of serialized data
 * @param[in] compress_data whether to compress the serialized data
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_OUT_OF_RESOURCE if run out of memory
 */
static int hioi_manifest_serialize_json (json_object *json_object, unsigned char **data, size_t *data_size,
                                         bool compress_data) {
  const char *serialized;
  unsigned int serialized_len;
  int rc;

  serialized = json_object_to_json_string (json_object);
  serialized_len = strlen (serialized) + 1;
  if (compress_data) {
    unsigned int compressed_size = serialized_len;
    char *tmp;

    tmp = malloc (serialized_len);
    if (NULL == tmp) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    rc = BZ2_bzBuffToBuffCompress (tmp, &compressed_size, (char *) serialized, serialized_len, 3, 0, 0);
    if (BZ_OK != rc) {
      free (tmp);
      return HIO_ERROR;
    }

    *data = realloc (tmp, compressed_size);
    if (NULL == *data) {
      free (tmp);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    *data_size = compressed_size;
  } else {
    *data_size = serialized_len;

    *data = calloc (*data_size, 1);
    if (NULL == *data) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    memcpy (*data, serialized, *data_size - 1);
  }

  return HIO_SUCCESS;
}

void hioi_manifest_release (hio_manifest_t manifest) {
  if (NULL != manifest) {
    json_object_put (manifest->json_object);
    free (manifest);
  }
}

int hioi_manifest_serialize (hio_manifest_t manifest, unsigned char **data, size_t *data_size, bool compress_data) {
  return hioi_manifest_serialize_json (manifest->json_object, data, data_size, compress_data);
}

int hioi_manifest_save (hio_manifest_t manifest, bool compress_data, const char *path) {
  unsigned char *data;
  size_t data_size;
  int rc;

  rc = hioi_manifest_serialize_json (manifest->json_object, &data, &data_size, compress_data);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  errno = 0;
  int fd = open (path, O_WRONLY | O_CREAT, 0644);
  if (0 > fd) {
    free (data);
    return hioi_err_errno (errno);
  }

  errno = 0;
  rc = write (fd, data, data_size);
  free (data);
  close (fd);

  if (0 > rc) {
    return hioi_err_errno (errno);
  }

  return rc == data_size ? HIO_SUCCESS : HIO_ERR_TRUNCATE;
}

static int hioi_manifest_parse_segment_2_1 (hio_element_t element, json_object *segment_object) {
  unsigned long file_offset, app_offset0, length, file_index;
  int rc;

  rc = hioi_manifest_get_number (segment_object, HIO_SEGMENT_KEY_FILE_OFFSET, &file_offset);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = hioi_manifest_get_number (segment_object, HIO_SEGMENT_KEY_APP_OFFSET0, &app_offset0);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = hioi_manifest_get_number (segment_object, HIO_SEGMENT_KEY_LENGTH, &length);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = hioi_manifest_get_number (segment_object, HIO_SEGMENT_KEY_FILE_INDEX, &file_index);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERROR, &element->e_object, "Manfest segment missing file_index property");
    return rc;
  }

  return hioi_element_add_segment (element, file_index, file_offset, app_offset0, length);
}

static int hioi_manifest_parse_segments_2_1 (hio_element_t element, json_object *object) {
  hio_context_t context = hioi_object_context (&element->e_object);
  int segment_count = json_object_array_length (object);

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "parsing %d segments in element %s", segment_count,
            hioi_object_identifier (&element->e_object));

  for (int i = 0 ; i < segment_count ; ++i) {
    json_object *segment_object = json_object_array_get_idx (object, i);
    int rc = hioi_manifest_parse_segment_2_1 (element, segment_object);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_element_2_0 (hio_dataset_t dataset, json_object *element_object) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  hio_element_t element = NULL;
  json_object *segments_object;
  bool new_element = true;
  const char *tmp_string;
  unsigned long value;
  int rc, rank;

  rc = hioi_manifest_get_string (element_object, HIO_MANIFEST_KEY_IDENTIFIER, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERROR, &dataset->ds_object, "manifest element missing identifier property");
    return HIO_ERROR;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "parsing manifest element: %s", tmp_string);

  if (HIO_SET_ELEMENT_UNIQUE == dataset->ds_mode) {
    rc = hioi_manifest_get_number (element_object, HIO_MANIFEST_KEY_RANK, &value);
    if (HIO_SUCCESS != rc) {
      hioi_object_release (&element->e_object);
      return HIO_ERR_BAD_PARAM;
    }

    if (value != context->c_rank) {
      /* nothing to do */
      return HIO_SUCCESS;
    }

    rank = value;
  } else {
    rank = -1;
  }

  hioi_list_foreach (element, dataset->ds_elist, struct hio_element, e_list) {
    if (!strcmp (hioi_object_identifier(element), tmp_string) && rank == element->e_rank) {
      new_element = false;
      break;
    }
  }

  if (new_element) {
    element = hioi_element_alloc (dataset, (const char *) tmp_string, rank);
    if (NULL == element) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }
  }

  rc = hioi_manifest_get_number (element_object, HIO_MANIFEST_KEY_SIZE, &value);
  if (HIO_SUCCESS != rc) {
    hioi_object_release (&element->e_object);
    return HIO_ERR_BAD_PARAM;
  }

  if (dataset->ds_mode == HIO_SET_ELEMENT_UNIQUE || value > element->e_size) {
    element->e_size = value;
  }

  segments_object = hioi_manifest_find_object (element_object, HIO_MANIFEST_KEY_SEGMENTS);
  if (NULL != segments_object) {
    rc = hioi_manifest_parse_segments_2_1 (element, segments_object);
    if (HIO_SUCCESS != rc) {
      hioi_object_release (&element->e_object);
      return rc;
    }
  }

  if (new_element) {
    hioi_dataset_add_element (dataset, element);
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "found element with identifier %s in manifest",
	    element->e_object.identifier);

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_elements_2_0 (hio_dataset_t dataset, json_object *object) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  int element_count = json_object_array_length (object);

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "parsing %d elements in manifest", element_count);
  for (int i = 0 ; i < element_count ; ++i) {
    json_object *element_object = json_object_array_get_idx (object, i);
    int rc = hioi_manifest_parse_element_2_0 (dataset, element_object);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_2_0 (hio_dataset_t dataset, json_object *object) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  json_object *elements_object;
  unsigned long mode = 0, size;
  const char *tmp_string;
  long status;
  int rc;

  /* check for compatibility with this manifest version */
  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_COMPAT, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, &dataset->ds_object, "manifest missing required %s key",
                   HIO_MANIFEST_KEY_COMPAT);
    return rc;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "compatibility version of manifest: %s",
            (char *) tmp_string);

  if (strcmp ((char *) tmp_string, "2.0")) {
    /* incompatible version */
    return HIO_ERROR;
  }

  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_DATASET_MODE, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, &dataset->ds_object, "manifest missing required %s key",
                   HIO_MANIFEST_KEY_DATASET_MODE);
    return rc;
  }

  if (0 == strcmp (tmp_string, "unique")) {
    mode = HIO_SET_ELEMENT_UNIQUE;
  } else if (0 == strcmp (tmp_string, "shared")) {
    mode = HIO_SET_ELEMENT_SHARED;
  } else {
    hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object,
                   "unknown dataset mode specified in manifest: %s", (const char *) tmp_string);
    return HIO_ERR_BAD_PARAM;
  }

  if (mode != dataset->ds_mode) {
    hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object,
                   "mismatch in dataset mode. requested: %d, actual: %d", mode,
                   dataset->ds_mode);
    return HIO_ERR_BAD_PARAM;
  }

  if (HIO_SET_ELEMENT_UNIQUE == mode) {
    /* verify that the same number of ranks are in use */
    rc = hioi_manifest_get_number (object, HIO_MANIFEST_KEY_COMM_SIZE, &size);
    if (HIO_SUCCESS != rc) {
      hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "manifest missing required %s key",
                     HIO_MANIFEST_KEY_COMM_SIZE);
      return HIO_ERR_BAD_PARAM;
    }

    if (size != context->c_size) {
      hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "communicator size %d does not match dataset %d",
                     context->c_size, size);
      return HIO_ERR_BAD_PARAM;
    }
  }

  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_FILE_MODE, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "file mode was not specified in manifest");
    return HIO_ERR_BAD_PARAM;
  }

  rc = hio_config_set_value (&dataset->ds_object, "dataset_file_mode", (const char *) tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "bad file mode: %s", tmp_string);
    return HIO_ERR_BAD_PARAM;
  }

  rc = hioi_manifest_get_signed_number (object, HIO_MANIFEST_KEY_STATUS, &status);
  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  dataset->ds_status = status;

  /* find and parse all elements covered by this manifest */
  elements_object = hioi_manifest_find_object (object, HIO_MANIFEST_KEY_ELEMENTS);
  if (NULL == elements_object) {
    /* no elements in this file */
    return HIO_SUCCESS;
  }

  return hioi_manifest_parse_elements_2_0 (dataset, elements_object);
}

static int hioi_manifest_parse_3_0 (hio_dataset_t dataset, json_object *object) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  json_object *elements_object, *config, *perf;
  unsigned long mode = 0, size;
  const char *tmp_string;
  long status;
  int rc;

  /* check for compatibility with this manifest version */
  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_COMPAT, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, &dataset->ds_object, "manifest missing required %s key",
                   HIO_MANIFEST_KEY_COMPAT);
    return rc;
  }

  if (strcmp ((char *) tmp_string, "3.0")) {
    /* incompatible version */
    return hioi_manifest_parse_2_0 (dataset, object);
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "compatibility version of manifest: %s",
            (char *) tmp_string);

  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_DATASET_MODE, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, &dataset->ds_object, "manifest missing required %s key",
                   HIO_MANIFEST_KEY_DATASET_MODE);
    return rc;
  }

  if (0 == strcmp (tmp_string, "unique")) {
    mode = HIO_SET_ELEMENT_UNIQUE;
  } else if (0 == strcmp (tmp_string, "shared")) {
    mode = HIO_SET_ELEMENT_SHARED;
  } else {
    hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object,
                   "unknown dataset mode specified in manifest: %s", (const char *) tmp_string);
    return HIO_ERR_BAD_PARAM;
  }

  if (mode != dataset->ds_mode) {
    hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object,
                   "mismatch in dataset mode. requested: %d, actual: %d", mode,
                   dataset->ds_mode);
    return HIO_ERR_BAD_PARAM;
  }

  if (HIO_SET_ELEMENT_UNIQUE == mode) {
    /* verify that the same number of ranks are in use */
    rc = hioi_manifest_get_number (object, HIO_MANIFEST_KEY_COMM_SIZE, &size);
    if (HIO_SUCCESS != rc) {
      hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "manifest missing required %s key",
                     HIO_MANIFEST_KEY_COMM_SIZE);
      return HIO_ERR_BAD_PARAM;
    }

    if (size != context->c_size) {
      hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "communicator size %d does not match dataset %d",
                     context->c_size, size);
      return HIO_ERR_BAD_PARAM;
    }
  }


  config = hioi_manifest_find_object (object, HIO_MANIFEST_KEY_CONFIG);
  if (NULL != config) {
    json_object_object_foreach (config, key, value) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "found manifest configuration key %s", key);
      (void) hioi_config_set_value (&dataset->ds_object, key, json_object_get_string (value));
    }
  }

  perf = hioi_manifest_find_object (object, HIO_MANIFEST_KEY_PERF);
  if (NULL != perf) {
    json_object_object_foreach (perf, key, value) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "found manifest performance key %s", key);
      (void) hioi_perf_set_value (&dataset->ds_object, key, json_object_get_string (value));
    }
  }

  rc = hioi_manifest_get_signed_number (object, HIO_MANIFEST_KEY_STATUS, &status);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "manifest status key missing");
    return HIO_ERR_BAD_PARAM;
  }

  dataset->ds_status = status;

  /* find and parse all elements covered by this manifest */
  elements_object = hioi_manifest_find_object (object, HIO_MANIFEST_KEY_ELEMENTS);
  if (NULL == elements_object) {
    /* no elements in this file */
    return HIO_SUCCESS;
  }

  return hioi_manifest_parse_elements_2_0 (dataset, elements_object);
}

static int hioi_manifest_parse_header_2_0 (hio_context_t context, hio_dataset_header_t *header, json_object *object) {
  const char *tmp_string;
  unsigned long value;
  long svalue;
  int rc;

  /* check for compatibility with this manifest version */
  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_COMPAT, &tmp_string);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "compatibility version of manifest: %s", (char *) tmp_string);

  if (strcmp (tmp_string, "2.0") && strcmp (tmp_string, "3.0")) {
    /* incompatible version */
    return HIO_ERROR;
  }

  /* fill in header */
  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_DATASET_MODE, &tmp_string);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  if (0 == strcmp (tmp_string, "unique")) {
    value = HIO_SET_ELEMENT_UNIQUE;
  } else if (0 == strcmp (tmp_string, "shared")) {
    value = HIO_SET_ELEMENT_SHARED;
  } else {
    hioi_err_push (HIO_ERR_BAD_PARAM, &context->c_object, "unknown dataset mode specified in manifest: "
                  "%s", tmp_string);
    return HIO_ERR_BAD_PARAM;
  }

  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_IDENTIFIER, &tmp_string);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  if (strlen (tmp_string) >= HIO_DATASET_NAME_MAX) {
    /* dataset name is too long */
    return HIO_ERR_BAD_PARAM;
  }

  strncpy (header->ds_name, tmp_string, sizeof (header->ds_name));

  header->ds_mode = value;

  rc = hioi_manifest_get_signed_number (object, HIO_MANIFEST_KEY_STATUS, &svalue);

  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  header->ds_status = svalue;

  rc = hioi_manifest_get_number (object, HIO_MANIFEST_KEY_MTIME, &value);

  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  header->ds_mtime = value;

  rc = hioi_manifest_get_number (object, HIO_MANIFEST_KEY_DATASET_ID, &value);

  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  header->ds_id = value;

  return HIO_SUCCESS;
}

static int hioi_manifest_decompress (unsigned char **data, size_t data_size) {
  const size_t increment = 8192;
  char *uncompressed, *tmp;
  bz_stream strm;
  size_t size;
  int rc;

  uncompressed = malloc (increment);
  if (NULL == uncompressed) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  strm.bzalloc = NULL;
  strm.bzfree = NULL;
  strm.opaque = NULL;
  strm.next_in = (char *) *data;
  strm.avail_in = data_size;
  strm.next_out = uncompressed;
  strm.avail_out = size = increment;

  BZ2_bzDecompressInit (&strm, 0, 0);

  do {
    rc = BZ2_bzDecompress (&strm);
    if (BZ_OK != rc) {
      BZ2_bzDecompressEnd (&strm);
      if (BZ_STREAM_END == rc) {
        break;
      }
      free (uncompressed);
      return HIO_ERROR;
    }

    tmp = realloc (uncompressed, size + increment);
    if (NULL == tmp) {
      BZ2_bzDecompressEnd (&strm);
      free (uncompressed);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    uncompressed = tmp;

    strm.next_out = uncompressed + size;
    strm.avail_out = increment;
    size += increment;
  } while (1);

  *data = (unsigned char *) uncompressed;
  return HIO_SUCCESS;
}

int hioi_manifest_generate (hio_dataset_t dataset, bool simple, hio_manifest_t *manifest_out) {
  hio_manifest_t manifest;

  manifest = calloc (1, sizeof (*manifest));
  if (NULL == manifest) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  if (simple) {
    manifest->json_object = hio_manifest_generate_simple_3_0 (dataset);
  } else {
    manifest->json_object = hio_manifest_generate_3_0 (dataset);
  }

  manifest->context = hioi_object_context (&dataset->ds_object);

  *manifest_out = manifest;

  return HIO_SUCCESS;
}

hio_manifest_t hioi_manifest_create (hio_context_t context) {
  hio_manifest_t manifest;

  manifest = calloc (1, sizeof (*manifest));
  if (NULL == manifest) {
    return NULL;
  }

  manifest->context = context;
  manifest->json_object = hio_manifest_generate_empty_3_0 ();

  return manifest;
}

int hioi_manifest_deserialize (hio_context_t context, const unsigned char *data, const size_t data_size, hio_manifest_t *manifest_out) {
  unsigned char *tmp = NULL;
  hio_manifest_t manifest;
  json_object *object;
  int rc;

  if (data_size < 2 || NULL == data) {
    return HIO_ERR_BAD_PARAM;
  }

  manifest = calloc (1, sizeof (*manifest));
  if (NULL == manifest) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  if ('B' == data[0] && 'Z' == data[1]) {
    tmp = (unsigned char *) data;
    /* gz compressed */
    rc = hioi_manifest_decompress ((unsigned char **) &tmp, data_size);
    if (HIO_SUCCESS != rc) {
      free (manifest);
      return rc;
    }

    data = tmp;
  }

  object = json_tokener_parse ((char *) data);
  free (tmp);
  if (NULL == object) {
    free (manifest);
    return HIO_ERROR;
  }

  manifest->context = context;
  manifest->json_object = object;

  *manifest_out = manifest;

  return HIO_SUCCESS;
}

int hioi_manifest_read (hio_context_t context, const char *path, hio_manifest_t *manifest_out) {
  unsigned char *buffer = NULL;
  size_t file_size;
  FILE *fh;
  int rc;

  if (access (path, F_OK)) {
    return HIO_ERR_NOT_FOUND;
  }

  if (access (path, R_OK)) {
    return HIO_ERR_PERM;
  }

  fh = fopen (path, "r");
  if (NULL == fh) {
    return hioi_err_errno (errno);
  }

  (void) fseek (fh, 0, SEEK_END);
  file_size = ftell (fh);
  (void) fseek (fh, 0, SEEK_SET);

  if (0 == file_size) {
    fclose (fh);
    return HIO_ERR_BAD_PARAM;
  }

  buffer = malloc (file_size);
  if (NULL == buffer) {
    fclose (fh);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  rc = fread (buffer, 1, file_size, fh);
  fclose (fh);
  if (file_size != rc) {
    free (buffer);
    return HIO_ERROR;
  }

  rc = hioi_manifest_deserialize (context, buffer, file_size, manifest_out);
  free (buffer);

  return rc;
}

int hioi_manifest_load (hio_dataset_t dataset, hio_manifest_t manifest) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Loading dataset manifest on %s:%" PRIu64,
            dataset->ds_object.identifier, dataset->ds_id);

  return hioi_manifest_parse_3_0 (dataset, manifest->json_object);
}


static bool hioi_manifest_compare_json (json_object *object1, json_object *object2, const char *key) {
  const char *value1, *value2;
  int rc;

  rc = hioi_manifest_get_string (object1, key, &value1);
  if (HIO_SUCCESS != rc) {
    return false;
  }

  rc = hioi_manifest_get_string (object2, key, &value2);
  if (HIO_SUCCESS != rc) {
    return false;
  }

  return 0 == strcmp (value1, value2);
}

static int hioi_manifest_array_find_matching (json_object *array, json_object *object, const char *key) {
  int array_size = json_object_array_length (array);
  const char *value = NULL;
  int rc;

  if (NULL != key) {
    rc = hioi_manifest_get_string (object, key, &value);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  } else {
    value = json_object_get_string (object);
  }

  for (int i = 0 ; i < array_size ; ++i) {
    json_object *array_object = json_object_array_get_idx (array, i);
    const char *value1 = NULL;

    assert (array_object);
    if (NULL != key) {
      rc = hioi_manifest_get_string (array_object, key, &value1);
      if (HIO_SUCCESS != rc) {
        return rc;
      }
    } else {
      value1 = json_object_get_string (array_object);
    }

    if (0 == strcmp (value, value1)) {
      return i;
    }
  }

  return HIO_ERR_NOT_FOUND;
}

static int segment_compare (const void *arg1, const void *arg2) {
  json_object * const *segment1 = (json_object * const *) arg1;
  json_object * const *segment2 = (json_object * const *) arg2;
  unsigned long base1 = 0, base2 = 0;

  (void) hioi_manifest_get_number (*segment1, HIO_SEGMENT_KEY_APP_OFFSET0, &base1);
  (void) hioi_manifest_get_number (*segment2, HIO_SEGMENT_KEY_APP_OFFSET0, &base2);

  if (base1 > base2) {
    return 1;
  }

  return (base1 < base2) ? -1 : 0;
}

static int hioi_manifest_merge_internal (json_object *object1, json_object *object2) {
  json_object *elements1, *elements2;
  int rc, manifest_mode;
  const char *tmp_string;

  /* sanity check. make sure the manifest meta-data matches */
  if (!hioi_manifest_compare_json (object1, object2, HIO_MANIFEST_KEY_DATASET_MODE) ||
      !hioi_manifest_compare_json (object1, object2, HIO_MANIFEST_KEY_HIO_VERSION) ||
      !hioi_manifest_compare_json (object1, object2, HIO_MANIFEST_KEY_DATASET_ID)) {
    return HIO_ERR_BAD_PARAM;
  }

  rc = hioi_manifest_get_string (object1, HIO_MANIFEST_KEY_DATASET_MODE, &tmp_string);
  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  if (0 == strcmp (tmp_string, "unique")) {
    manifest_mode = HIO_SET_ELEMENT_UNIQUE;
  } else if (0 == strcmp (tmp_string, "shared")) {
    manifest_mode = HIO_SET_ELEMENT_SHARED;
  } else {
    return HIO_ERR_BAD_PARAM;
  }

  elements1 = hioi_manifest_find_object (object1, HIO_MANIFEST_KEY_ELEMENTS);
  elements2 = hioi_manifest_find_object (object2, HIO_MANIFEST_KEY_ELEMENTS);

  if (NULL == elements1 && NULL != elements2) {
    /* move the array from object2 to object. the reference count needs to be
     * incremented before it is deleted to ensure it is not freed prematurely and
     * the reference count remains correct after it is added to object1. */
    json_object_get (elements2);
    json_object_object_del (object2, HIO_MANIFEST_KEY_ELEMENTS);
    json_object_object_add (object1, HIO_MANIFEST_KEY_ELEMENTS, elements2);
    elements1 = elements2;
    elements2 = NULL;
  }

  /* check if elements need to be merged */
  if (NULL != elements2) {
    int elements2_count = json_object_array_length (elements2);

    for (int i = 0 ; i < elements2_count ; ++i) {
      json_object *segments, *element = json_object_array_get_idx (elements2, i);
      int segment_count = 0;

      assert (NULL != element);

      segments = hioi_manifest_find_object (element, HIO_MANIFEST_KEY_SEGMENTS);
      if (NULL != segments) {
        segment_count = json_object_array_length (segments);
      }

      if (HIO_SET_ELEMENT_UNIQUE != manifest_mode) {
        /* check if this element already exists */
        rc = hioi_manifest_array_find_matching (elements1, element, HIO_MANIFEST_KEY_IDENTIFIER);
      } else {
        /* assuming the manifests are from different ranks for now.. I may changet this
         * in the future. */
        rc = HIO_ERR_NOT_FOUND;
      }

      if (0 <= rc) {
        json_object *element1 = json_object_array_get_idx (elements1, rc);
        json_object *segments1 = hioi_manifest_find_object (element1, HIO_MANIFEST_KEY_SEGMENTS);
        unsigned long element_size = 0, element1_size = 0;

        /* remove the segments from the element in the object2. get a reference first
         * so the array doesn't get freed before we are done with it */
        json_object_get (segments);
        json_object_object_del (element, HIO_MANIFEST_KEY_SEGMENTS);

        if (NULL != segments1) {
          for (int j = 0 ; j < segment_count ; ++j) {
            json_object *segment = json_object_array_get_idx (segments, j);
            json_object_get (segment);
            json_object_array_add (segments1, segment);
          }

          json_object_put (segments);
          /* re-sort the segment array by base pointer */
          json_object_array_sort (segments1, segment_compare);
        } else {
          json_object_object_add (element1, HIO_MANIFEST_KEY_SEGMENTS, segments);
        }

        (void) hioi_manifest_get_number (element, HIO_MANIFEST_KEY_SIZE, &element_size);
        (void) hioi_manifest_get_number (element1, HIO_MANIFEST_KEY_SIZE, &element1_size);
        if (element_size > element1_size) {
          /* use the larger of the two sizes */
          json_object_object_del (element1, HIO_MANIFEST_KEY_SIZE);
          hioi_manifest_set_number (element1, HIO_MANIFEST_KEY_SIZE, element_size);
        }
      } else {
        json_object_get (element);
        json_object_array_add (elements1, element);
      }

    }

    /* remove the elements array from object2 */
    json_object_object_del (object2, HIO_MANIFEST_KEY_ELEMENTS);
  }

  return HIO_SUCCESS;
}

int hioi_manifest_merge_data (hio_manifest_t manifest, hio_manifest_t manifest2) {
  if (NULL == manifest || NULL == manifest2) {
    return HIO_ERR_BAD_PARAM;
  }

  return hioi_manifest_merge_internal (manifest->json_object, manifest2->json_object);
}

static int rank_compare (const void *a, const void *b) {
  return (((const int *) a)[0] - ((const int *) b)[0]);
}

int hioi_manifest_ranks (hio_manifest_t manifest, int **ranks, int *rank_count) {
  json_object *object = NULL, *elements;
  int manifest_ranks = 0, element_count;
  unsigned char *rank_map = NULL;
  int *rank_list;
  int rc = HIO_SUCCESS;
  unsigned long size;

  *ranks = NULL;
  *rank_count = 0;

  object = manifest->json_object;
  elements = hioi_manifest_find_object (object, HIO_MANIFEST_KEY_ELEMENTS);
  element_count = elements ? json_object_array_length (elements) : 0;
  if (0 == element_count) {
    /* no elements */
    return HIO_SUCCESS;
  }

  /* find out how many ranks were used to write the manifest */
  rc = hioi_manifest_get_number (object, HIO_MANIFEST_KEY_COMM_SIZE, &size);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  /* allocate enough space to hold the maximum number of ranks that
   * can appear in the manifest. */
  rank_map = calloc (1, (size >> 3) + 1);
  rank_list = (int *) calloc (element_count, sizeof (ranks[0][0]));
  if (NULL == rank_map || NULL == rank_list) {
    free (rank_map);
    free (rank_list);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  for (int i = 0 ; i < element_count ; ++i) {
    json_object *element = json_object_array_get_idx (elements, i);
    unsigned long rank;

    assert (NULL != element);

    rc = hioi_manifest_get_number (element, HIO_MANIFEST_KEY_RANK, &rank);
    if (HIO_SUCCESS != rc) {
      free (rank_map);
      free (rank_list);
      return rc;
    }

    if (!(rank_map[rank >> 3] & (1 << (rank & 0x7)))) {
      rank_list[manifest_ranks++] = (int) rank;
      rank_map[rank >> 3] |= 1 << (rank & 0x7);
    }
  }

  if (0 == manifest_ranks) {
    free (rank_list);
    free (rank_map);
    return HIO_ERR_BAD_PARAM;
  }

  /* shrink the array if there were fewer ranks than elements */
  void *tmp = realloc (rank_list, sizeof (ranks[0][0]) * manifest_ranks);
  if (NULL == tmp) {
    free (rank_list);
    free (rank_map);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  *ranks = (int *) tmp;
  *rank_count = manifest_ranks;

  /* sort the rank array by rank */
  (void) qsort (*ranks, manifest_ranks, sizeof (int), rank_compare);

  free (rank_map);

  return rc;
}

int hioi_manifest_read_header (hio_manifest_t manifest, hio_dataset_header_t *header) {
  if (NULL == manifest) {
    return HIO_ERR_BAD_PARAM;
  }

  return hioi_manifest_parse_header_2_0 (manifest->context, header, manifest->json_object);
}
