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

#include "hio_internal.h"

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

#define HIO_MANIFEST_PROP_VERSION     "hio_manifest_version"
#define HIO_MANIFEST_PROP_COMPAT      "hio_manifest_compat"
#define HIO_MANIFEST_PROP_IDENTIFIER  "identifier"
#define HIO_MANIFEST_PROP_DATASET_ID  "dataset_id"
#define HIO_MANIFEST_PROP_SIZE        "size"
#define HIO_MANIFEST_PROP_HIO_VERSION "hio_version"
#define HIO_MANIFEST_PROP_RANK        "rank"

#define HIO_MANIFEST_KEY_DATASET_MODE "hio_dataset_mode"
#define HIO_MANIFEST_KEY_FILE_MODE    "hio_file_mode"
#define HIO_MANIFEST_KEY_BLOCK_SIZE   "block_size"
#define HIO_MANIFEST_KEY_FILE_COUNT   "file_count"
#define HIO_MANIFEST_KEY_MTIME        "hio_mtime"
#define HIO_MANIFEST_KEY_COMM_SIZE    "hio_comm_size"
#define HIO_MANIFEST_KEY_STATUS       "hio_status"
#define HIO_SEGMENT_KEY_FILE_OFFSET   "loff"
#define HIO_SEGMENT_KEY_APP_OFFSET0   "off"
#define HIO_SEGMENT_KEY_LENGTH        "len"
#define HIO_SEGMENT_KEY_FILE_INDEX    "findex"

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

static json_object *hio_manifest_new_array (json_object *parent, const char *name) {
  json_object *new_object = json_object_new_array ();

  if (NULL == new_object) {
    return NULL;
  }

  json_object_object_add (parent, name, new_object);

  return new_object;
}

static json_object *hioi_manifest_find_object (json_object *parent, const char *name) {
  json_object *object;

  if (json_object_object_get_ex (parent, name, &object)) {
    return object;
  }

  return NULL;
}

static int hioi_manifest_get_string (json_object *parent, const char *name, const char **string) {
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

static int hioi_manifest_get_number (json_object *parent, const char *name, unsigned long *value) {
  json_object *object;

  object = hioi_manifest_find_object (parent, name);
  if (NULL == object) {
    return HIO_ERR_NOT_FOUND;
  }

  *value = (unsigned long) json_object_get_int64 (object);

  return HIO_SUCCESS;
}

static int hioi_manifest_get_signed_number (json_object *parent, const char *name, long *value) {
  json_object *object;

  object = hioi_manifest_find_object (parent, name);
  if (NULL == object) {
    return HIO_ERR_NOT_FOUND;
  }

  *value = (long) json_object_get_int64 (object);

  return HIO_SUCCESS;
}

static json_object *hio_manifest_generate_simple_3_0 (hio_dataset_t dataset) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  hio_object_t hio_object = &dataset->ds_object;
  json_object *top, *config;
  int rc, config_count;

  top = json_object_new_object ();
  if (NULL == top) {
    return NULL;
  }

  hioi_manifest_set_string (top, HIO_MANIFEST_PROP_VERSION, HIO_MANIFEST_VERSION);
  hioi_manifest_set_string (top, HIO_MANIFEST_PROP_COMPAT, HIO_MANIFEST_COMPAT);
  hioi_manifest_set_string (top, HIO_MANIFEST_PROP_HIO_VERSION, PACKAGE_VERSION);
  hioi_manifest_set_string (top, HIO_MANIFEST_PROP_IDENTIFIER, hio_object->identifier);
  hioi_manifest_set_number (top, HIO_MANIFEST_PROP_DATASET_ID, (unsigned long) dataset->ds_id);

  if (HIO_SET_ELEMENT_UNIQUE == dataset->ds_mode) {
    hioi_manifest_set_string (top, HIO_MANIFEST_KEY_DATASET_MODE, "unique");
  } else {
    hioi_manifest_set_string (top, HIO_MANIFEST_KEY_DATASET_MODE, "shared");
  }

  config = json_object_new_object ();
  assert (NULL != config);

  json_object_object_add (top, "config", config);


  rc = hio_config_get_count (&dataset->ds_object, &config_count);
  assert (HIO_SUCCESS == rc);

  for (int i = 0 ; i < config_count ; ++i) {
    hio_config_type_t type;
    char *name, *value;
    uint64_t usint;
    int64_t sint;

    rc = hioi_config_get_info (&dataset->ds_object, i, &name, &type, NULL);
    assert (HIO_SUCCESS == rc);

    rc = hio_config_get_value (&dataset->ds_object, name, &value);
    assert (HIO_SUCCESS == rc);

    switch (type) {
    case HIO_CONFIG_TYPE_INT32:
    case HIO_CONFIG_TYPE_INT64:
      sint = strtol (value, NULL, 0);
      hioi_manifest_set_signed_number (config, name, sint);
      break;
    case HIO_CONFIG_TYPE_UINT32:
    case HIO_CONFIG_TYPE_UINT64:
      usint = strtoul (value, NULL, 0);
      hioi_manifest_set_number (config, name, usint);
      break;
    case HIO_CONFIG_TYPE_BOOL:
    case HIO_CONFIG_TYPE_STRING:
      hioi_manifest_set_string (config, name, value);
      break;
    default:
      /* ignore other types for now */
      break;
    }

    free (value);
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

  elements = hio_manifest_new_array (top, "elements");
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

    hioi_manifest_set_string (element_object, HIO_MANIFEST_PROP_IDENTIFIER, element->e_object.identifier);
    hioi_manifest_set_number (element_object, HIO_MANIFEST_PROP_SIZE, (unsigned long) element->e_size);
    if (HIO_SET_ELEMENT_UNIQUE == dataset->ds_mode) {
      hioi_manifest_set_number (element_object, HIO_MANIFEST_PROP_RANK, (unsigned long) element->e_rank);
    }

    json_object_array_add (elements, element_object);

    if (element->e_scount) {
      json_object *segments_object = hio_manifest_new_array (element_object, "segments");
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

        hioi_manifest_set_number (segment_object, HIO_SEGMENT_KEY_FILE_OFFSET,
                                  (unsigned long) segment->seg_foffset);
        hioi_manifest_set_number (segment_object, HIO_SEGMENT_KEY_APP_OFFSET0,
                                  (unsigned long) segment->seg_offset);
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

int hioi_manifest_serialize (hio_dataset_t dataset, unsigned char **data, size_t *data_size, bool compress_data, bool simple) {
  json_object *json_object;
  int rc;

  if (simple) {
    json_object = hio_manifest_generate_simple_3_0 (dataset);
  } else {
    json_object = hio_manifest_generate_3_0 (dataset);
  }

  if (NULL == json_object) {
    return HIO_ERROR;
  }

  rc = hioi_manifest_serialize_json (json_object, data, data_size, compress_data);
  json_object_put (json_object);

  return rc;
}

int hioi_manifest_save (hio_dataset_t dataset, const unsigned char *manifest_data, size_t data_size, const char *path) {
  int rc;

  errno = 0;
  int fd = open (path, O_WRONLY | O_CREAT, 0644);
  if (0 > fd) {
    return hioi_err_errno (errno);
  }

  errno = 0;
  rc = write (fd, manifest_data, data_size);
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

  rc = hioi_manifest_get_string (element_object, HIO_MANIFEST_PROP_IDENTIFIER, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERROR, &dataset->ds_object, "manifest element missing identifier property");
    return HIO_ERROR;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "parsing manifest element: %s", tmp_string);

  if (HIO_SET_ELEMENT_UNIQUE == dataset->ds_mode) {
    rc = hioi_manifest_get_number (element_object, HIO_MANIFEST_PROP_RANK, &value);
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

  rc = hioi_manifest_get_number (element_object, HIO_MANIFEST_PROP_SIZE, &value);
  if (HIO_SUCCESS != rc) {
    hioi_object_release (&element->e_object);
    return HIO_ERR_BAD_PARAM;
  }

  if (dataset->ds_mode == HIO_SET_ELEMENT_UNIQUE || value > element->e_size) {
    element->e_size = value;
  }

  segments_object = hioi_manifest_find_object (element_object, "segments");
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
  rc = hioi_manifest_get_string (object, HIO_MANIFEST_PROP_COMPAT, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, &dataset->ds_object, "manifest missing required %s key",
                   HIO_MANIFEST_PROP_COMPAT);
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
      hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "communicator size does not match dataset",
                     HIO_MANIFEST_KEY_COMM_SIZE);
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
  elements_object = hioi_manifest_find_object (object, "elements");
  if (NULL == elements_object) {
    /* no elements in this file */
    return HIO_SUCCESS;
  }

  return hioi_manifest_parse_elements_2_0 (dataset, elements_object);
}

static int hioi_manifest_parse_3_0 (hio_dataset_t dataset, json_object *object) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  json_object *elements_object, *config;
  unsigned long mode = 0, size;
  const char *tmp_string;
  long status;
  int rc;

  /* check for compatibility with this manifest version */
  rc = hioi_manifest_get_string (object, HIO_MANIFEST_PROP_COMPAT, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, &dataset->ds_object, "manifest missing required %s key",
                   HIO_MANIFEST_PROP_COMPAT);
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
      hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "communicator size does not match dataset",
                     HIO_MANIFEST_KEY_COMM_SIZE);
      return HIO_ERR_BAD_PARAM;
    }
  }


  config = hioi_manifest_find_object (object, "config");
  if (NULL != config) {
    json_object_object_foreach (object, key, value) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "found manifest configuration key %s", key);
      rc = hio_config_set_value (&dataset->ds_object, key, json_object_get_string (value));
      if (HIO_SUCCESS != rc) {
        hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "error parsing manifest configuration key: %s", key);
      }
    }
  }

  rc = hioi_manifest_get_signed_number (object, HIO_MANIFEST_KEY_STATUS, &status);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERR_BAD_PARAM, &dataset->ds_object, "manifest status key missing");
    return HIO_ERR_BAD_PARAM;
  }

  dataset->ds_status = status;

  /* find and parse all elements covered by this manifest */
  elements_object = hioi_manifest_find_object (object, "elements");
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
  rc = hioi_manifest_get_string (object, HIO_MANIFEST_PROP_COMPAT, &tmp_string);
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

  rc = hioi_manifest_get_number (object, HIO_MANIFEST_PROP_DATASET_ID, &value);

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

int hioi_manifest_deserialize (hio_dataset_t dataset, const unsigned char *data, size_t data_size) {
  bool free_data = false;
  json_object *object;
  int rc;

  if (data_size < 2) {
    return HIO_ERR_BAD_PARAM;
  }

  if ('B' == data[0] && 'Z' == data[1]) {
    /* gz compressed */
    rc = hioi_manifest_decompress ((unsigned char **) &data, data_size);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    free_data = true;
  }

  object = json_tokener_parse ((char *) data);
  if (NULL == object) {
    if (free_data) {
      free ((char *) data);
    }
    return HIO_ERROR;
  }

  rc = hioi_manifest_parse_3_0 (dataset, object);
  if (free_data) {
    free ((char *) data);
  }

  return rc;
}

int hioi_manifest_read (const char *path, unsigned char **manifest_out, size_t *manifest_size_out)
{
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

  *manifest_out = buffer;
  *manifest_size_out = file_size;

  return HIO_SUCCESS;
}

int hioi_manifest_load (hio_dataset_t dataset, const char *path) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  unsigned char *manifest;
  size_t manifest_size;
  int rc;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Loading dataset manifest for %s:%" PRIu64
            " from %s", dataset->ds_object.identifier, dataset->ds_id, path);

  rc = hioi_manifest_read (path, &manifest, &manifest_size);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = hioi_manifest_deserialize (dataset, manifest, manifest_size);
  free (manifest);

  return rc;
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

  if (NULL != key) {
    (void) hioi_manifest_get_string (object, key, &value);
  } else {
    value = json_object_get_string (object);
  }

  for (int i = 0 ; i < array_size ; ++i) {
    json_object *array_object = json_object_array_get_idx (array, i);
    const char *value1 = NULL;

    assert (array_object);
    if (NULL != key) {
      (void) hioi_manifest_get_string (array_object, key, &value1);
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
      !hioi_manifest_compare_json (object1, object2, HIO_MANIFEST_PROP_HIO_VERSION) ||
      !hioi_manifest_compare_json (object1, object2, HIO_MANIFEST_PROP_DATASET_ID)) {
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

  elements1 = hioi_manifest_find_object (object1, "elements");
  elements2 = hioi_manifest_find_object (object2, "elements");

  if (NULL == elements1 && NULL != elements2) {
    /* move the array from object2 to object. the reference count needs to be
     * incremented before it is deleted to ensure it is not freed prematurely and
     * the reference count remains correct after it is added to object1. */
    json_object_get (elements2);
    json_object_object_del (object2, "elements");
    json_object_object_add (object1, "elements", elements2);
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

      segments = hioi_manifest_find_object (element, "segments");
      if (NULL != segments) {
        segment_count = json_object_array_length (segments);
      }

      if (HIO_SET_ELEMENT_UNIQUE != manifest_mode) {
        /* check if this element already exists */
        rc = hioi_manifest_array_find_matching (elements1, element, HIO_MANIFEST_PROP_IDENTIFIER);
      } else {
        /* assuming the manifests are from different ranks for now.. I may changet this
         * in the future. */
        rc = HIO_ERR_NOT_FOUND;
      }

      if (0 <= rc) {
        json_object *element1 = json_object_array_get_idx (elements1, rc);
        json_object *segments1 = hioi_manifest_find_object (element1, "segments");
        unsigned long element_size = 0, element1_size = 0;

        /* remove the segments from the element in the object2. get a reference first
         * so the array doesn't get freed before we are done with it */
        json_object_get (segments);
        json_object_object_del (element, "segments");

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
          json_object_object_add (element1, "segments", segments);
        }

        (void) hioi_manifest_get_number (element, HIO_MANIFEST_PROP_SIZE, &element_size);
        (void) hioi_manifest_get_number (element1, HIO_MANIFEST_PROP_SIZE, &element1_size);
        if (element_size > element1_size) {
          /* use the larger of the two sizes */
          json_object_object_del (element1, HIO_MANIFEST_PROP_SIZE);
          hioi_manifest_set_number (element1, HIO_MANIFEST_PROP_SIZE, element_size);
        }
      } else {
        json_object_get (element);
        json_object_array_add (elements1, element);
      }

    }

    /* remove the elements array from object2 */
    json_object_object_del (object2, "elements");
  }

  return HIO_SUCCESS;
}

int hioi_manifest_merge_data2 (unsigned char **data1, size_t *data1_size, const unsigned char *data2, size_t data2_size) {
  bool free_data2 = false, compressed = false;
  json_object *object1, *object2;
  unsigned char *data1_save;
  int rc;

  if (NULL == data1[0]) {
    if (NULL != data2 && data2_size) {
      data1[0] = malloc (data2_size);
      if (NULL == data1[0]) {
        return HIO_ERR_OUT_OF_RESOURCE;
      }

      memcpy (data1[0], data2, data2_size);
    } else {
      data1[0] = NULL;
    }

    *data1_size = data2_size;
    return HIO_SUCCESS;
  }

  /* decompress the data if necessary */
  if ('B' == data1[0][0] && 'Z' == data1[0][1]) {
    data1_save = data1[0];
    /* bz2 compressed */
    rc = hioi_manifest_decompress (data1, *data1_size);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    compressed = true;
    free (data1_save);
  }

  object1 = json_tokener_parse ((char *) data1[0]);
  if (NULL == object1) {
    return HIO_ERROR;
  }

  /* decompress the data if necessary */
  if ('B' == data2[0] && 'Z' == data2[1]) {
    /* bz2 compressed */
    rc = hioi_manifest_decompress ((unsigned char **) &data2, data2_size);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    /* data2 was malloc'd by decompress so it needs to be freed */
    free_data2 = true;
  }

  object2 = json_tokener_parse ((char *) data2);
  if (NULL == object2) {
    return HIO_ERROR;
  }

  rc = hioi_manifest_merge_internal (object1, object2);
  if (free_data2) {
    free ((void *) data2);
  }
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  data1_save = data1[0];
  rc = hioi_manifest_serialize_json (object1, data1, data1_size, compressed);
  free (data1_save);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  return rc;
}

static int rank_compare (const void *a, const void *b) {
  return (((const int *) a)[0] - ((const int *) b)[0]);
}

int hioi_manifest_ranks (const unsigned char *manifest, size_t manifest_size, int **ranks, int *rank_count) {
  json_object *object = NULL, *elements;
  int manifest_ranks, element_count;
  unsigned char *rank_map = NULL;
  bool free_manifest = false;
  int rc = HIO_SUCCESS;
  unsigned long size;

  if ('B' == manifest[0] && 'Z' == manifest[1]) {
    /* bz2 compressed */
    rc = hioi_manifest_decompress ((unsigned char **) &manifest, manifest_size);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    /* manifest was malloc'd by decompress so it needs to be freed */
    free_manifest = true;
  }

  do {
    object = json_tokener_parse ((char *) manifest);
    if (NULL == object) {
      rc = HIO_ERROR;
      break;
    }

    elements = hioi_manifest_find_object (object, "elements");
    element_count = elements ? json_object_array_length (elements) : 0;
    if (0 == element_count) {
      /* no elements */
      *ranks = NULL;
      *rank_count = 0;
      break;
    }

    /* find out how many ranks were used to write the manifest */
    rc = hioi_manifest_get_number (object, HIO_MANIFEST_KEY_COMM_SIZE, &size);
    if (HIO_SUCCESS != rc) {
      break;
    }

    rank_map = calloc (1, (size >> 3) + 1);
    if (NULL == rank_map) {
      rc = HIO_ERR_OUT_OF_RESOURCE;
      break;
    }

    manifest_ranks = 0;

    /* allocate enough space to hold the maximum number of ranks that
     * can appear in the manifest. */
    *ranks = (int *) calloc (element_count, sizeof (ranks[0][0]));
    if (NULL == *ranks) {
      rc = HIO_ERR_OUT_OF_RESOURCE;
      break;
    }

    for (int i = 0 ; i < element_count ; ++i) {
      json_object *element = json_object_array_get_idx (elements, i);
      unsigned long rank;

      assert (NULL != element);

      rc = hioi_manifest_get_number (element, HIO_MANIFEST_PROP_RANK, &rank);
      if (!(rank_map[rank >> 3] & (1 << (rank & 0x7)))) {
        ranks[0][manifest_ranks++] = (int) rank;
        rank_map[rank >> 3] |= 1 << (rank & 0x7);
      }
    }

    /* shrink the array if there were fewer ranks than elements */
    void *tmp = realloc (*ranks, sizeof (ranks[0][0]) * manifest_ranks);
    if (NULL == tmp) {
      free (*ranks);
      *ranks = NULL;
      rc = HIO_ERR_OUT_OF_RESOURCE;
      break;
    }
    *ranks = (int *) tmp;

    /* sort the rank array by rank */
    (void) qsort (*ranks, manifest_ranks, sizeof (int), rank_compare);
    *rank_count = manifest_ranks;
  } while (0);

  if (object) {
    json_object_put (object);
  }

  if (free_manifest) {
    free ((void *) manifest);
  }

  free (rank_map);

  return rc;
}

int hioi_manifest_read_header (hio_context_t context, hio_dataset_header_t *header, const char *path) {
  unsigned char *manifest;
  size_t manifest_size;
  json_object *object;
  int rc;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "loading json dataset manifest header from %s", path);

  if (access (path, F_OK)) {
    return HIO_ERR_NOT_FOUND;
  }

  if (access (path, R_OK)) {
    return HIO_ERR_PERM;
  }

  rc = hioi_manifest_read (path, &manifest, &manifest_size);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  if ('B' == manifest[0] && 'Z' == manifest[1]) {
    unsigned char *data = manifest;
    /* gz compressed */
    rc = hioi_manifest_decompress ((unsigned char **) &data, manifest_size);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
    free (manifest);
    manifest = data;
  }

  object = json_tokener_parse ((char *) manifest);
  if (NULL == object) {
    free (manifest);
    return HIO_ERROR;
  }

  rc = hioi_manifest_parse_header_2_0 (context, header, object);
  json_object_put (object);
  free (manifest);

  return rc;
}
