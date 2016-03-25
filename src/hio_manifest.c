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

#include <json.h>

#define HIO_MANIFEST_VERSION "2.1"
#define HIO_MANIFEST_COMPAT  "2.0"

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
#define HIO_MANIFEST_KEY_MTIME        "hio_mtime"
#define HIO_MANIFEST_KEY_COMM_SIZE    "hio_comm_size"
#define HIO_MANIFEST_KEY_STATUS       "hio_status"
#define HIO_SEGMENT_KEY_FILE_OFFSET   "loff"
#define HIO_SEGMENT_KEY_APP_OFFSET0   "off"
#define HIO_SEGMENT_KEY_LENGTH        "len"
#define HIO_SEGMENT_KEY_FILE_INDEX    "findex"

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

static json_object *hio_manifest_new_object (json_object *parent, const char *name) {
  json_object *new_object = json_object_new_object ();

  if (NULL == new_object) {
    return NULL;
  }

  json_object_object_add (parent, name, new_object);

  return new_object;
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
    fprintf (stderr, "Could not find JSON object for %s\n", name);
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
    fprintf (stderr, "Could not find JSON object for %s\n", name);
    return HIO_ERR_NOT_FOUND;
  }

  *value = (unsigned long) json_object_get_int64 (object);

  return HIO_SUCCESS;
}

static int hioi_manifest_get_signed_number (json_object *parent, const char *name, long *value) {
  json_object *object;

  object = hioi_manifest_find_object (parent, name);
  if (NULL == object) {
    fprintf (stderr, "Could not find JSON object for %s\n", name);
    return HIO_ERR_NOT_FOUND;
  }

  *value = (long) json_object_get_int64 (object);

  return HIO_SUCCESS;
}

static json_object *hio_manifest_generate_2_0 (hio_dataset_t dataset) {
  json_object *elements, *object, *segments, *top, *files;
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  hio_object_t hio_object = &dataset->ds_object;
  hio_manifest_segment_t *segment;
  hio_manifest_file_t *file;
  hio_element_t element;
  char *string_tmp;
  int rc;

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

  rc = hio_config_get_value (&dataset->ds_object, "dataset_file_mode", &string_tmp);
  assert (HIO_SUCCESS == rc);

  hioi_manifest_set_string (top, HIO_MANIFEST_KEY_FILE_MODE, string_tmp);
  free (string_tmp);

  if (HIO_FILE_MODE_OPTIMIZED == dataset->ds_fmode) {
    hioi_manifest_set_number (top, HIO_MANIFEST_KEY_BLOCK_SIZE, (unsigned long) dataset->ds_bs);
  }
  hioi_manifest_set_number (top, HIO_MANIFEST_KEY_COMM_SIZE, (unsigned long) context->c_size);
  hioi_manifest_set_signed_number (top, HIO_MANIFEST_KEY_STATUS, (long) dataset->ds_status);
  hioi_manifest_set_number (top, HIO_MANIFEST_KEY_MTIME, (unsigned long) time (NULL));

  if (HIO_FILE_MODE_BASIC == dataset->ds_fmode) {
    /* NTH: for now do not write elements for basic mode. this may change in future versions */
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

  if (dataset->ds_file_count) {
    json_object *files_object = hio_manifest_new_array (top, "files");
    if (NULL == files_object) {
      json_object_put (top);
      return NULL;
    }

    for (int i = 0 ; i < dataset->ds_file_count ; ++i) {
      hio_manifest_file_t *file = dataset->ds_flist + i;
      json_object *file_object = json_object_new_object ();
      if (NULL == file_object) {
        json_object_put (top);
        return NULL;
      }

      hioi_manifest_set_string (file_object, HIO_MANIFEST_PROP_IDENTIFIER,
                                file->f_name);
      json_object_array_add (files_object, file_object);
    }
  }

  return top;
}

int hioi_manifest_serialize (hio_dataset_t dataset, unsigned char **data, size_t *data_size, bool compress_data ) {
  json_object *json_object;
  const char *serialized;
  unsigned int serialized_len;
  size_t size;
  int rc;

  json_object = hio_manifest_generate_2_0 (dataset);
  if (NULL == json_object) {
    return HIO_ERROR;
  }

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

  /* free the json object */
  json_object_put (json_object);

  return HIO_SUCCESS;
}

int hioi_manifest_save (hio_dataset_t dataset, const char *path) {
  const char *extension = strrchr (path, '.') + 1;
  unsigned char *data;
  size_t data_size;
  int rc;

  if (0 == strcmp (extension, "bz2")) {
    rc = hioi_manifest_serialize (dataset, &data, &data_size, true);
  } else {
    rc = hioi_manifest_serialize (dataset, &data, &data_size, false);
  }

  if (HIO_SUCCESS != rc) {
    return rc;
  }

  int fd = open (path, O_WRONLY | O_CREAT, 0644);
  if (0 > fd) {
    return hioi_err_errno (errno);
  }

  errno = 0;
  write (fd, data, data_size);
  close (fd);
  free (data);

  return hioi_err_errno (errno);
}

static int hioi_manifest_parse_file_2_1 (hio_dataset_t dataset, json_object *file_object) {
  const char *tmp_string;
  int rc;

  rc = hioi_manifest_get_string (file_object, HIO_MANIFEST_PROP_IDENTIFIER, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERROR, &dataset->ds_object, "Manifest file entry missing identifier");
    return HIO_ERROR;
  }

  return hioi_dataset_add_file (dataset, tmp_string);
}


static int hioi_manifest_parse_segment_2_1 (hio_element_t element, json_object *files, json_object *segment_object, bool merge) {
  unsigned long file_offset, app_offset0, length, file_index;
  hio_dataset_t dataset = hioi_element_dataset (element);
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

  if (files && merge) {
    json_object *file_object;
    const char *tmp_string;

    file_object = json_object_array_get_idx (files, file_index);
    if (NULL == file_object) {
      hioi_err_push (HIO_ERROR, &element->e_object, "Manifest segment specified invalid file index");
      return HIO_ERROR;
    }

    if (merge) {
      rc = hioi_manifest_parse_file_2_1 (dataset, file_object);
      if (0 > rc) {
        return rc;
      }

      file_index = rc;
    }
  }

  return hioi_element_add_segment (element, file_index, file_offset, app_offset0, length);
}

static int hioi_manifest_parse_segments_2_1 (hio_element_t element, json_object *files, json_object *object, bool merge) {
  int segment_count = json_object_array_length (object);

  for (int i = 0 ; i < segment_count ; ++i) {
    json_object *segment_object = json_object_array_get_idx (object, i);
    int rc = hioi_manifest_parse_segment_2_1 (element, files, segment_object, merge);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_element_2_0 (hio_dataset_t dataset, json_object *files, json_object *element_object, bool merge) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  hio_element_t element = NULL, tmp_element;
  bool new_element = false;
  json_object *segments_object;
  const char *tmp_string;
  unsigned long value;
  int rc, rank;

  rc = hioi_manifest_get_string (element_object, HIO_MANIFEST_PROP_IDENTIFIER, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERROR, &dataset->ds_object, "Manifest element missing identifier property");
    return HIO_ERROR;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Manifest element: %s", tmp_string);

  if (HIO_SET_ELEMENT_UNIQUE == dataset->ds_mode) {
    rc = hioi_manifest_get_number (element_object, HIO_MANIFEST_PROP_RANK, &value);
    if (HIO_SUCCESS != rc) {
      hioi_object_release (&element->e_object);
      return HIO_ERR_BAD_PARAM;
    }

    rank = value;
  } else {
    rank = -1;
  }

  if (merge) {
    hioi_list_foreach (tmp_element, dataset->ds_elist, struct hio_element, e_list) {
      if (!strcmp (tmp_element->e_object.identifier, tmp_string) && rank == tmp_element->e_rank) {
        element = tmp_element;
        break;
      }
    }
  }

  if (NULL == element) {
    element = hioi_element_alloc (dataset, (const char *) tmp_string, rank);
    if (NULL == element) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    new_element = true;
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
    rc = hioi_manifest_parse_segments_2_1 (element, files, segments_object, merge);
    if (HIO_SUCCESS != rc) {
      hioi_object_release (&element->e_object);
      return rc;
    }
  }

  if (new_element) {
    hioi_dataset_add_element (dataset, element);
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Found element with identifier %s in manifest",
	    element->e_object.identifier);

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_elements_2_0 (hio_dataset_t dataset, json_object *files, json_object *object, bool merge) {
  int element_count = json_object_array_length (object);

  for (int i = 0 ; i < element_count ; ++i) {
    json_object *element_object = json_object_array_get_idx (object, i);
    int rc = hioi_manifest_parse_element_2_0 (dataset, files, element_object, merge);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_files_2_1 (hio_dataset_t dataset, json_object *object) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  int file_count = json_object_array_length (object);

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "Parsing %d file entries in manifest", file_count);
  for (int i = 0 ; i < file_count ; ++i) {
    json_object *file_object = json_object_array_get_idx (object, i);
    int rc = hioi_manifest_parse_file_2_1 (dataset, file_object);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  return HIO_SUCCESS;
}

static int hioi_manifest_parse_2_0 (hio_dataset_t dataset, json_object *object, bool merge) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  json_object *elements_object, *files_object;
  unsigned long mode = 0, size;
  const char *tmp_string;
  long status;
  int rc;

  if (!merge) {
    /* check for compatibility with this manifest version */
    rc = hioi_manifest_get_string (object, HIO_MANIFEST_PROP_COMPAT, &tmp_string);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Compatibility version of manifest: %s",
              (char *) tmp_string);

    if (strcmp ((char *) tmp_string, "2.0")) {
      /* incompatible version */
      return HIO_ERROR;
    }

    rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_DATASET_MODE, &tmp_string);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    if (0 == strcmp ((const char *) tmp_string, "unique")) {
      mode = HIO_SET_ELEMENT_UNIQUE;
    } else if (0 == strcmp ((const char *) tmp_string, "shared")) {
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

    if (HIO_FILE_MODE_OPTIMIZED == dataset->ds_fmode) {
      rc = hioi_manifest_get_number (object, HIO_MANIFEST_KEY_BLOCK_SIZE, &size);
      if (HIO_SUCCESS != rc) {
        return HIO_ERR_BAD_PARAM;
      }

      dataset->ds_bs = size;
    } else {
      dataset->ds_bs = (uint64_t) -1;
    }
  }

  rc = hioi_manifest_get_signed_number (object, HIO_MANIFEST_KEY_STATUS, &status);
  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  if (!merge || !dataset->ds_status) {
    dataset->ds_status = status;
  }

  files_object = hioi_manifest_find_object (object, "files");
  if (!merge && files_object) {
    rc = hioi_manifest_parse_files_2_1 (dataset, files_object);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    files_object = NULL;
  }

  /* find and parse all elements covered by this manifest */
  elements_object = hioi_manifest_find_object (object, "elements");
  if (NULL == elements_object) {
    /* no elements in this file. odd but still valid */
    return HIO_SUCCESS;
  }

  return hioi_manifest_parse_elements_2_0 (dataset, files_object, elements_object, merge);
}

static int hioi_manifest_parse_header_2_0 (hio_context_t context, hio_dataset_header_t *header, json_object *object) {
  json_object *elements_object;
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

  if (strcmp ((char *) tmp_string, "2.0")) {
    /* incompatible version */
    return HIO_ERROR;
  }

  /* fill in header */
  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_DATASET_MODE, &tmp_string);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  if (0 == strcmp ((const char *) tmp_string, "unique")) {
    value = HIO_SET_ELEMENT_UNIQUE;
  } else if (0 == strcmp ((const char *) tmp_string, "shared")) {
    value = HIO_SET_ELEMENT_SHARED;
  } else {
    hioi_err_push (HIO_ERR_BAD_PARAM, &context->c_object, "unknown dataset mode specified in manifest: "
                  "%s", (const char *) tmp_string);
    return HIO_ERR_BAD_PARAM;
  }

  if (HIO_SUCCESS != rc) {
    return HIO_ERR_BAD_PARAM;
  }

  header->ds_mode = value;

  rc = hioi_manifest_get_string (object, HIO_MANIFEST_KEY_FILE_MODE, &tmp_string);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (HIO_ERR_BAD_PARAM, &context->c_object, "file mode was not specified in manifest");
    return HIO_ERR_BAD_PARAM;
  }

  if (0 == strcmp ((const char *) tmp_string, "basic")) {
    value = HIO_FILE_MODE_BASIC;
  } else if (0 == strcmp ((const char *) tmp_string, "optimized")) {
    value = HIO_FILE_MODE_OPTIMIZED;
  } else {
    hioi_err_push (HIO_ERR_BAD_PARAM, &context->c_object, "unrecognized file mode in manifest: %s",
                  tmp_string);
    return HIO_ERR_BAD_PARAM;
  }

  header->ds_fmode = value;

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

static int hioi_manifest_decompress (hio_dataset_t dataset, const unsigned char **data, size_t data_size) {
  const size_t increment = 8192;
  char *uncompressed, *tmp;
  json_object *object;
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
    rc = hioi_manifest_decompress (dataset, &data, data_size);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    free_data = true;
  }

  object = json_tokener_parse ((char *) data);
  if (NULL == object) {
    return HIO_ERROR;
  }

  rc = hioi_manifest_parse_2_0 (dataset, object, false);
  if (free_data) {
    free ((char *) data);
  }

  return rc;
}

static int hioi_manifest_read (const char *path, json_object **object_out, char **buffer_out)
{
  const char *extension = strrchr (path, '.') + 1;
  char *buffer = NULL, *tmp;
  json_object *object;
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

  if (0 == strcmp (extension, "bz2")) {
    BZFILE *bzfh = BZ2_bzReadOpen (&rc, fh, 0, 0, NULL, 0);
    if (BZ_OK != rc) {
      fclose (fh);
      return hioi_err_errno (errno);
    }

    for (size_t offset = 0, size = 8192 ; ; offset = size, size += 8192) {
      tmp = realloc (buffer, size);
      if (NULL == tmp) {
        free (buffer);
        BZ2_bzReadClose (&rc, bzfh);
        fclose (fh);
        return HIO_ERR_OUT_OF_RESOURCE;
      }
      buffer = tmp;

      int bytes_read = BZ2_bzRead (&rc, bzfh, (void *)((intptr_t) buffer + offset), size - offset);
      if (bytes_read < size - offset) {
        int close_rc;
        BZ2_bzReadClose (&close_rc, bzfh);
        fclose (fh);
        if (BZ_STREAM_END != rc) {
          free (buffer);
          return hioi_err_errno (errno);
        }
        break;
      }
    }
  } else {
    size_t size = fseek (fh, 0, SEEK_END);

    buffer = malloc (size);
    if (NULL == buffer) {
      fclose (fh);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    fread (buffer, size, 1, fh);
    fclose (fh);
  }

  object = json_tokener_parse (buffer);
  if (NULL == object) {
    return HIO_ERROR;
  }

  *buffer_out = buffer;
  *object_out = object;

  return HIO_SUCCESS;
}

int hioi_manifest_load (hio_dataset_t dataset, const char *path) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  json_object *object;
  char *buffer;
  int rc;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Loading dataset manifest for %s:%lu from %s",
	    dataset->ds_object.identifier, dataset->ds_id, path);

  rc = hioi_manifest_read (path, &object, &buffer);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = hioi_manifest_parse_2_0 (dataset, object, false);
  free (buffer);

  return rc;
}

int hioi_manifest_merge_data (hio_dataset_t dataset, const unsigned char *data, size_t data_size) {
  json_object *object, *merged;
  bool free_data = false;
  int rc;

  if ('B' == data[0] && 'Z' == data[1]) {
    /* gz compressed */
    rc = hioi_manifest_decompress (dataset, &data, data_size);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    free_data = true;
  }

  object = json_tokener_parse ((char *) data);
  if (NULL == object) {
    return HIO_ERROR;
  }

  rc = hioi_manifest_parse_2_0 (dataset, object, true);
  if (free_data) {
    free ((char *) data);
  }

  return rc;
}

int hioi_manifest_read_header (hio_context_t context, hio_dataset_header_t *header, const char *path) {
  json_object *object;
  char *buffer;
  int rc;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "loading json dataset manifest header from %s", path);

  if (access (path, F_OK)) {
    return HIO_ERR_NOT_FOUND;
  }

  if (access (path, R_OK)) {
    return HIO_ERR_PERM;
  }

  rc = hioi_manifest_read (path, &object, &buffer);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  rc = hioi_manifest_parse_header_2_0 (context, header, object);
  free (buffer);

  return rc;
}
