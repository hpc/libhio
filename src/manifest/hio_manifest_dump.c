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

#include <string.h>

struct hioi_key_mapping_t {
  char *key;
  char *value;
};

static struct hioi_key_mapping_t hioi_key_mapping[] = {
  {.key = HIO_MANIFEST_KEY_VERSION, .value = "Manifest version"},
  {.key = HIO_MANIFEST_KEY_COMPAT, .value = "Manifest compatibility version"},
  {.key = HIO_MANIFEST_KEY_IDENTIFIER, .value = "Name"},
  {.key = HIO_MANIFEST_KEY_DATASET_ID, .value = "Dataset identifier"},
  {.key = HIO_MANIFEST_KEY_SIZE, .value = "Size"},
  {.key = HIO_MANIFEST_KEY_HIO_VERSION, .value = "libhio version"},
  {.key = HIO_MANIFEST_KEY_RANK, .value = "Rank"},
  {.key = HIO_MANIFEST_KEY_DATASET_MODE, .value = "Element offset mode"},
  {.key = HIO_MANIFEST_KEY_COMM_SIZE, .value = "IO ranks"},
  {.key = HIO_MANIFEST_KEY_STATUS, .value = "Status"},
  {.key = HIO_MANIFEST_KEY_MTIME, .value = "Modification time"},
  {.key = HIO_MANIFEST_KEY_CONFIG, .value = "Configuration"},
  {.key = HIO_MANIFEST_KEY_ELEMENTS, .value = "Elements"},
  {.key = HIO_MANIFEST_KEY_SEGMENTS, .value = "Segments"},
  {.key = HIO_SEGMENT_KEY_FILE_OFFSET, .value = "File offset"},
  {.key = HIO_SEGMENT_KEY_APP_OFFSET0, .value = "Offset"},
  {.key = HIO_SEGMENT_KEY_LENGTH, .value = "Length"},
  {.key = HIO_SEGMENT_KEY_FILE_INDEX, .value = "File index"},
  {.key = NULL},
};

static int hioi_manifest_segment_sort (const void *a, const void *b) {
  json_object * const *ja = (json_object * const *) a;
  json_object * const *jb = (json_object * const *) b;
  unsigned long offset_a, offset_b;

  hioi_manifest_get_number (*ja, HIO_SEGMENT_KEY_APP_OFFSET0, &offset_a);
  hioi_manifest_get_number (*jb, HIO_SEGMENT_KEY_APP_OFFSET0, &offset_b);
  return (offset_a > offset_b) ? 1 : -1;
}

static void hioi_manifest_print_yaml (const char *key, json_object *object, FILE *fh, bool array, int level, int rank) {
  bool is_time = false, first = true, is_elements = false;
  json_type type;
  int length;

  type = json_object_get_type (object);

  if (!array) {
    for (int i = 0 ; hioi_key_mapping[i].key ; ++i) {
      if (0 == strcasecmp (key, hioi_key_mapping[i].key)) {
        if (hioi_key_mapping[i].key == HIO_MANIFEST_KEY_MTIME) {
          is_time = true;
        } else if (hioi_key_mapping[i].key == HIO_MANIFEST_KEY_SEGMENTS) {
          json_object_array_sort (object, hioi_manifest_segment_sort);
        } else if (hioi_key_mapping[i].key == HIO_MANIFEST_KEY_ELEMENTS) {
          is_elements = true;
        }
        key = hioi_key_mapping[i].value;
      }
    }
  }

  fprintf (fh, "%*s%s%c", level, "", array ? "-" : key, array ? ' ' : ':');
  switch (type) {
  case json_type_object:
    if (!array) {
      fprintf (fh, "\n");
    }
    json_object_object_foreach (object, next_key, val) {
      int next_level = array ? !first * (level + 2) : level + 4;
      hioi_manifest_print_yaml (next_key, val, fh, false, next_level, rank);
      first = false;
    }
    break;
  case json_type_array:
    fprintf (fh, "\n");
    length = json_object_array_length (object);
    for (int i = 0 ; i < length ; ++i) {
      json_object *val = json_object_array_get_idx (object, i);

      if (rank >= 0 && is_elements) {
        long element_rank = -1;
        hioi_manifest_get_signed_number (val, HIO_MANIFEST_KEY_RANK, &element_rank);
        if (element_rank >= 0 && (int) element_rank != rank) {
          continue;
        }
      }

      hioi_manifest_print_yaml (NULL, val, fh, true, level + 4, rank);
    }
    break;
  default:
    if (is_time) {
      time_t mtime = (time_t) json_object_get_int64 (object);
      fprintf (fh, " %s", ctime (&mtime));
    } else {
      fprintf (fh, " %s\n", json_object_get_string (object));
    }
  }
}

static int hioi_manifest_dump_2_0 (hio_context_t context, json_object *object, int32_t flags, int rank, FILE *fh) {
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

  if (!(flags & HIO_DUMP_FLAG_CONFIG)) {
    json_object_object_del (object, HIO_MANIFEST_KEY_CONFIG);
  }


  json_object_object_foreach (object, key, val) {
    hioi_manifest_print_yaml (key, val, fh, false, 4, rank);
  }

  return HIO_SUCCESS;
}


int hioi_manifest_dump_file (hio_context_t context, const char *path, uint32_t flags, int rank, FILE *fh) {
  hio_manifest_t manifest;
  size_t manifest_size;
  json_object *object;
  int rc;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "dumping json dataset manifest header from %s", path);

  rc = hioi_manifest_read (context, path, &manifest);
  if (HIO_SUCCESS != rc || NULL == manifest) {
    return rc;
  }

  rc = hioi_manifest_dump (manifest, flags, rank, fh);
  hioi_manifest_release (manifest);

  return rc;
}

int hioi_manifest_dump (hio_manifest_t manifest, uint32_t flags, int rank, FILE *fh) {
  int rc;

  return hioi_manifest_dump_2_0 (manifest->context, manifest->json_object, flags, rank, fh);
}
