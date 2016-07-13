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
#include <string.h>
#include <assert.h>


static void hioi_element_release (hio_object_t object) {
  hio_element_t element = (hio_element_t) object;

  free (element->e_sarray);
}

hio_element_t hioi_element_alloc (hio_dataset_t dataset, const char *name, const int rank) {
  hio_element_t element;

  element = (hio_element_t) hioi_object_alloc (name, HIO_OBJECT_TYPE_ELEMENT, &dataset->ds_object,
                                               sizeof (*element), hioi_element_release);
  if (NULL == element) {
    return NULL;
  }

  element->e_rank = rank;
  element->e_file.f_fd = -1;
  element->e_index = -1;

  return element;
}

int hioi_element_open_internal (hio_dataset_t dataset, hio_element_t *element_out, const char *element_name,
                                int flags, int rank) {
  hio_element_t element;
  int rc = HIO_SUCCESS;

  if (HIO_SET_ELEMENT_SHARED == dataset->ds_mode) {
    /* rank is meaningless in shared mode */
    rank = -1;
  }

  hioi_object_lock (&dataset->ds_object);
  hioi_list_foreach (element, dataset->ds_elist, struct hio_element, e_list) {
    if (!strcmp (hioi_object_identifier(element), element_name) && rank == element->e_rank) {
      *element_out = element;
      if (0 == element->e_open_count++ && hioi_dataset_doing_io (dataset)) {
        /* don't actually "open" the element unless this rank is performing IO directly */
        rc = dataset->ds_element_open (dataset, element);
        if (HIO_SUCCESS != rc) {
          element->e_open_count = 0;
        }
      }
      hioi_object_unlock (&dataset->ds_object);
      return rc;
    }
  }

  /* no existing element matches */
  do {
    element = hioi_element_alloc (dataset, element_name, rank);
    if (NULL == element) {
      rc = HIO_ERR_OUT_OF_RESOURCE;
      break;
    }

    if (hioi_dataset_doing_io (dataset)) {
      /* don't actually "open" the element unless this rank is performing IO directly */
      rc = dataset->ds_element_open (dataset, element);
      if (HIO_SUCCESS != rc) {
        hioi_object_release (&element->e_object);
        break;
      }
    }

    element->e_open_count = 1;
    *element_out = element;

    hioi_dataset_add_element (dataset, element);
  } while (0);

  hioi_object_unlock (&dataset->ds_object);

  return rc;
}

int hioi_element_close_internal (hio_element_t element) {
  hio_dataset_t dataset = hioi_element_dataset (element);
  int rc = HIO_SUCCESS;

  hioi_object_lock (&dataset->ds_object);
  if (0 == --element->e_open_count && hioi_dataset_doing_io (dataset)) {
    if (dataset->ds_flags & HIO_FLAG_WRITE) {
      hioi_object_unlock (&dataset->ds_object);
      rc = hio_element_flush (element, HIO_FLUSH_MODE_LOCAL);
      hioi_object_lock (&dataset->ds_object);
    }

    rc = element->e_close (element);
    hioi_file_close (&element->e_file);
  }
  hioi_object_unlock (&dataset->ds_object);

  return rc;
}

static int hioi_element_segment_compare (const void *key, const void *value) {
  hio_manifest_segment_t *segment = (hio_manifest_segment_t *) value;
  uint64_t offset = (intptr_t) key;
  uint64_t base, bound;

  base = segment->seg_offset;
  bound = base + segment->seg_length;

  if (offset < base) {
    return -1;
  } else if (offset > bound) {
    return 1;
  }

  return 0;
}

/**
 * Add a segment descriptor to an element
 *
 * @param[in] element hio element handle
 * @param[in] file_index index in element e_flist for the associated file
 * @param[in] file_offset offset where the application segment lives
 * @param[in] app_offset application offset
 * @param[in[ seg_length length of application segment
 *
 * This function adds a segment to an hio element handle. This segment
 * will be written to the manifest when the dataset containing the
 * element is closed.
 */
int hioi_element_add_segment (hio_element_t element, int file_index, uint64_t file_offset, uint64_t app_offset,
                              size_t seg_length) {
  hio_manifest_segment_t *segment = NULL;
  int seg_index = 0;
  void *tmp;

  hioi_object_lock (&element->e_object);

  if (element->e_sarray) {
    unsigned long last_offset, last_file_offset;

    segment = (hio_manifest_segment_t *) bsearch ((void *) (intptr_t) app_offset, element->e_sarray,
                                                  element->e_scount, sizeof (element->e_sarray[0]),
                                                  hioi_element_segment_compare);
    if (segment) {
      last_offset = segment->seg_offset + segment->seg_length;
      last_file_offset = segment->seg_foffset + segment->seg_length;

      /* in order to match this segment must fall in the same logical file and have both file and applications
       * offsets that immediately follow the existing segment */
      if (last_offset == app_offset && last_file_offset == file_offset && segment->seg_file_index == file_index) {
        segment->seg_length += seg_length;
        hioi_object_unlock (&element->e_object);
        return HIO_SUCCESS;
      }
    }
  }

  for (seg_index = 0 ; seg_index < element->e_scount ; ++seg_index) {
    segment = element->e_sarray + seg_index;
    if (segment->seg_offset > app_offset) {
      break;
    }
  }

  if (element->e_scount == element->e_ssize) {
    element->e_ssize += 32;
    tmp = realloc (element->e_sarray, element->e_ssize * sizeof (element->e_sarray[0]));
    if (NULL == tmp) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    element->e_sarray = (hio_manifest_segment_t *) tmp;
  }

  segment = element->e_sarray + seg_index;
  if (element->e_scount != seg_index) {
    memmove (element->e_sarray + seg_index + 1, segment, sizeof (element->e_sarray[0]) * (element->e_scount - seg_index));
  }

  segment->seg_foffset = (uint64_t) file_offset;
  segment->seg_offset = app_offset;
  segment->seg_length = seg_length;
  segment->seg_file_index = file_index;

  assert (seg_length > 0);

  ++element->e_scount;

  hioi_object_unlock (&element->e_object);

  return HIO_SUCCESS;
}

/**
 * Translate an application offset into a logical file and offset
 *
 * @param[in] element hio element handle
 * @param[in] app_offset application offset
 * @param[out] file_index logical file index
 * @param[out] offset logical file offset
 * @param[inout] length length of application segment
 *
 * This function translates an application block into a logical file
 * segment. If a segment exists that matches the beginning of the
 * segment the index and offset are returned. If the application
 * block extends past the end of the segment the length is adjusted
 * to the end of the file segment.
 */
int hioi_element_translate_offset (hio_element_t element, uint64_t app_offset, int *file_index,
                                   uint64_t *offset, size_t *length) {
  hio_manifest_segment_t *segment;
  uint64_t base, bound, remaining;
  int rc = HIO_ERR_NOT_FOUND;

  hioi_object_lock (&element->e_object);

  segment = (hio_manifest_segment_t *) bsearch ((void *) (intptr_t) app_offset, element->e_sarray,
                                                element->e_scount, sizeof (element->e_sarray[0]),
                                                hioi_element_segment_compare);
  if (NULL == segment) {
    hioi_object_unlock (&element->e_object);
    return HIO_ERR_NOT_FOUND;
  }

  base = segment->seg_offset;
  bound = base + segment->seg_length;

  if (app_offset == bound) {
    int seg_index = ((intptr_t) segment - (intptr_t) element->e_sarray) / sizeof (*segment);
    if (seg_index != element->e_scount - 1) {
      ++segment;
      base = segment->seg_offset;
      bound = base + segment->seg_length;
    }
  }

  /* check if the base falls in the file segment */
  if (app_offset >= base && app_offset < bound) {
    /* fill in return values */
    *offset = segment->seg_foffset + (app_offset - base);
    *file_index = segment->seg_file_index;

    remaining = segment->seg_length - (app_offset - base);

    if (remaining < *length) {
      *length = remaining;
    }

    rc = HIO_SUCCESS;
  }

  hioi_object_unlock (&element->e_object);

  return rc;
}
