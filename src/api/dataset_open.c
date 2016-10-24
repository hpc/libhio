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

struct hio_dataset_item_t {
  hio_dataset_header_t header;
  hio_module_t        *module;
  int                  ordering;
};
typedef struct hio_dataset_item_t hio_dataset_item_t;

static void hio_dataset_item_swap (hio_dataset_item_t *itema, hio_dataset_item_t *itemb) {
  hio_dataset_item_t tmp = *itema;
  *itema = *itemb;
  *itemb = tmp;
}

static int hioi_dataset_header_highest_setid (hio_dataset_header_t *ha, hio_dataset_header_t *hb) {
  if (ha->ds_id > hb->ds_id) {
    return 1;
  }
  if (ha->ds_id == hb->ds_id) {
    return 0;
  }
  return -1;
}

static int hioi_dataset_header_newest (hio_dataset_header_t *ha, hio_dataset_header_t *hb) {
  if (ha->ds_mtime > hb->ds_mtime) {
    return 1;
  }
  if (ha->ds_mtime == hb->ds_mtime) {
    return 0;
  }
  return -1;
}

/**
 * Insert a potential set_id/module combination into the priority queue
 *
 * @param[in]     items      priority queue of set_id/module pairs
 * @param[in,out] item_count number of items in the queue
 * @param[in]     set_id     identifier of dataset to insert
 * @param[in]     module     module associated with this dataset idenfier
 */
static void hio_dataset_item_insert (hio_dataset_item_t *items, int *item_count, hio_dataset_header_t *header,
                                     hio_module_t *module, int ordering, hioi_dataset_header_compare_t compare) {
  int item_index = *item_count;
  int ret;

  items[item_index].header = *header;
  items[item_index].module = module;
  items[item_index].ordering = ordering;

  ++*item_count;

  while (item_index) {
    int parent = (item_index - 1) >> 1;

    ret = compare (&items[parent].header, &items[item_index].header);
    if (1 == ret || (0 == ret && items[parent].ordering <= items[item_index].ordering)) {
      break;
    }

    /* the new item has a larger set id. swap with parent */
    hio_dataset_item_swap (items + parent, items + item_index);
    item_index = parent;
  }
}

/**
 * Remove the highest set_id/module combination from the priority queue
 *
 * @param[in]     items      priority queue of set_id/module pairs
 * @param[in,out] item_count number of items in the queue
 * @param[out]    set_id     highest dataset identifier
 * @param[out]    module     module associated with the highest set_id
 *
 * @returns HIO_SUCCESS on success
 * @returns HIO_ERR_NOT_FOUND on failure (empty queue)
 */
static int hio_dataset_item_pop (hio_dataset_item_t *items, int *item_count, hio_dataset_header_t *header,
                                 hio_module_t **module, hioi_dataset_header_compare_t compare) {
  int item_index = 0;
  int ret;

  if (0 == *item_count) {
    return HIO_ERR_NOT_FOUND;
  }

  *header = items[0].header;
  *module = items[0].module;

  hio_dataset_item_swap (items, items + *item_count - 1);

  --*item_count;
  while (item_index < *item_count - 1) {
    int left = (item_index << 1) | 1, right = left + 1, child = left;

    if (right < *item_count && compare (&items[right].header, &items[left].header)) {
      child = right;
    }

    ret = compare (&items[item_index].header, &items[child].header);
    if (1 == ret || (0 == ret && items[item_index].ordering <= items[child].ordering)) {
      break;
    }

    /* swap this item with the larger child */
    hio_dataset_item_swap (items + item_index, items + child);
    item_index = child;
  }

  return HIO_SUCCESS;
}

static int hio_dataset_open_last (hio_dataset_t dataset, hioi_dataset_header_compare_t compare) {
  hio_context_t context = hioi_object_context ((hio_object_t) dataset);
  int item_count = 0, rc, count = 0;
  hio_dataset_item_t *items = NULL;
  hio_dataset_header_t *headers = NULL, header;
  hio_module_t *module;
  void *tmp;

  for (int i = 0 ; i < context->c_mcount ; ++i) {
    module = context->c_modules[i];

    rc = module->dataset_list (module, hioi_object_identifier (dataset), &headers, &count);
    if (!(HIO_SUCCESS == rc && count)) {
      continue;
    }
  }

  if (0 == count) {
    free (headers);
    return HIO_ERR_NOT_FOUND;
  }

  items = (hio_dataset_item_t *) calloc (count, sizeof (*items));
  if (NULL == items) {
    free (headers);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  for (int j = 0 ; j < count ; ++j) {
    /* insert this set_id/module combination into the priority queue */
    hio_dataset_item_insert (items, &item_count, headers + j, module, j, compare);
  }

  free (headers);

  while (HIO_SUCCESS == (rc = hio_dataset_item_pop (items, &item_count, &header, &module, compare))) {
    /* set the current dataset id to the one we are attempting to open */
    dataset->ds_id = header.ds_id;
    rc = hioi_dataset_open_internal (module, dataset);
    if (HIO_SUCCESS == rc) {
      break;
    }

    if (HIO_SUCCESS != rc) {
      /* reset the id to the id originally requested */
      dataset->ds_id = dataset->ds_id_requested;
    }
  }

  free (items);

  return rc;
}

static int hio_dataset_open_specific (hio_context_t context, hio_dataset_t dataset) {
  int rc = HIO_ERR_NOT_FOUND;

  for (int i = 0 ; i <= context->c_mcount ; ++i) {
    int module_index = (context->c_cur_module + i) % context->c_mcount;

    hio_module_t *module = context->c_modules[module_index];
    if (NULL == module) {
      /* internal error */
      return HIO_ERROR;
    }

    rc = hioi_dataset_open_internal (module, dataset);
    if (HIO_SUCCESS == rc) {
      break;
    }
  }

  return rc;
}

int hio_dataset_open (hio_dataset_t dataset) {
  hio_context_t context;

  if (HIO_OBJECT_NULL == dataset) {
    return HIO_ERR_BAD_PARAM;
  }

  if (dataset->ds_flags & HIO_FLAG_TRUNC) {
    /* ensure we take the create path later */
    dataset->ds_flags |= HIO_FLAG_CREAT;
  }

  context = hioi_object_context ((hio_object_t) dataset);

  if (HIO_DATASET_ID_HIGHEST == dataset->ds_id) {
    return hio_dataset_open_last (dataset, hioi_dataset_header_highest_setid);
  } else if (HIO_DATASET_ID_NEWEST == dataset->ds_id) {
    return hio_dataset_open_last (dataset, hioi_dataset_header_newest);
  }

  return hio_dataset_open_specific (context, dataset);
}
