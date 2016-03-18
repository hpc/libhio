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

#include "hio_types.h"

#include <stdlib.h>

struct hio_dataset_item_t {
  hio_dataset_header_t header;
  hio_module_t        *module;
  int                  ordering;
};
typedef struct hio_dataset_item_t hio_dataset_item_t;

static int hio_dataset_open_internal (hio_module_t *module, hio_dataset_t dataset) {
  /* get timestamp before open call */
  uint64_t rotime = hioi_gettime ();
  int rc;

  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "Opening dataset %s::%llu with flags 0x%x with backend module %p",
            dataset->ds_object.identifier, dataset->ds_id, dataset->ds_flags, module);

  /* Several things need to be done here:
   * 1) check if the user is requesting a specific dataset or the newest available,
   * 2) check if the dataset specified already exists in any module,
   * 3) if the dataset does not exist and we are creating then use the current
   *    module to open (create) the dataset. */
  rc = module->dataset_open (module, dataset);
  if (HIO_SUCCESS != rc) {
    hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "Failed to open dataset %s::%llu on data root %s",
              dataset->ds_object.identifier, dataset->ds_id, module->data_root);
  } else {
    dataset->ds_rotime = rotime;
  }

  return rc;
}

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
  int item_count = 0, rc, count;
  hio_dataset_item_t *items = NULL;
  hio_dataset_header_t *headers, header;
  hio_module_t *module;
  void *tmp;

  for (int i = 0 ; i < context->c_mcount ; ++i) {
    module = context->c_modules[i];

    rc = module->dataset_list (module, hioi_object_identifier (dataset), &headers, &count);
    if (!(HIO_SUCCESS == rc && count)) {
      continue;
    }

    tmp = realloc ((void *) items, (item_count + count) * sizeof (*items));
    if (NULL == tmp) {
      free (items);
      free (headers);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    items = (hio_dataset_item_t *) tmp;

    for (int j = 0 ; j < count ; ++j) {
      /* insert this set_id/module combination into the priority queue */
      hio_dataset_item_insert (items, &item_count, headers + j, module, i, compare);
    }

    free (headers);
  }

  if (0 == item_count) {
    if (items) {
      free (items);
    }

    return HIO_ERR_NOT_FOUND;
  }

  while (HIO_SUCCESS == (rc = hio_dataset_item_pop (items, &item_count, &header, &module, compare))) {
    /* set the current dataset id to the one we are attempting to open */
    dataset->ds_id = header.ds_id;
    rc = hio_dataset_open_internal (module, dataset);
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

    rc = hio_dataset_open_internal (module, dataset);
    if (HIO_SUCCESS == rc) {
      break;
    }
  }

  return rc;
}

int hio_dataset_open (hio_dataset_t dataset) {
  hio_context_t context;
  int rc;

  if (HIO_OBJECT_NULL == dataset) {
    return HIO_ERR_BAD_PARAM;
  }

  context = hioi_object_context ((hio_object_t) dataset);

  if (HIO_DATASET_ID_HIGHEST == dataset->ds_id) {
    return hio_dataset_open_last (dataset, hioi_dataset_header_highest_setid);
  } else if (HIO_DATASET_ID_NEWEST == dataset->ds_id) {
    return hio_dataset_open_last (dataset, hioi_dataset_header_newest);
  }

  return hio_dataset_open_specific (context, dataset);
}
