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

struct hio_dataset_item_t {
  hio_dataset_header_t header;
  hio_module_t        *module;
  int                  ordering;
};
typedef struct hio_dataset_item_t hio_dataset_item_t;

static int hio_dataset_open_internal (hio_module_t *module, hio_dataset_t *set_out, const char *name,
                                      int64_t set_id, int flags, hio_dataset_mode_t mode) {
  int rc;

  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "Opening dataset %s::%llu with flags 0x%x with backend module %p",
            name, set_id, flags, module);

  /* Several things need to be done here:
   * 1) check if the user is requesting a specific dataset or the newest available,
   * 2) check if the dataset specified already exists in any module,
   * 3) if the dataset does not exist and we are creating then use the current
   *    module to open (create) the dataset. */
  rc = module->dataset_open (module, set_out, name, set_id, flags, mode);
  if (HIO_SUCCESS != rc) {
    hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "Failed to open dataset %s::%llu on data root %s", name, set_id,
              module->data_root);
  }

  return rc;
}

static void hio_dataset_item_swap (hio_dataset_item_t *itema, hio_dataset_item_t *itemb) {
  hio_dataset_item_t tmp = *itema;
  *itema = *itemb;
  *itemb = tmp;
}

static int hioi_dataset_header_highest_setid (hio_dataset_header_t *ha, hio_dataset_header_t *hb) {
  if (ha->dataset_id > hb->dataset_id) {
    return 1;
  }
  if (ha->dataset_id == hb->dataset_id) {
    return 0;
  }
  return -1;
}

static int hioi_dataset_header_newest (hio_dataset_header_t *ha, hio_dataset_header_t *hb) {
  if (ha->dataset_mtime > hb->dataset_mtime) {
    return 1;
  }
  if (ha->dataset_mtime == hb->dataset_mtime) {
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

static int hio_dataset_open_last (hio_context_t context, hio_dataset_t *set_out, const char *name,
                                  int flags, hio_dataset_mode_t mode, hioi_dataset_header_compare_t compare) {
  int item_count = 0, rc, count;
  hio_dataset_item_t *items = NULL;
  hio_dataset_header_t *headers, header;
  hio_module_t *module;
  void *tmp;

  if ((flags & (HIO_FLAG_CREAT | HIO_FLAG_TRUNC)) == HIO_FLAG_CREAT) {
    return HIO_ERR_BAD_PARAM;
  }

  for (int i = 0 ; i < context->context_module_count ; ++i) {
    module = context->context_modules[i];

    rc = module->dataset_list (module, name, &headers, &count);
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
    rc = hio_dataset_open_internal (module, set_out, name, header.dataset_id, flags, mode);
    if (HIO_SUCCESS == rc) {
      break;
    }
  }

  free (items);

  return rc;
}

static int hio_dataset_open_specific (hio_context_t context, hio_dataset_t *set_out, const char *name,
                                      int64_t set_id, int flags, hio_dataset_mode_t mode) {
  hio_module_t *module = hioi_context_select_module (context);
  int rc = HIO_ERR_NOT_FOUND;

  if (NULL == module) {
    hio_err_push (HIO_ERROR, context, NULL, "Could not select hio module");
    return HIO_ERR_NOT_FOUND;
  }

  for (int i = 0 ; i <= context->context_module_count ; ++i) {
    int module_index = (context->context_current_module + i) % context->context_module_count;

    module = context->context_modules[module_index];

    rc = hio_dataset_open_internal (module, set_out, name, set_id, flags, mode);
    if (HIO_SUCCESS == rc) {
      break;
    }
  }

  return rc;
}

int hio_dataset_open (hio_context_t context, hio_dataset_t *set_out, const char *name,
                      int64_t set_id, int flags, hio_dataset_mode_t mode) {
  int rc;

  if (NULL == context || NULL == set_out || NULL == name) {
    return HIO_ERR_BAD_PARAM;
  }

  if (0 == context->context_module_count) {
    /* create hio modules for each item in the specified data roots */
    rc = hioi_context_create_modules (context);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  if (HIO_DATASET_ID_HIGHEST == set_id) {
    return hio_dataset_open_last (context, set_out, name, flags, mode, hioi_dataset_header_highest_setid);
  } else if (HIO_DATASET_ID_NEWEST == set_id) {
    return hio_dataset_open_last (context, set_out, name, flags, mode, hioi_dataset_header_newest);
  }

  return hio_dataset_open_specific (context, set_out, name, set_id, flags, mode);
}
