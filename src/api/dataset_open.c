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
  int64_t set_id;
  hio_module_t *module;
};
typedef struct hio_dataset_item_t hio_dataset_item_t;

static int hio_dataset_open_internal (hio_module_t *module, hio_dataset_t *set_out, const char *name,
                                      int64_t set_id, hio_flags_t flags, hio_dataset_mode_t mode) {
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

/**
 * Insert a potential set_id/module combination into the priority queue
 *
 * @param[in]     items      priority queue of set_id/module pairs
 * @param[in,out] item_count number of items in the queue
 * @param[in]     set_id     identifier of dataset to insert
 * @param[in]     module     module associated with this dataset idenfier
 */
static void hio_dataset_item_insert (hio_dataset_item_t *items, int *item_count, int64_t set_id, hio_module_t *module) {
  int item_index = *item_count;

  items[item_index].set_id = set_id;
  items[item_index].module = module;

  ++*item_count;

  while (item_index) {
    int parent = item_index >> 1;
    if (items[parent].set_id <= set_id) {
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
static int hio_dataset_item_pop (hio_dataset_item_t *items, int *item_count, int64_t *set_id, hio_module_t **module) {
  int item_index = 0;

  if (0 == *item_count) {
    return HIO_ERR_NOT_FOUND;
  }

  *set_id = items[0].set_id;
  *module = items[0].module;

  hio_dataset_item_swap (items, items + *item_count - 1);

  --*item_count;
  while (item_index < *item_count/2) {
    int left = item_index << 1, right = left | 1, child = left;

    if (right < *item_count && items[right].set_id > items[left].set_id) {
      child = right;
    }

    if (items[item_index].set_id > items[child].set_id) {
      break;
    }

    /* swap this item with the larger child */
    hio_dataset_item_swap (items + item_index, items + child);
    item_index = child;
  }

  return HIO_SUCCESS;
}

static int hio_dataset_open_last (hio_context_t context, hio_dataset_t *set_out, const char *name,
                                  hio_flags_t flags, hio_dataset_mode_t mode) {
  int item_count = 0, rc, num_set_ids;
  hio_dataset_item_t *items = NULL;
  int64_t *set_ids, set_id;
  hio_module_t *module;
  void *tmp;

  for (int i = 0 ; i < context->context_module_count ; ++i) {
    module = context->context_modules[i];

    rc = module->dataset_list (module, name, &set_ids, &num_set_ids);
    if (HIO_SUCCESS == rc && num_set_ids) {
      tmp = realloc ((void *) items, (item_count + num_set_ids) * sizeof (*items));
      if (NULL == tmp) {
        free (items);
        free (set_ids);
        return HIO_ERR_OUT_OF_RESOURCE;
      }

      items = (hio_dataset_item_t *) tmp;

      for (int j = 0 ; j < num_set_ids ; ++j) {
        /* insert this set_id/module combination into the priority queue */
        hio_dataset_item_insert (items, &item_count, set_ids[j], module);
      }
    }

    free (set_ids);
  }

  if (0 == item_count) {
    return HIO_ERR_NOT_FOUND;
  }

  while (HIO_SUCCESS == (rc = hio_dataset_item_pop (items, &item_count, &set_id, &module))) {
    rc = hio_dataset_open_internal (module, set_out, name, set_id, flags, mode);
    if (HIO_SUCCESS == rc) {
      break;
    }
  }

  free (items);

  return rc;
}

static int hio_dataset_open_specific (hio_context_t context, hio_dataset_t *set_out, const char *name,
                                      int64_t set_id, hio_flags_t flags, hio_dataset_mode_t mode) {
  hio_module_t *module = hioi_context_select_module (context);
  int rc;

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
                      int64_t set_id, hio_flags_t flags, hio_dataset_mode_t mode) {
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

  if (HIO_DATASET_ID_LAST == set_id) {
    return hio_dataset_open_last (context, set_out, name, flags, mode);
  }

  return hio_dataset_open_specific (context, set_out, name, set_id, flags, mode);
}
