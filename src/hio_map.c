/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2016      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "hio_internal.h"

#if HIO_MPI_HAVE(3)

#include <stdlib.h>
#include <string.h>
#include <alloca.h>
#include <assert.h>

/* number of entries to get at once */
#define HIO_MAP_BUCKET_SIZE 8

enum hio_map_state_t {
  HIO_MAP_STATE_FREE,
  HIO_MAP_STATE_PENDING,
  HIO_MAP_STATE_VALID,
};

typedef struct hio_map_header_t {
  /** total number of entries */
  uint64_t mh_count;
  /** total number of slots */
  uint64_t mh_size;
} hio_map_header_t;

typedef struct hio_map_item_common_t {
  uint64_t state;
  uint64_t cksum;
} hio_map_item_common_t;

typedef struct hio_map_element_t {
  hio_map_item_common_t me_common;
  /** name of hio dataset element */
  char     me_name[HIO_ELEMENT_NAME_MAX + 1];
  /** map index (used for addressing element segments) */
  uint32_t me_index;
} hio_map_element_t;

typedef struct hio_map_segment_t {
  hio_map_item_common_t ms_common;

  struct hio_map_segment_key_t {
    /** reserved for future use (padding) */
    uint32_t ms_resv0;
    /** element index */
    uint32_t ms_index;
    /** segment application offset */
    uint64_t ms_aoff;
    /** segment size */
    uint64_t ms_size;
  } key;

  struct hio_map_segment_value_t {
    /* file address */
    /** reserved for future use (padding) */
    uint32_t ms_resv1;
    /** index of file holding the segment */
    uint32_t ms_findex;
    /** offset of segment within the file */
    uint64_t ms_foff;
  } value;
} hio_map_segment_t;

typedef bool (*hioi_map_key_compare_fn_t) (const void *a, const void *b);
typedef void (*hioi_map_element_prepare_fn_t) (MPI_Win win, void *data, void *key, int64_t count);
typedef uint64_t (*hioi_map_hash_fn_t) (void *value);

static void hioi_prepare_element (MPI_Win win, void *data, void *value, int64_t count) {
  hio_map_element_t *map_element = (hio_map_element_t *) data;
  map_element->me_index = count;
}

static void hioi_prepare_segment (MPI_Win win, void *data, void *value, int64_t count) {
  hio_map_segment_t *map_segment = (hio_map_segment_t *) data;
  struct hio_map_segment_value_t *ms_value = (struct hio_map_segment_value_t *) value;

  map_segment->value = ms_value[0];
}

static uint64_t hioi_hash_element (void *key) {
  char *key_string = (char *) key;
  uint64_t value = 5381;

  do {
    value = (int64_t) key_string[0] + (value << 5) + value;
  } while ((++key_string)[0]);

  return value;
}

static uint64_t hioi_hash_int64 (int64_t key) {
  uint64_t value = (uint64_t) key;

  value += ~(value << 15);
  value ^= value >> 10;
  value += value << 3;
  value ^= value >> 6;
  value += ~(value >> 11);
  value ^= value >> 16;

  return value;
}

#define HIOI_DEFINE_HASH(bits)                                          \
  static uint64_t hioi_hash_segment_ ## bits (void *key) {               \
    struct hio_map_segment_key_t *ms_key = (struct hio_map_segment_key_t *) key; \
    int64_t app_offset = ms_key->ms_aoff & ~ ((1l << bits) - 1);        \
    return hioi_hash_int64 (app_offset);                                \
  }

HIOI_DEFINE_HASH(10)
HIOI_DEFINE_HASH(20)
HIOI_DEFINE_HASH(27)
HIOI_DEFINE_HASH(30)
HIOI_DEFINE_HASH(40)
HIOI_DEFINE_HASH(50)

struct hio_segment_hash_t {
  size_t sh_bits;
  hioi_map_hash_fn_t sh_fn;
};

static const struct hio_segment_hash_t hio_segment_hashes[] = {
  {.sh_bits = 10, .sh_fn = hioi_hash_segment_10},
  {.sh_bits = 20, .sh_fn = hioi_hash_segment_20},
  {.sh_bits = 27, .sh_fn = hioi_hash_segment_27},
  {.sh_bits = 30, .sh_fn = hioi_hash_segment_30},
  {.sh_bits = 40, .sh_fn = hioi_hash_segment_40},
  {.sh_bits = 50, .sh_fn = hioi_hash_segment_50},
};

static const int hio_segment_hash_count = sizeof (hio_segment_hashes) / sizeof (hio_segment_hashes[0]);

static bool hioi_map_compare_string (const void *a, const void *b) {
  return 0 == strcmp ((const char *) a, (const char *) b);
}

static bool hioi_map_compare_segment (const void *a, const void *b) {
  struct hio_map_segment_key_t *sega = (struct hio_map_segment_key_t *) a;
  struct hio_map_segment_key_t *segb = (struct hio_map_segment_key_t *) b;

  return (sega->ms_aoff == segb->ms_aoff) && (sega->ms_index == segb->ms_index);
}

static int hioi_dataset_map_insert (hio_dataset_map_data_t *map, int *node_leaders, void *key, size_t key_len,
                                    void *value, void *item_out, hioi_map_hash_fn_t hash_fn,
                                    hioi_map_key_compare_fn_t compare_fn,
                                    hioi_map_element_prepare_fn_t prepare_fn)
{
  int kv_size =  map->md_element_size - sizeof (hio_map_item_common_t);
  uint64_t hash = hash_fn (key);
  size_t get_size = HIO_MAP_BUCKET_SIZE * map->md_element_size;
  void *bucket = alloca (get_size);
  int target, target_bucket, rc;
  MPI_Aint bucket_offset;
  bool bucket_full = true;

  do {
    hash = hash % map->md_global_size;
    if (0 == hash) {
      /* the first bucket contains map data */
      ++hash;
    }

    target = node_leaders[hash / map->md_local_size];
    target_bucket = hash % map->md_local_size;

    bucket_offset = target_bucket * get_size;

    rc = MPI_Get (bucket, get_size, MPI_BYTE, target, bucket_offset, get_size, MPI_BYTE, map->md_win);
    if (MPI_SUCCESS != rc) {
      return hioi_err_mpi (rc);
    }

    /* execute get */
    rc = MPI_Win_flush (target, map->md_win);
    if (MPI_SUCCESS != rc) {
      return hioi_err_mpi (rc);
    }

    bucket_full = true;
    for (int i = 0 ; i < HIO_MAP_BUCKET_SIZE ; ++i, bucket_offset += map->md_element_size) {
      hio_map_item_common_t *item = (hio_map_item_common_t *) ((intptr_t) bucket + i * map->md_element_size);
      void *element_key = (void *)(item + 1);
      bool match = false;
      int64_t cksum;

      if (HIO_MAP_STATE_FREE == item->state) {
          int64_t old_state, new_state = HIO_MAP_STATE_PENDING, incr = 1, count;
          MPI_Compare_and_swap (&new_state, &item->state, &old_state, MPI_INT64_T, target,
                                bucket_offset, map->md_win);
          MPI_Win_flush (target, map->md_win);
          if (HIO_MAP_STATE_FREE != old_state) {
            /* another process beat us to it. try again */
            bucket_full = false;
            break;
          }

          MPI_Fetch_and_op (&incr, &count, MPI_INT64_T, node_leaders[0], 0, MPI_SUM, map->md_win);
          MPI_Win_flush (node_leaders[0], map->md_win);

          /* insert */
          memcpy (element_key, key, key_len);
          prepare_fn (map->md_win, (void *) item, value, count);

          item->state = HIO_MAP_STATE_VALID;
          cksum = item->cksum = hioi_crc64 ((unsigned char *) element_key, kv_size);

          MPI_Put (item, map->md_element_size, MPI_BYTE, target, bucket_offset, map->md_element_size,
                   MPI_BYTE, map->md_win);
          MPI_Win_flush (target, map->md_win);
          match = true;
      } else if (HIO_MAP_STATE_VALID == item->state) {
        cksum = hioi_crc64 (element_key, kv_size);
        match = compare_fn (key, element_key);
      }

      if (HIO_MAP_STATE_VALID != item->state || item->cksum != cksum) {
        bucket_full = false;
        break;
      }

      if (match) {
        if (item_out) {
          memcpy (item_out, (void *) item, map->md_element_size);
        }

        return HIO_SUCCESS;
      }
    }

    if (bucket_full) {
      /* move on to next bucket */
      ++hash;
    } else {
      /* sleep a little while before trying again */
      const struct timespec interval = {.tv_sec = 0, .tv_nsec = 500};
      nanosleep (&interval, NULL);
    }
  } while (1);

  return HIO_ERROR;
}

static int32_t hioi_dataset_map_search (hio_dataset_map_data_t *map, int *node_leaders, void *key, void *data,
                                        hioi_map_hash_fn_t hash_fn, hioi_map_key_compare_fn_t compare_fn)
{
  uint64_t hash = hash_fn (key);
  size_t get_size = HIO_MAP_BUCKET_SIZE * map->md_element_size;
  void *bucket = alloca (get_size);
  int target, target_bucket, rc;
  bool next_bucket = true;
  MPI_Aint bucket_offset;

  do {
    hash = hash % map->md_global_size;
    if (0 == hash) {
      /* the first bucket contains map data */
      ++hash;
    }

    target = node_leaders[hash / map->md_local_size];
    target_bucket = hash % map->md_local_size;

    bucket_offset = target_bucket * get_size;

    rc = MPI_Get (bucket, get_size, MPI_BYTE, target, bucket_offset, get_size, MPI_BYTE, map->md_win);
    if (MPI_SUCCESS != rc) {
      return hioi_err_mpi (rc);
    }

    /* execute get */
    rc = MPI_Win_flush (target, map->md_win);
    if (MPI_SUCCESS != rc) {
      return hioi_err_mpi (rc);
    }

    next_bucket = true;
    for (int i = 0 ; i < HIO_MAP_BUCKET_SIZE ; ++i) {
      hio_map_item_common_t *item = (hio_map_item_common_t *) ((intptr_t) bucket + i * map->md_element_size);
      void *element_key = (void *)(item + 1);

      if (HIO_MAP_STATE_FREE == item->state) {
        return HIO_ERR_NOT_FOUND;
      } else if (HIO_MAP_STATE_PENDING == item->state) {
        next_bucket = false;
        break;
      } else if (HIO_MAP_STATE_VALID == item->state && compare_fn (key, element_key)) {
        memcpy (data, (void *) item, map->md_element_size);
        return HIO_SUCCESS;
      }
    }

    if (next_bucket) {
      /* move on to next bucket */
      ++hash;
    } else {
      /* sleep a little while before continuing */
      const struct timespec interval = {.tv_sec = 0, .tv_nsec = 1000};
      nanosleep (&interval, NULL);
    }
  } while (1);

  return HIO_ERROR;
}

static int hioi_dataset_map_data_initialize (hio_dataset_t dataset, hio_dataset_map_data_t *map,
                                             size_t global_size, size_t element_size) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  size_t alloc_size;
  MPI_Info info;
  MPI_Win win;
  void *base;
  int rc;

  map->md_win = MPI_WIN_NULL;

  if (0 == global_size) {
    return HIO_SUCCESS;
  }

  global_size = (1 + (global_size + HIO_MAP_BUCKET_SIZE * context->c_node_count - 1) / (HIO_MAP_BUCKET_SIZE * context->c_node_count)) *
    HIO_MAP_BUCKET_SIZE * context->c_node_count;

  map->md_global_size = global_size / HIO_MAP_BUCKET_SIZE;
  map->md_element_size = element_size;
  map->md_local_size = global_size / (HIO_MAP_BUCKET_SIZE * context->c_node_count);

  if (0 == context->c_shared_rank) {
    alloc_size = map->md_local_size * element_size * HIO_MAP_BUCKET_SIZE;
  } else {
    alloc_size = 0;
  }

  rc = MPI_Info_create (&info);
  if (MPI_SUCCESS != rc) {
    return hioi_err_mpi (rc);
  }

  /* will only use compare-and-swap and fetch-and-op on this window so allow the
   * MPI implemenation to optimize for this case. */
  rc = MPI_Info_set (info, "max_accumulate_count", "1");
  if (MPI_SUCCESS != rc) {
    MPI_Info_free (&info);
    return hioi_err_mpi (rc);
  }

  rc = MPI_Info_set (info, "same_disp_unit", "true");
  if (MPI_SUCCESS != rc) {
    MPI_Info_free (&info);
    return hioi_err_mpi (rc);
  }

  hioi_timed_call(rc = MPI_Win_allocate (alloc_size, 1, info, context->c_comm, (void *) &base, &win));
  MPI_Info_free (&info);
  if (MPI_SUCCESS != rc) {
    return hioi_err_mpi (rc);
  }

  if (alloc_size) {
    memset (base, 0, alloc_size);
  }

  MPI_Barrier (context->c_comm);

  MPI_Win_lock_all (0, win);

  map->md_win = win;

  return HIO_SUCCESS;
}

static void hioi_dataset_map_data_finalize (hio_dataset_map_data_t *map) {
  if (MPI_WIN_NULL == map->md_win) {
    return;
  }

  MPI_Win_unlock_all (map->md_win);

  (void) MPI_Win_free (&map->md_win);
  map->md_global_size = 0;
  map->md_local_size = 0;
  map->md_element_size = 0;
}

int hioi_dataset_map_insert_element (hio_element_t element) {
  hio_context_t context = hioi_object_context (&element->e_object);
  hio_dataset_t dataset = hioi_element_dataset (element);
  hio_dataset_map_t *map = &dataset->ds_map;
  hio_map_element_t item;
  int rc;

  rc = hioi_dataset_map_insert (&map->map_elements, context->c_node_leaders,
                                element->e_object.identifier,
                                strlen (element->e_object.identifier) + 1,
                                NULL, &item, hioi_hash_element, hioi_map_compare_string,
                                hioi_prepare_element);
  if (HIO_SUCCESS == rc) {
    element->e_index = item.me_index;
  }

  return rc;
}

static int hioi_dataset_map_generate_element_map (hio_dataset_t dataset, uint64_t max_element_count) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  hio_dataset_map_t *map = &dataset->ds_map;
  int rc;

  rc = hioi_dataset_map_data_initialize (dataset, &map->map_elements, 2 * max_element_count,
                                         sizeof (hio_map_element_t));
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  if (0 == context->c_shared_rank) {
    hio_element_t element;

    hioi_list_foreach (element, dataset->ds_elist, struct hio_element, e_list) {
      hioi_timed_call(rc = hioi_dataset_map_insert_element (element));
      if (HIO_SUCCESS != rc) {
        return rc;
      }
    }
  }

  MPI_Barrier (context->c_shared_comm);

  return HIO_SUCCESS;
}

/* insert a segment into the segment map */
int hioi_dataset_map_insert_segment (hio_element_t element, hio_manifest_segment_t *segment) {
  hio_context_t context = hioi_object_context (&element->e_object);
  hio_dataset_t dataset = hioi_element_dataset (element);
  hio_dataset_map_t *map = &dataset->ds_map;

  struct hio_map_segment_key_t key = {.ms_index = element->e_index,
                                      .ms_aoff = segment->seg_offset,
                                      .ms_size = segment->seg_length};
  struct hio_map_segment_key_t bound_key = {.ms_aoff = segment->seg_offset + segment->seg_length - 1};
  struct hio_map_segment_value_t value = {.ms_findex = segment->seg_file_index,
                                          .ms_foff = segment->seg_foffset};
  int rc;

  for (int i = 0 ; i < hio_segment_hash_count ; ++i) {
    /* make an entry for this segment at each relevant hash size */
    if (segment->seg_length > (1ul << hio_segment_hashes[i].sh_bits) && i < (hio_segment_hash_count - 1)) {
      continue;
    }

    rc = hioi_dataset_map_insert (&map->map_segments, context->c_node_leaders, &key, sizeof (key),
                                  &value, NULL, hio_segment_hashes[i].sh_fn, hioi_map_compare_segment,
                                  hioi_prepare_segment);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    if (hio_segment_hashes[i].sh_fn (&key) != hio_segment_hashes[i].sh_fn (&bound_key)) {
      /* crosses a hash block boundary */
      uint64_t next_block = (bound_key.ms_aoff + 1) & ~((1ul << hio_segment_hashes[i].sh_bits) - 1);
      uint64_t block_offset = next_block - segment->seg_offset;

      key.ms_aoff = next_block;
      key.ms_size -= block_offset;
      value.ms_foff += block_offset;

      rc = hioi_dataset_map_insert (&map->map_segments, context->c_node_leaders, &key, sizeof (key),
                                        &value, NULL, hio_segment_hashes[i].sh_fn, hioi_map_compare_segment,
                                        hioi_prepare_segment);
    }

    return rc;
  }

  return HIO_SUCCESS;
}

static int hioi_dataset_map_generate_segment_map (hio_dataset_t dataset, uint64_t max_segment_count) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  hio_dataset_map_t *map = &dataset->ds_map;
  int rc;

  /* each segment may be in the worst case hashed at every size times to speed lookups */
  rc = hioi_dataset_map_data_initialize (dataset, &map->map_segments, 4 * max_segment_count,
                                         sizeof (hio_map_segment_t));
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  if (0 == context->c_shared_rank) {
    hio_element_t element;

    hioi_list_foreach (element, dataset->ds_elist, struct hio_element, e_list) {
      for (int i = 0 ; i < element->e_scount ; ++i) {
        rc = hioi_dataset_map_insert_segment (element, element->e_sarray + i);
        if (HIO_SUCCESS != rc) {
          return rc;
        }
      }
    }
  }

  MPI_Barrier (context->c_comm);

  return HIO_SUCCESS;
}

int hioi_dataset_generate_map (hio_dataset_t dataset) {
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  uint64_t counts[2] = {0, 0};
  hio_element_t element;
  int rc;

  if (HIO_SET_ELEMENT_SHARED != dataset->ds_mode || !hioi_context_using_mpi (context)) {
    return HIO_ERR_NOT_AVAILABLE;
  }

  /* clean up any existing maps */
  (void) hioi_dataset_map_release (dataset);

  /* generate the leader communicator if it hasn't been already */
  hioi_timed_call(rc = hioi_context_generate_leader_list (context));
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  hioi_object_lock (&context->c_object);

  do {
    if (0 == context->c_shared_rank) {
      /* determine the number of elements and segments in the dataset */
      hioi_list_foreach (element, dataset->ds_elist, struct hio_element, e_list) {
        ++counts[0];
        counts[1] += element->e_scount;
      }

      rc = MPI_Allreduce (MPI_IN_PLACE, counts, 2, MPI_INT64_T, MPI_SUM,
                          context->c_node_leader_comm);
      if (MPI_SUCCESS != rc) {
        rc = hioi_err_mpi (rc);
        break;
      }
    }

    rc = MPI_Bcast (counts, 2, MPI_INT64_T, 0, context->c_shared_comm);
    if (MPI_SUCCESS != rc) {
      rc = hioi_err_mpi (rc);
      break;
    }

    hioi_timed_call(rc = hioi_dataset_map_generate_element_map (dataset, counts[0]));
    if (HIO_SUCCESS != rc) {
      break;
    }

    hioi_timed_call(rc = hioi_dataset_map_generate_segment_map (dataset, counts[1]));
  } while (0);

  hioi_object_unlock (&context->c_object);

  return rc;
}

int hioi_dataset_map_release (hio_dataset_t dataset) {
  hioi_dataset_map_data_finalize (&dataset->ds_map.map_segments);
  hioi_dataset_map_data_finalize (&dataset->ds_map.map_elements);

  return HIO_SUCCESS;
}

static int hioi_dataset_map_lookup_element (hio_element_t element) {
  hio_context_t context = hioi_object_context (&element->e_object);
  char *element_id = hioi_object_identifier (&element->e_object);
  hio_dataset_t dataset = hioi_element_dataset (element);
  hio_map_element_t map_element;
  int rc;

  if (MPI_WIN_NULL == dataset->ds_map.map_elements.md_win) {
    return HIO_ERR_NOT_FOUND;
  }

  rc = hioi_dataset_map_search (&dataset->ds_map.map_elements, context->c_node_leaders,
                                element_id, &map_element, hioi_hash_element,
                                hioi_map_compare_string);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  element->e_index = map_element.me_index;

  return HIO_SUCCESS;
}


static bool hioi_map_contains_segment (const void *a, const void *b) {
  struct hio_map_segment_key_t *sega = (struct hio_map_segment_key_t *) a;
  struct hio_map_segment_key_t *segb = (struct hio_map_segment_key_t *) b;

  if (sega->ms_index != segb->ms_index) {
    return false;
  }

  return (sega->ms_aoff >= segb->ms_aoff && sega->ms_aoff < (segb->ms_aoff + segb->ms_size));
}

static int hioi_dataset_map_lookup_segment (hio_element_t element, int64_t app_offset,
                                            hio_map_segment_t *segment) {
  hio_context_t context = hioi_object_context (&element->e_object);
  hio_dataset_t dataset = hioi_element_dataset (element);
  struct hio_map_segment_key_t key = {.ms_resv0 = 0, .ms_index = element->e_index,
                                      .ms_aoff = app_offset, .ms_size = 0};
  int rc;

  if (MPI_WIN_NULL == dataset->ds_map.map_segments.md_win) {
    return HIO_ERR_NOT_FOUND;
  }

  for (int i = 0 ; i < hio_segment_hash_count ; ++i) {
    rc = hioi_dataset_map_search (&dataset->ds_map.map_segments, context->c_node_leaders,
                                  &key, segment, hio_segment_hashes[i].sh_fn, hioi_map_contains_segment);
    if (HIO_SUCCESS == rc) {
      return HIO_SUCCESS;
    }
  }

  return HIO_ERR_NOT_FOUND;
}

int hioi_dataset_map_translate_offset (hio_element_t element, uint64_t app_offset,
                                       int *file_index, uint64_t *offset, size_t *length) {
  hio_map_segment_t segment = {.key = {.ms_aoff = 0, .ms_size = 0}, .value = {.ms_findex = -1, .ms_foff = -1}};
  uint64_t base, bound;
  int rc;

  if (-1 == element->e_index) {
    rc = hioi_dataset_map_lookup_element (element);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  rc = hioi_dataset_map_lookup_segment (element, app_offset, &segment);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  base = segment.key.ms_aoff;
  bound = base + segment.key.ms_size;

  *file_index = segment.value.ms_findex;
  *offset = segment.value.ms_foff + app_offset - base;
  if (app_offset + *length > bound) {
    *length = bound - app_offset;
  }

  return HIO_SUCCESS;
}

#endif
