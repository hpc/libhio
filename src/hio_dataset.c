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
#include <string.h>
#include <assert.h>

static hio_var_enum_value_t hioi_dataset_file_mode_values[] = {
  {.string_value = "basic", .value = HIO_FILE_MODE_BASIC},
  {.string_value = "optimized", .value = HIO_FILE_MODE_OPTIMIZED}};

static hio_var_enum_t hioi_dataset_file_modes = {
  .count  = 2,
  .values = hioi_dataset_file_mode_values,
};

static hio_var_enum_value_t hioi_dataset_fs_type_enum_values[] = {
  {.string_value = "default", .value = HIO_FS_TYPE_DEFAULT},
  {.string_value = "lustre", .value = HIO_FS_TYPE_LUSTRE},
  {.string_value = "gpfs", .value = HIO_FS_TYPE_GPFS}};

static hio_var_enum_t hioi_dataset_fs_type_enum = {
  .count  = 3,
  .values = hioi_dataset_fs_type_enum_values,
};

hio_element_t hioi_element_alloc (hio_dataset_t dataset, const char *name) {
  hio_element_t element;

  element = (hio_element_t) calloc (1, sizeof (*element));
  if (NULL == element) {
    return NULL;
  }

  element->e_object.identifier = strdup (name);
  if (NULL == element->e_object.identifier) {
    free (element);
    return NULL;
  }

  element->e_object.type = HIO_OBJECT_TYPE_ELEMENT;
  element->e_object.parent = &dataset->ds_object;

  hioi_list_init (element->e_slist);

  return element;
}

void hioi_element_release (hio_element_t element) {
  if (NULL != element) {
    if (NULL != element->e_object.identifier) {
      free (element->e_object.identifier);
    }

    if (NULL != element->e_bfile) {
      free (element->e_bfile);
    }

    free (element);
  }
}

static int hioi_dataset_data_lookup (hio_context_t context, const char *name, hio_dataset_data_t **data) {
  hio_dataset_data_t *ds_data;

  /* look for existing persistent data */
  pthread_mutex_lock (&context->c_lock);
  hioi_list_foreach (ds_data, context->c_ds_data, hio_dataset_data_t, dd_list) {
    if (0 == strcmp (ds_data->dd_name, name)) {
      pthread_mutex_unlock (&context->c_lock);
      *data = ds_data;
      return HIO_SUCCESS;
    }
  }

  /* allocate new persistent dataset data and add it to the context */
  ds_data = (hio_dataset_data_t *) calloc (1, sizeof (*ds_data));
  if (NULL == ds_data) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  ds_data->dd_name = strdup (name);
  if (NULL == ds_data->dd_name) {
    free (ds_data);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  ds_data->dd_last_id = -1;

  hioi_list_init (ds_data->dd_backend_data);

  hioi_list_append (ds_data, context->c_ds_data, dd_list);

  *data = ds_data;
  pthread_mutex_unlock (&context->c_lock);

  return HIO_SUCCESS;
}

hio_dataset_t hioi_dataset_alloc (hio_context_t context, const char *name, int64_t id,
                                  int flags, hio_dataset_mode_t mode, size_t dataset_size) {
  hio_dataset_t new_dataset;
  int rc;

  /* bozo check for invalid dataset object size */
  assert (dataset_size >= sizeof (*new_dataset));

  /* allocate new dataset object */
  new_dataset = calloc (1, dataset_size);
  if (NULL == new_dataset) {
    return NULL;
  }

  /* lookup/allocate persistent dataset data. this data will keep track of per-dataset
   * statistics (average write time, last successful checkpoint, etc) */
  rc = hioi_dataset_data_lookup (context, name, &new_dataset->ds_data);
  if (HIO_SUCCESS != rc) {
    free (new_dataset);
    return NULL;
  }

  /* initialize new dataset object */
  new_dataset->ds_object.identifier = strdup (name);
  if (NULL == new_dataset->ds_object.identifier) {
    free (new_dataset);
    return NULL;
  }

  new_dataset->ds_object.type = HIO_OBJECT_TYPE_DATASET;
  new_dataset->ds_object.parent = &context->c_object;
  new_dataset->ds_id = id;
  new_dataset->ds_flags = flags;
  new_dataset->ds_mode = mode;

  new_dataset->ds_fmode = HIO_FILE_MODE_BASIC;
  hioi_config_add (context, &new_dataset->ds_object, &new_dataset->ds_fmode,
                   "dataset_file_mode", HIO_CONFIG_TYPE_INT32, &hioi_dataset_file_modes,
                   "Modes for writing dataset files. Valid values: (0: basic, 1: optimized)", 0);

  if (context->c_size < 8192) {
    new_dataset->ds_bs = 1ul << 30;
  } else if (context->c_size < 131072) {
    new_dataset->ds_bs = 1ul << 34;
  } else {
    new_dataset->ds_bs = 1ul << 38;
  }

  new_dataset->ds_bs = 1ul << 34;
  hioi_config_add (context, &new_dataset->ds_object, &new_dataset->ds_bs,
                   "dataset_block_size", HIO_CONFIG_TYPE_INT64, NULL,
                   "Block size to use when writing in optimized mode (default: job size dependent)", 0);

  new_dataset->fs_fsattr.fs_type = HIO_FS_TYPE_DEFAULT;
  hioi_config_add (context, &new_dataset->ds_object, &new_dataset->fs_fsattr.fs_type,
                   "dataset_filesystem_type", HIO_CONFIG_TYPE_INT32, &hioi_dataset_fs_type_enum,
                   "Type of filesystem this dataset resides on", HIO_VAR_FLAG_READONLY);

  /* set up performance variables */
  hioi_perf_add (context, &new_dataset->ds_object, &new_dataset->ds_stat.s_bread, "bytes_read",
                 HIO_CONFIG_TYPE_UINT64, NULL, "Total number of bytes read in this dataset instance", 0);

  hioi_perf_add (context, &new_dataset->ds_object, &new_dataset->ds_stat.s_bwritten, "bytes_written",
                 HIO_CONFIG_TYPE_UINT64, NULL, "Total number of bytes written in this dataset instance", 0);

  hioi_list_init (new_dataset->ds_elist);

  return new_dataset;
}

void hioi_dataset_release (hio_dataset_t *set) {
  hio_element_t element, next;
  hio_context_t context;
  hio_module_t *module;

  if (!set || !*set) {
    return;
  }

  module = (*set)->ds_module;
  context = hioi_object_context (&(*set)->ds_object);

  hioi_list_foreach_safe(element, next, (*set)->ds_elist, struct hio_element, e_list) {
    if (element->e_is_open) {
      hioi_log (context, HIO_VERBOSE_WARN, "element still open at dataset close");
      module->element_close (module, element);
    }

    hioi_list_remove(element, e_list);
    hioi_element_release (element);
  }

  if ((*set)->ds_object.identifier) {
    free ((*set)->ds_object.identifier);
  }

  free (*set);
  *set = NULL;
}

void hioi_dataset_add_element (hio_dataset_t dataset, hio_element_t element) {
  hioi_list_append (element, dataset->ds_elist, e_list);
}

int hioi_element_add_segment (hio_element_t element, off_t file_offset, uint64_t app_offset,
                              int rank, size_t seg_length) {
  hio_manifest_segment_t *segment = NULL;

  if (element->e_slist.prev != &element->e_slist) {
    unsigned long last_offset;

    segment = hioi_list_item(element->e_slist.prev, hio_manifest_segment_t, seg_list);

    last_offset = segment->seg_offset + segment->seg_length;

    if (last_offset == app_offset) {
      segment->seg_length += seg_length;
      return HIO_SUCCESS;
    }
  }

  segment = (hio_manifest_segment_t *) malloc (sizeof (*segment));
  if (NULL == segment) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  segment->seg_foffset = (uint64_t) file_offset;
  segment->seg_offset = app_offset;
  segment->seg_rank = rank;
  segment->seg_length = seg_length;

  hioi_list_append (segment, element->e_slist, seg_list);

  return HIO_SUCCESS;
}

int hioi_element_find_offset (hio_element_t element, uint64_t app_offset, int rank,
                              off_t *offset, size_t *length) {
  hio_manifest_segment_t *segment;

  hioi_list_foreach(segment, element->e_slist, hio_manifest_segment_t, seg_list) {
    uint64_t base, bound, remaining;

    base = (uint64_t) segment->seg_offset;
    bound = base + segment->seg_length;

    if (app_offset >= base && app_offset <= bound) {
      *offset = segment->seg_foffset + (app_offset - base);

      remaining = segment->seg_length - (app_offset - base);

      if (remaining < *length) {
        *length = remaining;
      }

      return HIO_SUCCESS;
    }
  }

  return HIO_ERR_NOT_FOUND;
}

hio_dataset_backend_data_t *hioi_dbd_alloc (hio_dataset_data_t *data, const char *backend_name, size_t size) {
  hio_dataset_backend_data_t *new_backend_data;
  int rc;

  assert (size >= sizeof (*new_backend_data));

  new_backend_data = calloc (1, size);
  if (NULL == new_backend_data) {
    return NULL;
  }

  new_backend_data->dbd_backend_name = strdup (backend_name);
  if (NULL == new_backend_data->dbd_backend_name) {
    free (new_backend_data);
    return NULL;
  }

  hioi_list_append (new_backend_data, data->dd_backend_data, dbd_list);

  return new_backend_data;
}

/**
 * Retrieve stored backend data
 *
 * @param[in] data         dataset persistent data structure
 * @param[in] backend_name name of the requesting backend
 */
hio_dataset_backend_data_t *hioi_dbd_lookup_backend_data (hio_dataset_data_t *data, const char *backend_name) {
  hio_dataset_backend_data_t *dbd_data;

  hioi_list_foreach (dbd_data, data->dd_backend_data, hio_dataset_backend_data_t, dbd_list) {
    if (0 == strcmp (dbd_data->dbd_backend_name, backend_name)) {
      return dbd_data;
    }
  }

  return NULL;
}

int hioi_dataset_gather (hio_dataset_t dataset) {
#if HIO_USE_MPI
  hio_context_t context = (hio_context_t) dataset->ds_object.parent;
  int parent = (context->c_rank - 1) >> 1;
  int left = context->c_rank * 2 + 1, right = left + 1;
  long int recv_size_left = 0, recv_size_right = 0, send_size;
  MPI_Request reqs[2];
  int64_t *sizes;
  unsigned char *data;
  size_t data_size;
  int rc, nreqs = 0;

  if (1 == context->c_size) {
    /* nothing to do */
    return rc;
  }

  /* the needs of this routine are a little more complicated than MPI_Reduce. the data size may
   * grow as the results are reduced. this function implements a basic reduction algorithm on
   * the hio dataset */

  if (right < context->c_size) {
    MPI_Irecv (&recv_size_right, 1, MPI_LONG, right, 1001, context->c_comm, reqs + 1);
    ++nreqs;
  }

  if (left < context->c_size) {
    MPI_Irecv (&recv_size_left, 1, MPI_LONG, left, 1001, context->c_comm, reqs);
    ++nreqs;
  }

  if (nreqs) {
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "waiting on %d requests", nreqs);

    MPI_Waitall (nreqs, reqs, MPI_STATUSES_IGNORE);

    data = malloc (recv_size_right > recv_size_left ? recv_size_right : recv_size_left);
    if (NULL == data) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    if (right < context->c_size) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "receiving %lu bytes of manifest data from %d", recv_size_right,
                right);
      MPI_Recv (data, recv_size_right, MPI_CHAR, right, 1002, context->c_comm, MPI_STATUS_IGNORE);
      hioi_manifest_merge_data (dataset, data, recv_size_right);
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "receiving %lu bytes of manifest data from %d", recv_size_left,
              left);
    MPI_Recv (data, recv_size_left, MPI_CHAR, left, 1002, context->c_comm, MPI_STATUS_IGNORE);
    hioi_manifest_merge_data (dataset, data, recv_size_left);
    free (data);
  }

  if (parent >= 0) {
    rc = hioi_manifest_serialize (dataset, &data, &data_size);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    send_size = data_size;
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "sending %lu bytes of manifest data from %d to %d", send_size,
              context->c_rank, parent);

    MPI_Send (&send_size, 1, MPI_LONG, parent, 1001, context->c_comm);
    MPI_Send (data, send_size, MPI_CHAR, parent, 1002, context->c_comm);

    free (data);
  }
#endif
  return HIO_SUCCESS;
}

int hioi_dataset_scatter (hio_dataset_t dataset, int rc) {
#if HIO_USE_MPI
  hio_context_t context = (hio_context_t) dataset->ds_object.parent;
  unsigned char *data;
  size_t data_size;
  long ar_data[5];

  if (1 == context->c_size) {
    /* nothing to do */
    return rc;
  }

  if (HIO_SUCCESS == rc && 0 == context->c_rank) {
    rc = hioi_manifest_serialize (dataset, &data, &data_size);
  }

  ar_data[0] = rc;
  ar_data[1] = (long) data_size;
  ar_data[2] = dataset->ds_flags;
  ar_data[3] = dataset->fs_fsattr.fs_scount;
  ar_data[4] = dataset->fs_fsattr.fs_ssize;

  rc = MPI_Bcast (ar_data, 5, MPI_LONG, 0, context->c_comm);
  if (MPI_SUCCESS != rc) {
    return hio_err_mpi (rc);
  }

  if (HIO_SUCCESS != ar_data[0]) {
    return ar_data[0];
  }

  data_size = (size_t) ar_data[1];

  if (0 != context->c_rank) {
    data = malloc (data_size);
    assert (NULL != data);
  }

  rc = MPI_Bcast (data, data_size, MPI_BYTE, 0, context->c_comm);
  if (MPI_SUCCESS != rc) {
    return hio_err_mpi (rc);
  }

  if (0 != context->c_rank) {
    rc = hioi_manifest_deserialize (dataset, data, data_size);
  }

  /* copy flags determined by rank 0 */
  dataset->ds_flags = ar_data[2];
  dataset->fs_fsattr.fs_scount = ar_data[3];
  dataset->fs_fsattr.fs_ssize = ar_data[4];

  free (data);
#endif

  return rc;
}
