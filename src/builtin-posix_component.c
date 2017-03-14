/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "builtin-posix_component.h"
#include "hio_manifest.h"

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdarg.h>
#include <ftw.h>
#include <assert.h>

#include <string.h>

#if defined(HAVE_STRINGS_H)
#include <strings.h>
#endif

#include <errno.h>

#include <dirent.h>
#include <unistd.h>

#if defined(HAVE_SYS_STAT_H)
#include <sys/stat.h>
#endif

static hio_var_enum_t hioi_dataset_lock_strategies = {
  .count = 3,
  .values = (hio_var_enum_value_t []){
    {.string_value = "default", .value = HIO_FS_LOCK_DEFAULT},
    {.string_value = "group", .value = HIO_FS_LOCK_GROUP},
    {.string_value = "disable", .value = HIO_FS_LOCK_DISABLE},
  },
};

static hio_var_enum_t builtin_posix_apis = {
  .count = 3,
  .values = (hio_var_enum_value_t []){
    {.string_value = "posix", .value = HIO_FAPI_POSIX},
    {.string_value = "stdio", .value = HIO_FAPI_STDIO},
    {.string_value = "pposix", .value = HIO_FAPI_PPOSIX},
  },
};

static hio_var_enum_t hioi_dataset_file_modes = {
  .count  = 3,
  .values = (hio_var_enum_value_t []){
    {.string_value = "basic", .value = HIO_FILE_MODE_BASIC},
    {.string_value = "file_per_node", .value = HIO_FILE_MODE_OPTIMIZED},
    {.string_value = "strided", .value = HIO_FILE_MODE_STRIDED},
  },
};

/** static functions */
static int builtin_posix_module_dataset_unlink (struct hio_module_t *module, const char *name, int64_t set_id);
static int builtin_posix_module_dataset_close (hio_dataset_t dataset);
static int builtin_posix_module_element_open (hio_dataset_t dataset, hio_element_t element);
static int builtin_posix_module_element_flush (hio_element_t element, hio_flush_mode_t mode);
static int builtin_posix_module_element_complete (hio_element_t element);
static int builtin_posix_module_process_reqs (hio_dataset_t dataset, hio_internal_request_t **reqs, int req_count);
static int builtin_posix_module_dataset_manifest_list_all (const char *path, int **manifest_ids, size_t *count, size_t nnodes);


static void builtin_posix_trace (builtin_posix_module_dataset_t *posix_dataset, const char *event,
                                 int64_t value, uint64_t value2, uint64_t start, uint64_t stop) {
  if (NULL == posix_dataset->ds_trace_fh) {
    return;
  }

  fprintf (posix_dataset->ds_trace_fh, "%s::%" PRId64 ":%s:%" PRIu64 ":%" PRIu64 ":%" PRIu64 ":%" PRIu64 ":%" PRIu64 "\n",
           hioi_object_identifier (&posix_dataset->base), posix_dataset->base.ds_id, event, value, value2, start, stop,
           stop - start);
}

#define POSIX_TRACE_CALL(ds, c, e, v, v2)                       \
  do {                                                          \
    uint64_t _start, _stop;                                     \
    _start = hioi_gettime ();                                   \
    c;                                                          \
    _stop = hioi_gettime ();                                    \
    builtin_posix_trace ((ds), e, v, v2, _start, _stop);        \
  } while (0)

static int builtin_posix_dataset_path (struct hio_module_t *module, char **path, const char *name, uint64_t set_id) {
  hio_context_t context = module->context;
  int rc;

  rc = asprintf (path, "%s/%s.hio/%s/%lu", module->data_root, hioi_object_identifier(context), name,
                 (unsigned long) set_id);
  return (0 > rc) ? hioi_err_errno (errno) : HIO_SUCCESS;
}

static int builtin_posix_create_dataset_dirs (builtin_posix_module_t *posix_module, builtin_posix_module_dataset_t *posix_dataset) {
  mode_t access_mode = posix_module->access_mode;
  hio_context_t context = posix_module->base.context;
  char *path;
  int rc;

  if (context->c_rank > 0) {
    return HIO_SUCCESS;
  }

  /* create the data directory*/
  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "posix: creating dataset directory @ %s", posix_dataset->base_path);

  rc = asprintf (&path, "%s/data", posix_dataset->base_path);
  if (0 > rc) {
    return hioi_err_errno (errno);
  }

  rc = hioi_mkpath (context, path, access_mode);
  if (0 > rc || EEXIST == errno) {
    if (EEXIST != errno) {
      hioi_err_push (hioi_err_errno (errno), &context->c_object, "posix: error creating context directory: %s",
                    path);
    }
    free (path);

    return hioi_err_errno (errno);
  }

  /* set striping parameters on the directory */
  if (posix_dataset->base.ds_fsattr.fs_flags & HIO_FS_SUPPORTS_STRIPING) {
    rc = hioi_fs_set_stripe (path, &posix_dataset->base.ds_fsattr);
    if (HIO_SUCCESS != rc) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: could not set file system striping on %s", path);
    }
  }

  free (path);

  /* create trace directory if requested */
  if (context->c_enable_tracing) {
    rc = asprintf (&path, "%s/trace/posix", posix_dataset->base_path);
    if (0 > rc) {
      return hioi_err_errno (errno);
    }

    rc = hioi_mkpath (context, path, access_mode);
    if (0 > rc || EEXIST == errno) {
      if (EEXIST != errno) {
        hioi_err_push (hioi_err_errno (errno), &context->c_object, "posix: error creating context directory: %s",
                       path);
      }
      free (path);

      return hioi_err_errno (errno);
    }
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: successfully created dataset directories %s", posix_dataset->base_path);

  return HIO_SUCCESS;
}

int builtin_posix_module_dataset_list_internal (struct hio_module_t *module, const char *name,
                                                hio_dataset_header_t **headers, int *count) {
  hio_context_t context = module->context;
  int num_set_ids = 0, set_id_index = *count;
  hio_manifest_t manifest = NULL;
  int rc = HIO_SUCCESS;
  void *tmp;
  struct dirent *dp;
  char *path = NULL;
  DIR *dir = NULL;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "posix:dataset_list: listing dataset ids for dataset %s on data root %s",
            name, module->data_root);

  rc = asprintf (&path, "%s/%s.hio/%s", module->data_root, hioi_object_identifier(context), name);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  do {
    dir = opendir (path);
    if (NULL == dir) {
      rc = HIO_ERR_NOT_FOUND;
      break;
    }

    while (NULL != (dp = readdir (dir))) {
      if (dp->d_name[0] != '.') {
        num_set_ids++;
      }
    }

    if (0 == num_set_ids) {
      break;
    }

    tmp = realloc (*headers, (num_set_ids + *count) * sizeof (**headers));
    if (NULL == tmp) {
      num_set_ids = 0;
      break;
    }
    *headers = (hio_dataset_header_t *) tmp;

    rewinddir (dir);

    while (NULL != (dp = readdir (dir))) {
      hio_dataset_header_t *header = headers[0] + set_id_index;
      char *manifest_path, *tmp;
      int64_t ds_id;

      if ('.' == dp->d_name[0]) {
        continue;
      }

      /* verify that this is a valid dataset. at this time all identifiers MUST be unsigned integers */
      errno = 0;
      ds_id = strtol (dp->d_name, &tmp, 0);
      if (0 != errno || '\0' != *tmp) {
        hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_list: found non-integer dataset id: %s",
                  dp->d_name);
        continue;
      }

      hioi_log (context, HIO_VERBOSE_DEBUG_MED, "posix:dataset_list: processing dataset %s::%" PRId64,
                name, ds_id);

      rc = asprintf (&manifest_path, "%s/%s/manifest.json.bz2", path, dp->d_name);
      assert (0 <= rc);

      if (F_OK != access (manifest_path, R_OK)) {
        free (manifest_path);
        rc = asprintf (&manifest_path, "%s/%s/manifest.json", path, dp->d_name);
        assert (0 <= rc);
      }

      rc = hioi_manifest_read (context, manifest_path, &manifest);
      if (HIO_SUCCESS != rc) {
        hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_list: could not read manifest at path: %s. rc: %d",
                  manifest_path, rc);
      }

      rc = hioi_manifest_read_header (manifest, header);
      if (HIO_SUCCESS != rc) {
        if (NULL != manifest) {
          hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_list: error parsing manifest at path: %s. rc: %d",
                    manifest_path, rc);
        }

        /* directory exists but there is a broken/no manifest. still include this manifest in the list */
        strncpy (header->ds_name, name, sizeof (header->ds_name));
        header->ds_id = ds_id;
        header->ds_status = HIO_ERR_NOT_AVAILABLE;
        header->ds_mtime = 0;
      }

      hioi_manifest_release (manifest);
      free (manifest_path);
      manifest = NULL;

      ++set_id_index;
      rc = HIO_SUCCESS;
    }

    num_set_ids = set_id_index;
  } while (0);

  if (dir) {
    closedir (dir);
  }

  free (path);

  *count = num_set_ids;

  return rc;
}

static int builtin_posix_module_dataset_list (struct hio_module_t *module, const char *name,
                                              hio_dataset_header_t **headers, int *count) {
  int rc = HIO_SUCCESS, num_sets = *count, new_sets;
  hio_context_t context = module->context;
  struct dirent *dp;
  char *path = NULL;
  DIR *dir;

  if (0 == context->c_rank) {
    if (NULL == name) {
      /* find all datasets in the context */
      rc = asprintf (&path, "%s/%s.hio", module->data_root, hioi_object_identifier(context));
      assert (0 <= rc);

      dir = opendir (path);
      free (path);
      if (NULL == dir) {
        return HIO_ERR_NOT_FOUND;
      }

      while (NULL != (dp = readdir (dir))) {
        if ('.' == dp->d_name[0]) {
          continue;
        }

        rc = builtin_posix_module_dataset_list_internal (module, dp->d_name, headers, &num_sets);
      }

      closedir (dir);
    } else {
      rc = builtin_posix_module_dataset_list_internal (module, name, headers, &num_sets);
    }

    if (HIO_SUCCESS != rc) {
      num_sets = rc;
    }
  }

#if HIO_MPI_HAVE(1)
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (&num_sets, 1, MPI_INT, 0, context->c_comm);
  }
#endif

  new_sets = num_sets - *count;

  if (0 == new_sets) {
    return HIO_SUCCESS;
  }

  if (0 > num_sets) {
    return num_sets;
  }

  if (0 != context->c_rank) {
    *headers = (hio_dataset_header_t *) realloc (*headers, num_sets * sizeof (**headers));
    assert (NULL != *headers);
  }

#if HIO_MPI_HAVE(1)
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (*headers + *count, sizeof (**headers) * new_sets, MPI_BYTE, 0, context->c_comm);
  }
#endif

  /* set the correct module pointer for this rank */
  for (int i = *count ; i < num_sets ; ++i) {
    headers[0][i].module = module;
  }

  *count = num_sets;

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_dump_specific (struct hio_module_t *module, const char *name, int64_t id,
                                                       uint32_t flags, int rank, FILE *fh) {
  hio_context_t context = module->context;
  hio_manifest_t manifest, manifest2;
  char *path = NULL, *manifest_path;
  int rc = HIO_SUCCESS;

  rc = asprintf (&path, "%s/%s.hio/%s/%" PRId64, module->data_root, hioi_object_identifier(context), name, id);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  rc = asprintf (&manifest_path, "%s/manifest.json.bz2", path);
  if (0 > rc) {
    free (path);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  if (F_OK != access (manifest_path, R_OK)) {
    free (manifest_path);
    rc = asprintf (&manifest_path, "%s/manifest.json", path);
    if (0 > rc) {
      free (path);
      return HIO_ERR_OUT_OF_RESOURCE;
    }
  }

  rc = hioi_manifest_read (context, manifest_path, &manifest);
  if (HIO_SUCCESS != rc) {
    hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_list: could not load manifest from path: %s. rc: %d",
              manifest_path, rc);
    free (path);
    free (manifest_path);
    return rc;
  }
  free (manifest_path);

  if (flags & HIO_DUMP_FLAG_ELEMENTS) {
    int *manifest_ids;
    size_t manifest_count;
    rc = builtin_posix_module_dataset_manifest_list_all (path, &manifest_ids, &manifest_count, 1);
    if (HIO_SUCCESS == rc) {
      for (size_t i = 0 ; i < manifest_count ; ++i) {
        /* check for compressed manifest first */
        rc = asprintf (&manifest_path, "%s/manifest.%x.json.bz2", path, manifest_ids[i]);
        assert (0 < rc);
        if (F_OK != access(manifest_path, R_OK)) {
          free (manifest_path);
          /* compressed manifest not found. see if an uncompressed manifest exists */
          rc = asprintf (&manifest_path, "%s/manifest.%x.json", path, manifest_ids[i]);
          assert (0 < rc);
        }

        rc = hioi_manifest_read (context, manifest_path, &manifest2);
        free (manifest_path);
        if (HIO_SUCCESS != rc) {
          fprintf (stderr, "Error reading manifest\n");
          continue;
        }

        rc = hioi_manifest_merge_data (manifest, manifest2);
        if (HIO_SUCCESS != rc) {
          fprintf (stderr, "Error merging manifests\n");
          continue;
        }

        hioi_manifest_release (manifest2);
      }
    }
  }

  rc = hioi_manifest_dump (manifest, flags, rank, fh);
  if (HIO_SUCCESS != rc) {
    hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_list: could not dump manifest at path: %s. rc: %d",
              path, rc);
  }

  free (path);

  hioi_manifest_release (manifest);

  return rc;
}

static int builtin_posix_module_dataset_dump_internal (struct hio_module_t *module, const char *name, int64_t id,
                                                       uint32_t flags, int rank, FILE *fh) {
  hio_context_t context = module->context;
  char *path = NULL, *tmp;
  int rc = HIO_SUCCESS;
  struct dirent *dp;
  int64_t dir_id;
  DIR *dir;

  if (id > 0) {
    return builtin_posix_module_dataset_dump_specific (module, name, id, flags, rank, fh);
  }

  rc = asprintf (&path, "%s/%s.hio/%s", module->data_root, hioi_object_identifier(context), name);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  dir = opendir (path);
  free (path);
  if (NULL == dir) {
    return HIO_ERR_NOT_FOUND;
  }

  while (NULL != (dp = readdir (dir))) {
    if ('.' == dp->d_name[0]) {
      continue;
    }

    errno = 0;
    dir_id = strtol (dp->d_name, &tmp, 0);
    if (errno || tmp[0]) {
      /* non-numeric directory names are not currently supported */
      continue;
    }

    (void) builtin_posix_module_dataset_dump_specific (module, name, dir_id, flags, rank, fh);
  }

  closedir (dir);

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_dump (struct hio_module_t *module, const char *name, int64_t id,
                                              uint32_t flags, int rank, FILE *fh) {
  hio_context_t context = module->context;
  int rc = HIO_SUCCESS;
  struct dirent *dp;
  char *path = NULL;
  DIR *dir;

  if (NULL == name) {
    /* find all datasets in the context */
    rc = asprintf (&path, "%s/%s.hio", module->data_root, hioi_object_identifier(context));
    assert (0 <= rc);

    dir = opendir (path);
    free (path);
    if (NULL == dir) {
      return HIO_ERR_NOT_FOUND;
    }

    while (NULL != (dp = readdir (dir))) {
      if ('.' == dp->d_name[0]) {
        continue;
      }

      rc = builtin_posix_module_dataset_dump_internal (module, dp->d_name, -1, flags, rank, fh);
    }

    closedir (dir);
  } else {
    rc = builtin_posix_module_dataset_dump_internal (module, name, id, flags, rank, fh);
  }

  return rc;
}

static int manifest_index_compare (const void *a, const void *b) {
  int ia = ((int *) a)[0], ib = ((int *) b)[0];
  return ia - ib;
}

static int builtin_posix_module_dataset_manifest_list_all (const char *path, int **manifest_ids, size_t *count, size_t nnodes) {
  int num_manifest_ids = 0, manifest_id_index = 0;
  unsigned int manifest_id;
  int *tmp = NULL;
  struct dirent *dp;
  DIR *dir;

  *manifest_ids = NULL;
  *count = 0;

  dir = opendir (path);
  if (NULL == dir) {
    return hioi_err_errno (errno);
  }

  while (NULL != (dp = readdir (dir))) {
    if (dp->d_name[0] != '.' && 0 != sscanf (dp->d_name, "manifest.%x.json", &manifest_id)) {
      ++num_manifest_ids;
    }
  }

  if (0 == num_manifest_ids) {
    closedir (dir);
    return HIO_ERR_NOT_FOUND;
  }

  /* round up to a multiple of the number nodes */
  num_manifest_ids = nnodes * (num_manifest_ids + nnodes - 1) / nnodes;

  tmp = (int *) malloc (num_manifest_ids * sizeof (int));
  assert (NULL != tmp);
  memset (tmp, 0xff, sizeof (int) * num_manifest_ids);

  rewinddir (dir);

  while (NULL != (dp = readdir (dir))) {
    if ('.' == dp->d_name[0] || 0 == sscanf (dp->d_name, "manifest.%x.json", &manifest_id)) {
      continue;
    }

    tmp[manifest_id_index++] = (int) manifest_id;
  }

  /* put manifest files in numerical order */
  qsort (tmp, manifest_id_index, sizeof (int), manifest_index_compare);

  closedir (dir);

  *count = num_manifest_ids;
  *manifest_ids = tmp;

  return HIO_SUCCESS;
}

#if HIO_MPI_HAVE(3)
static int builtin_posix_module_dataset_manifest_list (builtin_posix_module_dataset_t *posix_dataset, int **manifest_ids, size_t *count) {
  hio_context_t context = hioi_object_context (&posix_dataset->base.ds_object);
  int num_manifest_ids = 0;
  int rc = HIO_SUCCESS;
  int *tmp = NULL;

  *manifest_ids = NULL;
  *count = 0;

  if (0 != context->c_shared_rank || !hioi_context_using_mpi (context)) {
    return HIO_SUCCESS;
  }

  if (0 == context->c_rank) {
    rc = builtin_posix_module_dataset_manifest_list_all (posix_dataset->base_path, &tmp, count, context->c_node_count);

    num_manifest_ids = (HIO_SUCCESS != rc) ? rc : *count / context->c_node_count;
  }

  MPI_Bcast (&num_manifest_ids, 1, MPI_INT, 0, context->c_node_leader_comm);

  if (0 < num_manifest_ids) {
    *manifest_ids = (int *) malloc (num_manifest_ids * sizeof (int));
    assert (NULL != *manifest_ids);

    MPI_Scatter (tmp, num_manifest_ids, MPI_INT, *manifest_ids, num_manifest_ids, MPI_INT,
                 0, context->c_node_leader_comm);
  }

  free (tmp);

  *count = num_manifest_ids;

  return num_manifest_ids >= 0 ? HIO_SUCCESS : num_manifest_ids;
}
#endif /* HIO_MPI_HAVE(3) */

static int builtin_posix_module_dataset_init (struct hio_module_t *module,
                                              builtin_posix_module_dataset_t *posix_dataset) {
  hio_context_t context = hioi_object_context ((hio_object_t) posix_dataset);
  int rc;

  rc = asprintf (&posix_dataset->base_path, "%s/%s.hio/%s/%lu", module->data_root,
                 hioi_object_identifier(context), hioi_object_identifier (posix_dataset),
                 (unsigned long) posix_dataset->base.ds_id);
  assert (0 < rc);

  /* initialize posix dataset specific data */
  for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
    posix_dataset->files[i].f_bid = -1;
    posix_dataset->files[i].f_hndl = NULL;
    posix_dataset->files[i].f_fd = -1;
  }

  /* default to strided output mode */
  posix_dataset->ds_fmode = HIO_FILE_MODE_STRIDED;
  hioi_config_add (context, &posix_dataset->base.ds_object, &posix_dataset->ds_fmode,
                   "dataset_file_mode", NULL, HIO_CONFIG_TYPE_INT32, &hioi_dataset_file_modes,
                   "Modes for writing dataset files. Valid values: (0: basic, 1: file_per_node, 2: strided)", 0);

  if (HIO_FILE_MODE_STRIDED == posix_dataset->ds_fmode && HIO_SET_ELEMENT_UNIQUE == posix_dataset->base.ds_mode) {
    /* strided mode only applies to shared datasets */
    posix_dataset->ds_fmode = HIO_FILE_MODE_BASIC;
  }

  if (HIO_FILE_MODE_BASIC != posix_dataset->ds_fmode) {
    posix_dataset->ds_bs = 1ul << 23;
    hioi_config_add (context, &posix_dataset->base.ds_object, &posix_dataset->ds_bs,
                     "dataset_block_size", NULL, HIO_CONFIG_TYPE_INT64, NULL,
                     "Block size to use when writing in optimized mode (default: 8M)", 0);
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_setup_striping (hio_context_t context, struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  hio_fs_attr_t *fs_attr = &dataset->ds_fsattr;
  int rc;

  /* query the filesystem for current striping parameters */
  rc = hioi_fs_query (context, module->data_root, fs_attr);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, &context->c_object, "posix: error querying the filesystem");
    return rc;
  }

  /* for now do not use stripe exclusivity in any path */
  posix_dataset->my_stripe = 0;

  /* set default stripe count */
  fs_attr->fs_scount = 1;

  posix_dataset->ds_fcount = 1;

  if (fs_attr->fs_flags & HIO_FS_SUPPORTS_STRIPING) {
    if (HIO_FILE_MODE_OPTIMIZED == posix_dataset->ds_fmode) {
      posix_dataset->ds_stripe_exclusivity = false;
      hioi_config_add (context, &dataset->ds_object, &posix_dataset->ds_stripe_exclusivity,
                       "posix_stripe_exclusivity", NULL, HIO_CONFIG_TYPE_BOOL, NULL, "Each rank will write to "
                       "its own stripe. This will potentially increase the metadata size associated "
                       "with the dataset", 0);

      /* pick a reasonable default stripe size */
      fs_attr->fs_ssize = 1 << 24;

      /* use group locking if available as we guarantee stripe exclusivity in optimized mode */
      fs_attr->fs_lock_strategy = HIO_FS_LOCK_GROUP;
      hioi_config_add (context, &dataset->ds_object, &fs_attr->fs_lock_strategy,
                       "lock_mode", NULL, HIO_CONFIG_TYPE_INT32, &hioi_dataset_lock_strategies,
                       "Lock mode for underlying files. default - Use filesystem default, "
                       " group - Use group locking, disabled - Disable locking", 0);

#if HIO_MPI_HAVE(3)
      /* if group locking is not available then each rank should attempt to write to
       * a different stripe to maximize the available IO bandwidth */
      fs_attr->fs_scount = min(context->c_shared_size, fs_attr->fs_smax_count);
#endif
    } else if (HIO_FILE_MODE_STRIDED == posix_dataset->ds_fmode) {
      /* pick a reasonable default stripe size */
      fs_attr->fs_ssize = posix_dataset->ds_bs;

#if HIO_MPI_HAVE(3)
      fs_attr->fs_scount = min(context->c_shared_size, fs_attr->fs_smax_count);
#else
      fs_attr->fs_scount = min(16, fs_attr->fs_smax_count);
#endif
      posix_dataset->ds_fcount = context->c_size / fs_attr->fs_scount;
      hioi_config_add (context, &dataset->ds_object, &posix_dataset->ds_fcount,
                       "dataset_file_count", NULL, HIO_CONFIG_TYPE_UINT64, NULL, "Number of files to use "
                       "in strided file mode", 0);
    } else if (HIO_SET_ELEMENT_UNIQUE != dataset->ds_mode) {
      /* set defaults striping count */
      fs_attr->fs_ssize = 1 << 20;
      fs_attr->fs_scount = max (1, (unsigned) ((float) fs_attr->fs_smax_count * 0.9));
    } else {
      fs_attr->fs_ssize = 1 << 20;
    }

    hioi_config_add (context, &dataset->ds_object, &fs_attr->fs_scount,
                     "stripe_count", NULL, HIO_CONFIG_TYPE_UINT32, NULL, "Stripe count for all dataset "
                     "data files", 0);

    hioi_config_add (context, &dataset->ds_object, &fs_attr->fs_ssize,
                     "stripe_size", NULL, HIO_CONFIG_TYPE_UINT64, NULL, "Stripe size for all dataset "
                     "data files", 0);

    if (fs_attr->fs_flags & HIO_FS_SUPPORTS_RAID) {
      hioi_config_add (context, &dataset->ds_object, &fs_attr->fs_raid_level,
                       "raid_level", NULL, HIO_CONFIG_TYPE_UINT64, NULL, "RAID level for dataset "
                       "data files. Keep in mind that some filesystems only support 1/2 RAID "
                       "levels", 0);
    }

    /* ensure stripe count is sane */
    if (fs_attr->fs_scount > fs_attr->fs_smax_count) {
      hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_open: requested stripe count %u exceeds the available resources. "
                "adjusting to maximum %u", fs_attr->fs_scount, fs_attr->fs_smax_count);
      fs_attr->fs_scount = fs_attr->fs_smax_count;
    }

    /* ensure the stripe size is a multiple of the stripe unit */
    fs_attr->fs_ssize = fs_attr->fs_sunit * ((fs_attr->fs_ssize + fs_attr->fs_sunit - 1) / fs_attr->fs_sunit);
    if (fs_attr->fs_ssize > fs_attr->fs_smax_size) {
      hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_open: requested stripe size %" PRIu64 " exceeds the maximum %"
                PRIu64 ". ", fs_attr->fs_ssize, fs_attr->fs_smax_size);
      fs_attr->fs_ssize = fs_attr->fs_smax_size;
    }

    if (HIO_FILE_MODE_OPTIMIZED == posix_dataset->ds_fmode && posix_dataset->ds_bs < fs_attr->fs_ssize) {
      posix_dataset->ds_bs = fs_attr->fs_ssize;
      if (posix_dataset->ds_stripe_exclusivity) {
        posix_dataset->my_stripe = context->c_shared_rank % fs_attr->fs_scount;
      }
    }
  }

  return HIO_SUCCESS;
}

#if HIO_MPI_HAVE(3)
static int bultin_posix_scatter_data (builtin_posix_module_dataset_t *posix_dataset) {
  hio_context_t context = hioi_object_context ((hio_object_t) posix_dataset);
  hio_manifest_t manifest = NULL, manifest2;
  size_t manifest_id_count = 0;
  int rc = HIO_SUCCESS;
  int *manifest_ids;
  char *path;

  if (HIO_SET_ELEMENT_UNIQUE == posix_dataset->base.ds_mode) {
    /* only read the manifest this rank wrote */
    manifest_id_count = 1;
    manifest_ids = malloc (sizeof (*manifest_ids));
    manifest_ids[0] = context->c_rank;
  } else {
    rc = builtin_posix_module_dataset_manifest_list (posix_dataset, &manifest_ids, &manifest_id_count);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  for (size_t i = 0 ; i < manifest_id_count ; ++i) {
    if (-1 == manifest_ids[i]) {
      /* nothing more to do */
      break;
    }

    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: reading manifest data from id %x\n",
              manifest_ids[i]);

    /* when writing the dataset in file_per_node mode each IO manager writes its own manifest. try
     * to open the manifest. if a manifest does not exist then it is likely this rank did not
     * write a manifest. IO managers will distribute the manifest data to the appropriate ranks
     * in hioi_dataset_scatter(). */
    rc = asprintf (&path, "%s/manifest.%x.json.bz2", posix_dataset->base_path, manifest_ids[i]);
    assert (0 < rc);

    if (access (path, F_OK)) {
      free (path);
      /* Check for a non-bzip'd manifest file. */
      rc = asprintf (&path, "%s/manifest.%x.json", posix_dataset->base_path, manifest_ids[i]);
      assert (0 < rc);
      if (access (path, F_OK)) {
        /* this might be a real error. we were told to read from a manifest but we couldn't
         * find it! */
        free (path);
        path = NULL;
      }
    }

    if (path) {
      /* read the manifest if it exists */
      rc = hioi_manifest_read (context, path, &manifest2);
      if (HIO_SUCCESS == rc) {
        if (NULL != manifest) {
          rc = hioi_manifest_merge_data (manifest, manifest2);
          hioi_manifest_release (manifest2);
        } else {
          manifest = manifest2;
        }
      }

      free (path);

      if (HIO_SUCCESS != rc) {
        break;
      }
    }
  }

  /* share dataset information with all processes on this node */
  if (HIO_SET_ELEMENT_UNIQUE == posix_dataset->base.ds_mode) {
    rc = hioi_dataset_scatter_unique (&posix_dataset->base, manifest, rc);
  } else {
    rc = hioi_dataset_scatter_comm (&posix_dataset->base, context->c_shared_comm, manifest, rc);
  }

  free (manifest_ids);

  if (manifest) {
    hioi_manifest_release (manifest);
  }

  return rc;
}
#endif

static int builtin_posix_module_dataset_open (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) module;
  hio_context_t context = hioi_object_context ((hio_object_t) dataset);
  hio_manifest_t manifest = NULL;
  uint64_t start, stop;
  int rc = HIO_SUCCESS;
  char *path = NULL;

  start = hioi_gettime ();

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "posix:dataset_open: opening dataset %s:%lu mpi: %d flags: 0x%x mode: 0x%x",
	    hioi_object_identifier (dataset), (unsigned long) dataset->ds_id, hioi_context_using_mpi (context),
            dataset->ds_flags, dataset->ds_mode);

  rc = builtin_posix_module_dataset_init (module, posix_dataset);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

#if HIO_MPI_HAVE(3)
  if (HIO_FILE_MODE_BASIC != posix_dataset->ds_fmode) {
    rc = hioi_context_generate_leader_list (context);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }
#endif

  rc = builtin_posix_module_setup_striping (context, module, dataset);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  /* NTH: need to dig a little deeper into this but stdio gives better performance when used
   * with datawarp. In all other cases the POSIX read/write calls appear to be faster. */
  if (HIO_FS_TYPE_DATAWARP == dataset->ds_fsattr.fs_type) {
    posix_dataset->ds_file_api = HIO_FAPI_STDIO;
  } else {
    posix_dataset->ds_file_api = HIO_FAPI_POSIX;
  }

  hioi_config_add (context, &posix_dataset->base.ds_object, &posix_dataset->ds_file_api,
                   "posix_file_api", NULL, HIO_CONFIG_TYPE_INT32, &builtin_posix_apis,
                   "API set to use for reading/writing files. This variable allows the user "
                   " to specify which API to use. Currently supported API are 0: posix (read/write)"
                   ", 1: stdio (fread/fwrite), or 2: pposix (pread/pwrite). The default is to use "
                   "posix", 0);

  if (HIO_FILE_MODE_OPTIMIZED == posix_dataset->ds_fmode) {
    posix_dataset->ds_use_bzip = true;
    hioi_config_add (context, &dataset->ds_object, &posix_dataset->ds_use_bzip,
                     "dataset_use_bzip", NULL, HIO_CONFIG_TYPE_BOOL, NULL,
                     "Use bzip2 compression for dataset manifests", 0);
  }

  if (dataset->ds_flags & HIO_FLAG_TRUNC) {
    /* blow away the existing dataset */
    if (0 == context->c_rank) {
      (void) builtin_posix_module_dataset_unlink (module, hioi_object_identifier(dataset),
                                                  dataset->ds_id);
    }
  }

  if (!(dataset->ds_flags & HIO_FLAG_CREAT)) {
    if (0 == context->c_rank) {
      /* load manifest. the manifest data will be shared with other processes in hioi_dataset_scatter */
      rc = asprintf (&path, "%s/manifest.json", posix_dataset->base_path);
      assert (0 < rc);
      if (access (path, F_OK)) {
        /* this should never happen on a valid dataset */
        free (path);
        hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: could not find top-level manifest %s", path);
        rc = HIO_ERR_NOT_FOUND;
      } else {
        rc = HIO_SUCCESS;
      }
    }

    /* read the manifest if it exists */
    if (HIO_SUCCESS == rc) {
      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: loading manifest header from %s...", path);
      rc = hioi_manifest_read (context, path, &manifest);
      free (path);
      path = NULL;
    }
  } else if (0 == context->c_rank) {
    rc = builtin_posix_create_dataset_dirs (posix_module, posix_dataset);
    if (HIO_SUCCESS == rc) {
      /* serialize the manifest to send to remote ranks */
      rc = hioi_manifest_generate (dataset, false, &manifest);
    }
  }

#if HIO_MPI_HAVE(1)
  /* share dataset header will all processes in the communication domain */
  rc = hioi_dataset_scatter_comm (dataset, context->c_comm, manifest, rc);
#endif
  hioi_manifest_release (manifest);
  if (HIO_SUCCESS != rc) {
    free (posix_dataset->base_path);
    return rc;
  }

  /* NTH: return an error if file_per_node was specified and could not be enabled. this limitation will
   * be lifted in a future release. */
  if (((HIO_FLAG_READ | HIO_FLAG_WRITE) & dataset->ds_flags) == (HIO_FLAG_READ | HIO_FLAG_WRITE) &&
      HIO_FILE_MODE_OPTIMIZED == posix_dataset->ds_fmode) {
    hioi_err_push (rc, &dataset->ds_object, "posix: it is not currently possible to use file_per_node mode with a read-write dataset");
    free (posix_dataset->base_path);
    return HIO_ERR_BAD_PARAM;
  }

  if (context->c_enable_tracing) {
    char *path;

    rc = asprintf (&path, "%s/trace/posix/trace.%d", posix_dataset->base_path, context->c_rank);
    if (rc > 0) {
      posix_dataset->ds_trace_fh = fopen (path, "a");
      free (path);
    }

    builtin_posix_trace (posix_dataset, "trace_begin", 0, 0, 0, 0);
  }

#if HIO_MPI_HAVE(3)
  if (!(dataset->ds_flags & HIO_FLAG_CREAT) && HIO_FILE_MODE_OPTIMIZED == posix_dataset->ds_fmode) {
    rc = bultin_posix_scatter_data (posix_dataset);
    if (HIO_SUCCESS != rc) {
      free (posix_dataset->base_path);
      return rc;
    }
  }

  /* if possible set up a shared memory window for this dataset */
  if (HIO_FILE_MODE_BASIC != posix_dataset->ds_fmode || HIO_SET_ELEMENT_SHARED == dataset->ds_mode) {
    POSIX_TRACE_CALL(posix_dataset, hioi_dataset_shared_init (dataset, dataset->ds_fsattr.fs_scount * posix_dataset->ds_fcount), "shared_init", 0, 0);
  }

  if (HIO_FILE_MODE_OPTIMIZED == posix_dataset->ds_fmode) {
    if (NULL == dataset->ds_shared_control) {
      /* no point in using optimized mode in this case */
      posix_dataset->ds_fmode = HIO_FILE_MODE_BASIC;
      hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_open: optimized file mode requested but not supported in this "
                "dataset mode. falling back to basic file mode, path: %s", posix_dataset->base_path);
    } else if (HIO_SET_ELEMENT_SHARED == dataset->ds_mode) {
      POSIX_TRACE_CALL(posix_dataset, rc = hioi_dataset_generate_map (dataset), "generate_map", 0, 0);
      if (HIO_SUCCESS != rc) {
        free (posix_dataset->base_path);
        return rc;
      }
    }
  }

  /* NTH: if requested more code is needed to load an optimized dataset with an older MPI */
#endif /* HIO_MPI_HAVE(3) */

  dataset->ds_module = module;
  dataset->ds_close = builtin_posix_module_dataset_close;
  dataset->ds_element_open = builtin_posix_module_element_open;
  dataset->ds_process_reqs = builtin_posix_module_process_reqs;

  /* record the open time */
  gettimeofday (&dataset->ds_otime, NULL);

  stop = hioi_gettime ();

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: successfully %s posix dataset "
            "%s:%" PRIu64 " on data root %s. open time %" PRIu64 " usec. file mode: %d",
            (dataset->ds_flags & HIO_FLAG_CREAT) ? "created" : "opened", hioi_object_identifier(dataset),
            dataset->ds_id, module->data_root, stop - start, posix_dataset->ds_fmode);

  builtin_posix_trace (posix_dataset, "open", 0, 0, start, stop);

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_close (hio_dataset_t dataset) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  hio_context_t context = hioi_object_context ((hio_object_t) dataset);
  hio_module_t *module = dataset->ds_module;
  hio_manifest_t manifest = NULL;
  uint64_t start, stop;
  int rc = HIO_SUCCESS;

  start = hioi_gettime ();

  for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
    if (posix_dataset->files[i].f_bid >= 0) {
      POSIX_TRACE_CALL(posix_dataset, hioi_file_close (posix_dataset->files + i), "file_close",
                       posix_dataset->files[i].f_bid, 0);
    }
  }

#if HIO_MPI_HAVE(3)
  /* release the shared state if it was allocated */
  (void) hioi_dataset_shared_fini (dataset);

  /* release the dataset map if one was allocated */
  (void) hioi_dataset_map_release (dataset);
#endif

  if (dataset->ds_flags & HIO_FLAG_WRITE) {
    char *path;

    /* write manifest header */
    POSIX_TRACE_CALL(posix_dataset, rc = hioi_dataset_gather_manifest (dataset, &manifest, true),
                     "gather_manifest", 0, 0);
    if (HIO_SUCCESS != rc) {
      dataset->ds_status = rc;
    }

    if (0 == context->c_rank) {
      rc = asprintf (&path, "%s/manifest.json", posix_dataset->base_path);
      if (0 > rc) {
        /* out of memory. not much we can do now */
        return hioi_err_errno (errno);
      }

      rc = hioi_manifest_save (manifest, false, path);
      hioi_manifest_release (manifest);
      free (path);
      if (HIO_SUCCESS != rc) {
        hioi_err_push (rc, &dataset->ds_object, "posix: error writing dataset manifest");
      }
    }

#if HIO_MPI_HAVE(3)
    if (HIO_FILE_MODE_OPTIMIZED == posix_dataset->ds_fmode) {
      /* optimized mode requires a data manifest to describe how the data landed on the filesystem */
      POSIX_TRACE_CALL(posix_dataset, rc = hioi_dataset_gather_manifest_comm (dataset, context->c_shared_comm, &manifest,
                                                                              false),
                       "gather_manifest", 0, 0);
      if (HIO_SUCCESS != rc) {
        dataset->ds_status = rc;
      }

      if (NULL != manifest) {
        rc = asprintf (&path, "%s/manifest.%x.json%s", posix_dataset->base_path, context->c_rank,
                       posix_dataset->ds_use_bzip ? ".bz2" : "");
        if (0 > rc) {
          return hioi_err_errno (errno);
        }

        rc = hioi_manifest_save (manifest, posix_dataset->ds_use_bzip, path);
        hioi_manifest_release (manifest);
        free (path);
        if (HIO_SUCCESS != rc) {
          hioi_err_push (rc, &dataset->ds_object, "posix: error writing dataset manifest");
        }
      }
    }
#endif
  }

#if HIO_MPI_HAVE(1)
  /* ensure all ranks have closed the dataset before continuing */
  if (hioi_context_using_mpi (context)) {
    MPI_Allreduce (MPI_IN_PLACE, &rc, 1, MPI_INT, MPI_MIN, context->c_comm);
  }
#endif

  free (posix_dataset->base_path);

  stop = hioi_gettime ();

  builtin_posix_trace (posix_dataset, "close", 0, 0, start, stop);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_close: successfully closed posix dataset "
            "%s:%" PRIu64 " on data root %s. close time %" PRIu64 " usec", hioi_object_identifier(dataset),
            dataset->ds_id, module->data_root, stop - start);

  builtin_posix_trace (posix_dataset, "trace_end", 0, 0, 0, 0);
  if (posix_dataset->ds_trace_fh) {
    fclose (posix_dataset->ds_trace_fh);
  }

  return rc;
}

#define UNLINK_MAX_THREADS 16

static pthread_mutex_t unlink_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_t unlink_threads[UNLINK_MAX_THREADS];
static uint32_t unlink_mask;
static int unlink_error;

static void *builtin_posix_unlink_thread (void *arg) {
  int ret = remove ((const char *) arg);
  if (ret != 0 && !unlink_error) {
    unlink_error = errno;
  }

  free (arg);

  return NULL;
}


static int builtin_posix_unlink_cb (const char *path, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
  char *new_path = strdup (path);
  static int last = 0;
  void *ret;

  if (FTW_DP == typeflag) {
    /* wait for all previous threads to exit before trying to remove a directory */
    for (int i = 0 ; i < UNLINK_MAX_THREADS ; ++i) {
      if (unlink_mask & (1 << i)) {
        pthread_join (unlink_threads[i], &ret);
      }
    }

    /* all threads clear */
    unlink_mask = 0;

    /* remove directory */
    builtin_posix_unlink_thread ((void *) new_path);
    return 0;
  }

  for (int i = 0 ; i < UNLINK_MAX_THREADS ; ++ i) {
    if (!(unlink_mask & (1 << i))) {
      unlink_mask |= (1 << i);
      pthread_create (unlink_threads + i, NULL, builtin_posix_unlink_thread, (void *) new_path);
      return 0;
    }
  }

  pthread_join (unlink_threads[last], &ret);
  last = (last + 1) % UNLINK_MAX_THREADS;
  pthread_create (unlink_threads + last, NULL, builtin_posix_unlink_thread, (void *) new_path);

  return 0;
}

static int builtin_posix_module_dataset_unlink (struct hio_module_t *module, const char *name, int64_t set_id) {
  struct stat statinfo;
  char *path = NULL;
  int rc;

  if (module->context->c_rank) {
    return HIO_ERR_NOT_AVAILABLE;
  }

  rc = builtin_posix_dataset_path (module, &path, name, set_id);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  if (stat (path, &statinfo)) {
    free (path);
    return hioi_err_errno (errno);
  }

  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "posix: unlinking existing dataset %s::%" PRId64,
            name, set_id);

  /* use tree walk depth-first to remove all of the files for this dataset */

  pthread_mutex_lock (&unlink_mutex);
  unlink_mask = 0;
  unlink_error = 0;
  (void) nftw (path, builtin_posix_unlink_cb, 32, FTW_DEPTH | FTW_PHYS);
  free (path);

  for (int i = 0 ; i < UNLINK_MAX_THREADS ; ++i) {
    void *ret;
    if (unlink_mask & (1 << i)) {
      pthread_join (unlink_threads[i], &ret);
    }
  }
  pthread_mutex_unlock (&unlink_mutex);

  if (unlink_error) {
    hioi_err_push (hioi_err_errno (unlink_error), &module->context->c_object, "posix: could not unlink dataset. errno: %d",
                  unlink_error);
  }

  return hioi_err_errno (unlink_error);
}

static int builtin_posix_open_file (builtin_posix_module_t *posix_module, builtin_posix_module_dataset_t *posix_dataset,
                                    char *path, hio_file_t *file) {
  hio_object_t hio_object = &posix_dataset->base.ds_object;
  int open_flags = 0, rc;

  /* determine the fopen file mode to use */
  if (HIO_FLAG_WRITE & posix_dataset->base.ds_flags) {
    open_flags |= O_CREAT | O_WRONLY;
  }

  if (HIO_FLAG_READ & posix_dataset->base.ds_flags) {
    open_flags |= O_RDONLY;
  }

  rc = hioi_file_open (file, path, open_flags, posix_dataset->ds_file_api, posix_module->access_mode);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, hio_object, "posix: error opening path %s. errno: %d", path, errno);
  }

  return rc;
}

static int builtin_posix_module_element_open_basic (builtin_posix_module_t *posix_module, builtin_posix_module_dataset_t *posix_dataset,
                                                    hio_element_t element) {
  const char *element_name = hioi_object_identifier(element);
  char *path;
  int rc;

  if (HIO_SET_ELEMENT_UNIQUE == posix_dataset->base.ds_mode) {
    rc = asprintf (&path, "%s/data/element_data.%s.%08d", posix_dataset->base_path, element_name,
                   element->e_rank);
  } else {
    rc = asprintf (&path, "%s/data/element_data.%s", posix_dataset->base_path, element_name);
  }

  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  if (F_OK != access (path, R_OK) && !(HIO_FLAG_WRITE & posix_dataset->base.ds_flags)) {
    /* fall back on old naming scheme */
    free (path);

    if (HIO_SET_ELEMENT_UNIQUE == posix_dataset->base.ds_mode) {
      rc = asprintf (&path, "%s/element_data.%s.%08d", posix_dataset->base_path, element_name,
                     element->e_rank);
    } else {
      rc = asprintf (&path, "%s/element_data.%s", posix_dataset->base_path, element_name);
    }

    if (0 > rc) {
      return hioi_err_errno (errno);
    }
  }

  POSIX_TRACE_CALL(posix_dataset, rc = builtin_posix_open_file (posix_module, posix_dataset, path, &element->e_file),
                   "file_open", 0, 0);
  free (path);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  element->e_size = element->e_file.f_size;

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_open (hio_dataset_t dataset, hio_element_t element) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) dataset->ds_module;
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  int rc;

  if (HIO_FILE_MODE_BASIC == posix_dataset->ds_fmode) {
    rc = builtin_posix_module_element_open_basic (posix_module, posix_dataset, element);
    if (HIO_SUCCESS != rc) {
      hioi_object_release (&element->e_object);
      return rc;
    }
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: %s element %p (identifier %s) for dataset %s",
	    (HIO_FLAG_WRITE & dataset->ds_flags) ? "created" : "opened", (void *) element,
            hioi_object_identifier(element), hioi_object_identifier(dataset));

  element->e_flush = builtin_posix_module_element_flush;
  element->e_complete = builtin_posix_module_element_complete;

  return HIO_SUCCESS;
}

/* reserve space in the local shared file for this rank's data */
static unsigned long builtin_posix_reserve (builtin_posix_module_dataset_t *posix_dataset, size_t *requested) {
  uint32_t stripe_count = posix_dataset->ds_stripe_exclusivity ? posix_dataset->base.ds_fsattr.fs_scount : 1;
  uint64_t block_size = posix_dataset->ds_bs;
  const int stripe = posix_dataset->my_stripe;
  unsigned long new_offset, to_use, space;
  int nstripes;

  if (posix_dataset->reserved_remaining) {
    to_use = (*requested > posix_dataset->reserved_remaining) ? posix_dataset->reserved_remaining : *requested;
    new_offset = posix_dataset->reserved_offset;

    /* update cached values */
    posix_dataset->reserved_offset += to_use;
    posix_dataset->reserved_remaining -= to_use;

    *requested = to_use;
    return new_offset;
  }

  space = *requested;

  if (space % posix_dataset->ds_bs) {
    space += posix_dataset->ds_bs - (space % posix_dataset->ds_bs);
  }

  if (1 < stripe_count && space > posix_dataset->ds_bs) {
    *requested = space = posix_dataset->ds_bs;
    nstripes = 1;
  } else {
    nstripes = space / posix_dataset->ds_bs;
  }

  unsigned long s_index = atomic_fetch_add (&posix_dataset->base.ds_shared_control->s_stripes[stripe].s_index, nstripes);
  new_offset = (s_index * stripe_count * block_size) + stripe * block_size;

  posix_dataset->reserved_offset = new_offset + *requested;
  posix_dataset->reserved_remaining = space - *requested;

  return new_offset;
}

static int builtin_posix_element_translate_strided (builtin_posix_module_t *posix_module, hio_element_t element,
                                                    uint64_t offset, size_t *size, hio_file_t **file_out) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  size_t block_id, block_base, block_bound, block_offset, file_id, file_block;
  hio_context_t context = hioi_object_context (&element->e_object);
  hio_file_t *file;
  int32_t file_index;
  char *path;
  int rc;

  block_id = offset / posix_dataset->ds_bs;

  file_id = block_id % posix_dataset->ds_fcount;
  file_block = block_id / posix_dataset->ds_fcount;

  block_base = block_id * posix_dataset->ds_bs;
  block_bound = block_base + posix_dataset->ds_bs;
  block_offset = file_block * posix_dataset->ds_bs + offset - block_base;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin_posix_element_translate_strided: element: %s, offset: %"
            PRIu64 ", file_id: %lu, file_block: %lu, block_offset: %lu, block_size: %" PRIu64,
            hioi_object_identifier(element), offset, file_id, file_id, block_offset, posix_dataset->ds_bs);

  if (offset + *size > block_bound) {
    *size = block_bound - offset;
  }

  rc = asprintf (&path, "%s/data/%s_block.%08lu", posix_dataset->base_path, hioi_object_identifier(element),
                 (unsigned long) file_id);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* use crc as a hash to pick a file index to use */
  file_index = file_id % HIO_POSIX_MAX_OPEN_FILES;
  file = posix_dataset->files + file_index;

  if (file_id != file->f_bid || file->f_element != element) {
    if (file->f_bid >= 0) {
      POSIX_TRACE_CALL(posix_dataset, hioi_file_close (file), "file_close", file->f_bid, 0);
    }
    file->f_bid = -1;

    file->f_element = element;

    POSIX_TRACE_CALL(posix_dataset, rc = builtin_posix_open_file (posix_module, posix_dataset, path, file),
                     "file_open", file_id, 0);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    file->f_bid = file_id;
  }

  POSIX_TRACE_CALL(posix_dataset, hioi_file_seek (file, block_offset, SEEK_SET), "file_seek", file->f_bid, block_offset);

  *file_out = file;

  return HIO_SUCCESS;
}

static int builtin_posix_element_translate_opt (builtin_posix_module_t *posix_module, hio_element_t element,
                                                uint64_t offset, size_t *size, hio_file_t **file_out,
                                                bool reading) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  hio_context_t context = hioi_object_context (&element->e_object);
  hio_file_t *file;
  uint64_t file_offset;
  int file_index = 0;
  char *path;
  int rc;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "translating element %s offset %" PRIu64 " size %lu",
            hioi_object_identifier (&element->e_object), offset, *size);
  POSIX_TRACE_CALL(posix_dataset, rc = hioi_element_translate_offset (element, offset, &file_index, &file_offset, size),
                   "translate_offset", offset, *size);
#if HIO_MPI_HAVE(3)
  if (HIO_SUCCESS != rc && reading) {
    POSIX_TRACE_CALL(posix_dataset, rc = hioi_dataset_map_translate_offset (element, offset, &file_index, &file_offset, size),
                     "map_translate_offset", offset, *size);
  }
#endif

  if (HIO_SUCCESS != rc) {
    if (reading) {
      hioi_log (context, HIO_VERBOSE_DEBUG_MED, "offset %" PRIu64 " not found", offset);
      /* not found */
      return rc;
    }

    file_offset = builtin_posix_reserve (posix_dataset, size);

    if (hioi_context_using_mpi (context)) {
      file_index = posix_dataset->base.ds_shared_control->s_master;
    } else {
      file_index = 0;
    }

    rc = asprintf (&path, "%s/data/data.%x", posix_dataset->base_path, file_index);
    if (0 > rc) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    hioi_element_add_segment (element, file_index, file_offset, offset, *size);
  } else {
    hioi_log (context, HIO_VERBOSE_DEBUG_MED, "offset found in file @ rank %d, offset %" PRIu64
              ", size %lu", file_index, file_offset, *size);
    rc = asprintf (&path, "%s/data/data.%x", posix_dataset->base_path, file_index);
    if (0 > rc) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    if (access (path, R_OK)) {
      free (path);
      rc = asprintf (&path, "%s/data.%x", posix_dataset->base_path, file_index);
      if (0 > rc) {
        return HIO_ERR_OUT_OF_RESOURCE;
      }
    }
  }

  /* use crc as a hash to pick a file index to use */
  int internal_index = file_index % HIO_POSIX_MAX_OPEN_FILES;
  file = posix_dataset->files + internal_index;

  if (file_index != file->f_bid) {
    if (file->f_bid >= 0) {
      POSIX_TRACE_CALL(posix_dataset, hioi_file_close (file), "file_close", file->f_bid, 0);
    }

    file->f_bid = -1;

    POSIX_TRACE_CALL(posix_dataset, rc = builtin_posix_open_file (posix_module, posix_dataset, path, file),
                     "file_open", file_index, 0);
    if (HIO_SUCCESS != rc) {
      free (path);
      return rc;
    }

    file->f_bid = file_index;
  }

  free (path);

  POSIX_TRACE_CALL(posix_dataset, hioi_file_seek (file, file_offset, SEEK_SET), "file_seek", file->f_bid, file_offset);

  *file_out = file;

  return HIO_SUCCESS;
}

static int builtin_posix_element_translate (builtin_posix_module_t *posix_module, hio_element_t element,
                                            uint64_t offset, size_t *size, hio_file_t **file_out, bool reading) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  int rc = HIO_SUCCESS;

  switch (posix_dataset->ds_fmode) {
  case HIO_FILE_MODE_BASIC:
    *file_out = &element->e_file;
    hioi_file_seek (&element->e_file, offset, SEEK_SET);
    break;
  case HIO_FILE_MODE_STRIDED:
    rc = builtin_posix_element_translate_strided (posix_module, element, offset, size, file_out);
    break;
  case HIO_FILE_MODE_OPTIMIZED:
    rc = builtin_posix_element_translate_opt (posix_module, element, offset, size, file_out, reading);
    break;
  }

  return rc;
}

static bool builtin_posix_stripe_lock (hio_element_t element, int stripe_id) {
  hio_dataset_t dataset = hioi_element_dataset (element);

  if (dataset->ds_shared_control) {
    /* locally lock the stripe */
    pthread_mutex_lock (&dataset->ds_shared_control->s_stripes[stripe_id].s_mutex);
    return true;
  }

  return false;
}

static void builtin_posix_stripe_unlock (hio_element_t element, int stripe_id) {
  hio_dataset_t dataset = hioi_element_dataset (element);

  if (dataset->ds_shared_control) {
    pthread_mutex_unlock (&dataset->ds_shared_control->s_stripes[stripe_id].s_mutex);
  }
}


static ssize_t builtin_posix_module_element_io_internal (builtin_posix_module_t *posix_module, hio_element_t element,
                                                         uint64_t offset, hio_iovec_t *iovec, int count, bool reading) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  size_t bytes_transferred = 0, total = 0, iov_index, iov_count, remaining, current;
  hio_dataset_t dataset = &posix_dataset->base;
  uint64_t stop, start, data;
  int rc, locked_stripe_id = -1;
  hio_file_t *file;
  ssize_t ret;

  assert ((!reading && dataset->ds_flags & HIO_FLAG_WRITE) || (reading && dataset->ds_flags & HIO_FLAG_READ));

  /* clean up the iovec if possible */
  count = hioi_iov_compress (iovec, count);

  for (int i = 0 ; i < count ; ++i) {
    total += iovec[i].size * iovec[i].count;
  }

  if (0 == total) {
    /* nothing to do */
    return 0;
  }

  start = hioi_gettime ();

  errno = 0;
  iov_index = 0;
  iov_count = iovec[0].count;
  data = iovec[0].base;
  remaining = iovec[0].size;

  do {
    size_t req = total, actual = total;

    /* translate as much as possible to minimize the number of manifest entries for this element */
    POSIX_TRACE_CALL(posix_dataset, rc = builtin_posix_element_translate (posix_module, element, offset, &actual,
                                                                          &file, reading),
                     "element_translate", offset, req);
    if (HIO_SUCCESS != rc) {
      break;
    }

    req = actual;

    do {
      current = actual < remaining ? actual : remaining;
      hioi_log (hioi_object_context (&element->e_object), HIO_VERBOSE_DEBUG_HIGH,
                "posix: %s %lu bytes at file offset %" PRIu64, (reading)?"reading":"writing", current, file->f_offset);

      /* If we are writing to the file we get better performance by reducing the contention on the
       * filesystem by locking before the write. Since this operation may be a network operation
       * in the future (currently it is local only) it is best to hold the lock until we are
       * done writing a particular stripe.  */
      if (!reading && (HIO_SET_ELEMENT_UNIQUE != posix_dataset->base.ds_mode || HIO_FILE_MODE_BASIC != posix_dataset->ds_fmode)) {
        uint64_t stripe = file->f_offset / dataset->ds_fsattr.fs_ssize;
        uint64_t stripe_bound = (stripe + 1) * dataset->ds_fsattr.fs_ssize;
        int next_stripe_id = (stripe % dataset->ds_fsattr.fs_scount);

        if (HIO_FILE_MODE_STRIDED == posix_dataset->ds_fmode) {
          next_stripe_id += file->f_bid * dataset->ds_fsattr.fs_scount;
        }

        if (current + file->f_offset > stripe_bound) {
          current = stripe_bound - file->f_offset;
        }

        /* lock this stripe if it is not already locked */
        if (next_stripe_id != locked_stripe_id) {
          if (locked_stripe_id >= 0) {
            /* unlock the last stripe */
            builtin_posix_stripe_unlock (element, locked_stripe_id);
          }

          if (builtin_posix_stripe_lock (element, next_stripe_id)) {
            locked_stripe_id = next_stripe_id;
          }
        }
      }

      /* perform actual io */
      if (reading) {
        POSIX_TRACE_CALL(posix_dataset, ret = hioi_file_read (file, (void *) data, current), "file_read", offset, actual);
      } else {
        POSIX_TRACE_CALL(posix_dataset, ret = hioi_file_write (file, (void *) data, current), "file_write", offset, actual);
      }

      if (ret > 0) {
        bytes_transferred += ret;
        actual -= current;
        remaining -= current;
      }

      if (ret < current) {
        /* short io */
        break;
      }

      data += current;
      offset += current;

      if (0 == remaining) {
        if (1 == iov_count) {
          /* finished with this entry */
          ++iov_index;
          if (iov_index < count) {
            iov_count = iovec[iov_index].count;
            data = iovec[iov_index].base;
          } else {
            /* should be nothing left. assert if there is */
            assert (0 == actual);
          }
        } else {
          /* move on to the next piece */
          data += iovec[iov_index].stride;
          --iov_count;
        }

        if (iov_index < count) {
          remaining = iovec[iov_index].size;
        }
      }
    } while (actual);

    if (HIO_SUCCESS != rc || actual) {
      break;
    }

    total -= req;
  } while (total);

  /* if we still have a stripe locked unlock it now */
  if (locked_stripe_id >= 0) {
    builtin_posix_stripe_unlock (element, locked_stripe_id);
  }

  if (0 == bytes_transferred || HIO_SUCCESS != rc) {
    if (0 == bytes_transferred) {
      rc = hioi_err_errno (errno);
    }

    dataset->ds_status = rc;
    return rc;
  }

  stop = hioi_gettime ();

  if (reading) {
    /* update read statistics */
    dataset->ds_stat.s_rtime += stop - start;

    if (0 < bytes_transferred) {
      dataset->ds_stat.s_bread += bytes_transferred;
    }
  } else {
    /* update size and write statistics */
    element->e_size = offset + bytes_transferred;
    dataset->ds_stat.s_wtime += stop - start;

    if (0 < bytes_transferred) {
      dataset->ds_stat.s_bwritten += bytes_transferred;
    }
  }

  hioi_log (hioi_object_context (&element->e_object), HIO_VERBOSE_DEBUG_LOW,
            "posix: finished %s. bytes transferred: %lu, time: %" PRIu64 " usec",
            reading ? "read" : "write", bytes_transferred, stop - start);

  return bytes_transferred;
}

static int builtin_posix_module_process_reqs (hio_dataset_t dataset, hio_internal_request_t **reqs, int req_count) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) dataset->ds_module;
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  uint64_t start, stop;
  int rc = HIO_SUCCESS;

  start = hioi_gettime ();

  hioi_object_lock (&dataset->ds_object);
  for (int i = 0 ; i < req_count ; ++i) {
    hio_internal_request_t *req = reqs[i];

    if (HIO_REQUEST_TYPE_READ == req->ir_type) {
      POSIX_TRACE_CALL(posix_dataset,
                       req->ir_status = builtin_posix_module_element_io_internal (posix_module, req->ir_element, req->ir_offset,
                                                                                  &req->ir_vec, 1, true),
                       "element_read", req->ir_offset, req->ir_vec.count * req->ir_vec.size);
    } else {
      POSIX_TRACE_CALL(posix_dataset,
                       req->ir_status = builtin_posix_module_element_io_internal (posix_module, req->ir_element, req->ir_offset,
                                                                                  &req->ir_vec, 1, false),
                       "element_write", req->ir_offset, req->ir_vec.count * req->ir_vec.size);
    }

    if (req->ir_urequest && req->ir_status > 0) {
      hio_request_t new_request = hioi_request_alloc (context);
      if (NULL == new_request) {
        rc = HIO_ERR_OUT_OF_RESOURCE;
        break;
      }

      req->ir_urequest[0] = new_request;
      new_request->req_transferred = req->ir_status;
      new_request->req_complete = true;
      new_request->req_status = HIO_SUCCESS;
    }

    if (req->ir_status < 0) {
      rc = (int) req->ir_status;
      break;
    }
  }

  hioi_object_unlock (&dataset->ds_object);

  stop = hioi_gettime ();

  builtin_posix_trace (posix_dataset, "process_requests", req_count, 0, start, stop);

  return rc;
}

static int builtin_posix_module_element_flush (hio_element_t element, hio_flush_mode_t mode) {
  builtin_posix_module_dataset_t *posix_dataset =
    (builtin_posix_module_dataset_t *) hioi_element_dataset (element);

  if (!(posix_dataset->base.ds_flags & HIO_FLAG_WRITE)) {
    return HIO_ERR_PERM;
  }

  if (HIO_FLUSH_MODE_COMPLETE != mode) {
    /* nothing to do at this time */
    return HIO_SUCCESS;
  }

  if (HIO_FILE_MODE_BASIC != posix_dataset->ds_fmode) {
    for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
      int ret = hioi_file_flush (posix_dataset->files + i);
      if (0 != ret) {
        return ret;
      }
    }

    return HIO_SUCCESS;
  }

  return hioi_file_flush (&element->e_file);
}

static int builtin_posix_module_element_complete (hio_element_t element) {
  hio_dataset_t dataset = hioi_element_dataset (element);

  /* reads in this module are always blocking. need to update this code if
   * that ever changes */
  if (!(dataset->ds_flags & HIO_FLAG_READ)) {
    return HIO_ERR_PERM;
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_fini (struct hio_module_t *module) {
  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "posix: finalizing module for data root %s",
	    module->data_root);

  free (module->data_root);
  free (module);

  return HIO_SUCCESS;
}

hio_module_t builtin_posix_module_template = {
  .dataset_open   = builtin_posix_module_dataset_open,
  .dataset_unlink = builtin_posix_module_dataset_unlink,

  .ds_object_size = sizeof (builtin_posix_module_dataset_t),

  .dataset_list   = builtin_posix_module_dataset_list,
  .dataset_dump   = builtin_posix_module_dataset_dump,

  .fini           = builtin_posix_module_fini,
  .version        = HIO_MODULE_VERSION_1,
};

static int builtin_posix_component_init (hio_context_t context) {
  /* nothing to do */
  return HIO_SUCCESS;
}

static int builtin_posix_component_fini (void) {
  /* nothing to do */
  return HIO_SUCCESS;
}

static int builtin_posix_component_query (hio_context_t context, const char *data_root,
					  const char *next_data_root, hio_module_t **module) {
  builtin_posix_module_t *new_module;

  if (0 == strncasecmp("datawarp", data_root, 8) || 0 == strncasecmp("dw", data_root, 2)) {
    return HIO_ERR_NOT_AVAILABLE;
  }

  if (0 == strncasecmp("posix:", data_root, 6)) {
    /* skip posix: */
    data_root += 6;
  }

  if (access (data_root, F_OK)) {
    hioi_err_push (hioi_err_errno (errno), &context->c_object, "posix: data root %s does not exist or can not be accessed",
                  data_root);
    return hioi_err_errno (errno);
  }

  new_module = calloc (1, sizeof (builtin_posix_module_t));
  if (NULL == new_module) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  memcpy (new_module, &builtin_posix_module_template, sizeof (builtin_posix_module_template));

  new_module->base.data_root = strdup (data_root);
  new_module->base.context = context;

  /* get the current umask */
  new_module->access_mode = umask (0);
  umask (new_module->access_mode);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: created module for data root %s. using umask %o",
	    data_root, new_module->access_mode);

  new_module->access_mode ^= 0777;
  *module = &new_module->base;

  return HIO_SUCCESS;
}

hio_component_t builtin_posix_component = {
  .init = builtin_posix_component_init,
  .fini = builtin_posix_component_fini,

  .query = builtin_posix_component_query,
  .flags = 0,
  .priority = 10,
  .version = HIO_COMPONENT_VERSION_1,
};
