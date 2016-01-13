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

#include "builtin-posix_component.h"

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

/** static functions */
static int builtin_posix_module_dataset_unlink (struct hio_module_t *module, const char *name, int64_t set_id);

static int builtin_posix_dataset_path (struct hio_module_t *module, char **path, const char *name, uint64_t set_id) {
  hio_context_t context = module->context;
  int rc;

  rc = asprintf (path, "%s/%s.hio/%s/%lu", module->data_root, context->c_object.identifier, name,
                 (unsigned long) set_id);
  return (0 > rc) ? hioi_err_errno (errno) : HIO_SUCCESS;
}

static int builtin_posix_create_dataset_dirs (builtin_posix_module_t *posix_module, builtin_posix_module_dataset_t *posix_dataset) {
  mode_t access_mode = posix_module->access_mode;
  hio_context_t context = posix_module->base.context;
  int rc;

  if (context->c_rank > 0) {
    return HIO_SUCCESS;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "posix: creating dataset directory @ %s", posix_dataset->base_path);

  rc = hio_mkpath (context, posix_dataset->base_path, access_mode);
  if (0 > rc || EEXIST == errno) {
    if (EEXIST != errno) {
      hio_err_push (hioi_err_errno (errno), context, NULL, "posix: error creating context directory: %s",
                    posix_dataset->base_path);
    }

    return hioi_err_errno (errno);
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: successfully created dataset directories");

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_list (struct hio_module_t *module, const char *name,
                                              hio_dataset_header_t **headers, int *count) {
  hio_context_t context = module->context;
  int num_set_ids = 0, set_id_index = 0;
  int rc = HIO_SUCCESS;
  struct dirent *dp;
  char *path = NULL;
  DIR *dir;

  *headers = NULL;
  *count = 0;

  do {
    if (0 != context->c_rank) {
      break;
    }

    rc = asprintf (&path, "%s/%s.hio/%s", module->data_root, context->c_object.identifier, name);
    assert (0 <= rc);

    dir = opendir (path);
    if (NULL == dir) {
      num_set_ids = 0;
      break;
    }

    while (NULL != (dp = readdir (dir))) {
      if (dp->d_name[0] != '.') {
        num_set_ids++;
      }
    }

    *headers = (hio_dataset_header_t *) calloc (num_set_ids, sizeof (**headers));
    assert (NULL != *headers);

    rewinddir (dir);

    while (NULL != (dp = readdir (dir))) {
      if ('.' == dp->d_name[0]) {
        continue;
      }

      char *manifest_path;

      rc = asprintf (&manifest_path, "%s/%s/manifest.json", path, dp->d_name);
      assert (0 <= rc);

      rc = hioi_manifest_read_header (context, headers[0] + set_id_index, manifest_path);
      if (HIO_SUCCESS == rc) {
        ++set_id_index;
      } else {
        /* skip dataset */
        hio_err_push (rc, context, NULL, "error reading manifest for %s", manifest_path);
      }

      free (manifest_path);
    }

    num_set_ids = set_id_index;
  } while (0);

#if HIO_USE_MPI
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (&num_set_ids, 1, MPI_INT, 0, context->c_comm);
  }
#endif

  if (0 == context->c_rank) {
    closedir (dir);
    free (path);
  }

  if (0 == num_set_ids) {
    free (*headers);
    *headers = NULL;

    return HIO_SUCCESS;
  }

  if (0 != context->c_rank) {
    *headers = (hio_dataset_header_t *) calloc (num_set_ids, sizeof (**headers));
    assert (NULL != *headers);
  }

#if HIO_USE_MPI
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (*headers, sizeof (**headers) * num_set_ids, MPI_BYTE, 0, context->c_comm);
  }
#endif

  *count = num_set_ids;

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_alloc (struct hio_module_t *module,
                                               hio_dataset_t *set_out, const char *name,
                                               int64_t set_id, int flags,
                                               hio_dataset_mode_t mode) {
  builtin_posix_module_dataset_t *posix_dataset;
  hio_context_t context = module->context;
  int rc;

  posix_dataset = (builtin_posix_module_dataset_t *)
    hioi_dataset_alloc (context, name, set_id, flags, mode,
                        sizeof (builtin_posix_module_dataset_t));
  if (NULL == posix_dataset) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  rc = asprintf (&posix_dataset->base_path, "%s/%s.hio/%s/%lu", module->data_root,
                 context->c_object.identifier, name, (unsigned long) set_id);
  assert (0 < rc);

  for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
    posix_dataset->files[i].f_bid = -1;
    posix_dataset->files[i].f_fh = NULL;
  }

  *set_out = &posix_dataset->base;

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_open (struct hio_module_t *module,
                                              hio_dataset_t *set_out, const char *name,
                                              int64_t set_id, int flags,
                                              hio_dataset_mode_t mode) {
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) module;
  hio_context_t context = module->context;
  builtin_posix_module_dataset_t *posix_dataset;
  hio_fs_attr_t *fs_attr;
  uint64_t start, stop;
  int rc = HIO_SUCCESS;
  char *path = NULL;

  start = hioi_gettime ();

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "posix:dataset_open: opening dataset %s:%lu mpi: %d flags: 0x%x mode: 0x%x",
	    name, (unsigned long) set_id, hioi_context_using_mpi (context), flags, mode);

  rc = builtin_posix_module_dataset_alloc (module, (hio_dataset_t *) &posix_dataset, name, set_id, flags, mode);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  fs_attr = &posix_dataset->base.fs_fsattr;

  rc = hioi_fs_query (context, module->data_root, fs_attr);
  if (HIO_SUCCESS != rc) {
    hio_err_push (rc, context, NULL, "posix: error querying the filesystem");
    return rc;
  }

  if (fs_attr->fs_flags & HIO_FS_SUPPORTS_STRIPING) {
    hioi_config_add (context, &posix_dataset->base.ds_object, &fs_attr->fs_scount,
                     "stripe_count", HIO_CONFIG_TYPE_UINT32, NULL, "Stripe count for all dataset "
                     "data files", 0);

    if (fs_attr->fs_scount > fs_attr->fs_smax_count) {
      hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_open: requested stripe count %u exceeds the available resources. "
                "adjusting to maximum %u", fs_attr->fs_scount, fs_attr->fs_smax_count);
      fs_attr->fs_scount = fs_attr->fs_smax_count;
    }

    hioi_config_add (context, &posix_dataset->base.ds_object, &fs_attr->fs_ssize,
                     "stripe_size", HIO_CONFIG_TYPE_UINT64, NULL, "Stripe size for all dataset "
                     "data files", 0);

    /* ensure the stripe size is a multiple of the stripe unit */
    fs_attr->fs_ssize = fs_attr->fs_sunit * ((fs_attr->fs_ssize + fs_attr->fs_sunit - 1) / fs_attr->fs_sunit);
    if (fs_attr->fs_ssize > fs_attr->fs_smax_size) {
      hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_open: requested stripe size %" PRIu64 " exceeds the maximum %"
                PRIu64 ". ", fs_attr->fs_ssize, fs_attr->fs_smax_size);
      fs_attr->fs_ssize = fs_attr->fs_smax_size;
    }
  }

  do {
    if (0 != context->c_rank) {
      break;
    }

    if (flags & HIO_FLAG_TRUNC) {
      /* blow away the existing dataset */
      (void) builtin_posix_module_dataset_unlink (module, name, set_id);

      /* ensure we take the create path later */
      flags |= HIO_FLAG_CREAT;
    }

    if (!(flags & HIO_FLAG_CREAT)) {
      /* load manifest. the manifest data will be shared with other processes in hioi_dataset_scatter */
      rc = asprintf (&path, "%s/manifest.json", posix_dataset->base_path);
      assert (0 < rc);

      if (access (path, F_OK)) {
        hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: could not find top-level manifest");
        rc = HIO_ERR_NOT_FOUND;
        break;
      }

      rc = hioi_manifest_load (&posix_dataset->base, path);
      free (path);
    } else {
      rc = builtin_posix_create_dataset_dirs (posix_module, posix_dataset);
    }

    posix_dataset->base.ds_flags = flags;
  } while (0);

  /* share dataset information will all processes in the communication domain */
  rc = hioi_dataset_scatter (&posix_dataset->base, rc);
  if (HIO_SUCCESS != rc) {
    free (posix_dataset->base_path);

    hioi_dataset_release ((hio_dataset_t *) &posix_dataset);
    return rc;
  }

  if (HIO_FILE_MODE_OPTIMIZED == posix_dataset->base.ds_fmode && HIO_SET_ELEMENT_UNIQUE == mode) {
    /* NTH: no optimized mode for N->N yet */
    posix_dataset->base.ds_fmode = HIO_FILE_MODE_BASIC;
    hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_open: optimized file mode requested but not supported in this "
              "dataset mode. falling back to basic file mode");
  }

  posix_dataset->base.ds_module = module;

  pthread_mutex_init (&posix_dataset->lock, NULL);

  /* record the open time */
  gettimeofday (&posix_dataset->base.ds_otime, NULL);

  stop = hioi_gettime ();

  *set_out = &posix_dataset->base;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: successfully %s posix dataset %s:%llu on data root %s. "
            "open time %lu usec", (flags & HIO_FLAG_CREAT) ? "created" : "opened", name, set_id, module->data_root,
            stop - start);

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_close (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  hio_context_t context = module->context;
  hio_element_t element;
  uint64_t start, stop;
  int rc = HIO_SUCCESS;

  start = hioi_gettime ();

  for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
    if (posix_dataset->files[i].f_fh) {
      fclose (posix_dataset->files[i].f_fh);
      posix_dataset->files[i].f_fh = NULL;
    }
  }

  hioi_list_foreach(element, dataset->ds_elist, struct hio_element, e_list) {
    if (element->e_fh) {
      fclose (element->e_fh);
      element->e_fh = NULL;
    }
  }

  if (dataset->ds_flags & HIO_FLAG_WRITE) {
    rc = hioi_dataset_gather (dataset);
    if (HIO_SUCCESS != rc) {
      dataset->ds_status = rc;
    }

    if (0 == context->c_rank) {
      char *path;

      rc = asprintf (&path, "%s/manifest.json", posix_dataset->base_path);
      if (0 < rc) {
        rc = hioi_manifest_save (dataset, path);
        free (path);
        if (HIO_SUCCESS != rc) {
          hio_err_push (rc, context, &dataset->ds_object, "posix: error writing dataset manifest");
        }
      } else {
        rc = HIO_ERR_OUT_OF_RESOURCE;
      }
    }
  }

#if HIO_USE_MPI
  /* ensure all ranks have closed the dataset before continuing */
  if (hioi_context_using_mpi (context)) {
    MPI_Allreduce (MPI_IN_PLACE, &rc, 1, MPI_INT, MPI_MIN, context->c_comm);
  }
#endif

  if (posix_dataset->base_path) {
    free (posix_dataset->base_path);
    posix_dataset->base_path = NULL;
  }

  pthread_mutex_destroy (&posix_dataset->lock);

  stop = hioi_gettime ();

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: successfully closed posix dataset %s:%llu on data root %s. "
            "close time %lu usec", dataset->ds_object.identifier, dataset->ds_id, module->data_root, stop - start);

  return rc;
}

static int builtin_posix_unlink_cb (const char *path, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
  return remove (path);
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

  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "posix: unlinking existing dataset %s::%llu",
            name, set_id);

  /* use tree walk depth-first to remove all of the files for this dataset */
  rc = nftw (path, builtin_posix_unlink_cb, 32, FTW_DEPTH | FTW_PHYS);
  free (path);
  if (0 > rc) {
    hio_err_push (hioi_err_errno (errno), module->context, NULL, "posix: could not unlink dataset. errno: %d",
                  errno);
    return hioi_err_errno (errno);
  }

  return HIO_SUCCESS;
}

static int builtin_posix_open_file (builtin_posix_module_t *posix_module, builtin_posix_module_dataset_t *posix_dataset,
                                    char *path, FILE **fh_out) {
  hio_object_t hio_object = &posix_dataset->base.ds_object;
  hio_context_t context = hioi_object_context (hio_object);
  hio_fs_attr_t *fs_attr = &posix_dataset->base.fs_fsattr;
  int open_flags;
  char *file_mode;
  int fd;

  /* determine the fopen file mode to use */
  if (HIO_FLAG_WRITE & posix_dataset->base.ds_flags) {
    file_mode = "w";
    open_flags = O_CREAT | O_WRONLY;
  } else {
    file_mode = "r";
    open_flags = O_RDONLY;
  }

  /* it is not possible to get open with create without truncation using fopen so use a
   * combination of open and fdopen to get the desired effect */
  //hioi_log (context, HIO_VERBOSE_DEBUG_HIGH, "posix: calling open; path: %s open_flags: %i", path, open_flags);
  fd = fs_attr->fs_open (path, fs_attr, open_flags, posix_module->access_mode);
  if (fd < 0) {
    hio_err_push (fd, context, hio_object, "posix: error opening element path %s. "
                  "errno: %d", path, errno);
    return fd;
  }

  //hioi_log (context, HIO_VERBOSE_DEBUG_HIGH, "posix: calling fdopen; fd: %d file_mode: %c", fd, file_mode);
  *fh_out = fdopen (fd, file_mode);
  if (NULL == *fh_out) {
    int hrc = hioi_err_errno (errno);
    hio_err_push (hrc, context, hio_object, "posix: error opening element file %s. "
                  "errno: %d", path, errno);
    return hrc;
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_open_basic (builtin_posix_module_t *posix_module, builtin_posix_module_dataset_t *posix_dataset,
                                                    hio_element_t element, int flags) {
  hio_context_t context = hioi_object_context (&posix_dataset->base.ds_object);
  const char *element_name = element->e_object.identifier;
  char *path;
  int rc;

  if (HIO_SET_ELEMENT_UNIQUE == posix_dataset->base.ds_mode) {
    rc = asprintf (&element->e_bfile, "element_data.%s.%05d", element_name,
                   context->c_rank);
  } else {
    rc = asprintf (&element->e_bfile, "element_data.%s", element_name);
  }

  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  rc = asprintf (&path, "%s/%s", posix_dataset->base_path, element->e_bfile);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  rc = builtin_posix_open_file (posix_module, posix_dataset, path, &element->e_fh);
  free (path);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  fseek (element->e_fh, 0, SEEK_END);
  element->e_size = ftell (element->e_fh);
  fseek (element->e_fh, 0, SEEK_SET);

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_open (struct hio_module_t *module, hio_dataset_t dataset,
                                              hio_element_t *element_out, const char *element_name,
                                              int flags) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) module;
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  hio_element_t element;
  int rc;

  hioi_list_foreach (element, dataset->ds_elist, struct hio_element, e_list) {
    if (!strcmp (element->e_object.identifier, element_name)) {
      *element_out = element;
      element->e_is_open = true;
      /* nothing more to do with optimized mode */
      return HIO_SUCCESS;
    }
  }

  element = hioi_element_alloc (dataset, element_name);
  if (NULL == element) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  if (HIO_FILE_MODE_BASIC == dataset->ds_fmode) {
    rc = builtin_posix_module_element_open_basic (posix_module, posix_dataset, element, flags);
    if (HIO_SUCCESS != rc) {
      hioi_element_release (element);
      return rc;
    }
  }

  hioi_dataset_add_element (dataset, element);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: %s element %p (identifier %s) for dataset %s",
	    (HIO_FLAG_WRITE & dataset->ds_flags) ? "created" : "opened", element, element_name,
            dataset->ds_object.identifier);

  *element_out = element;

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_close (struct hio_module_t *module, hio_element_t element) {
  element->e_is_open = false;
  return HIO_SUCCESS;
}

static int builtin_posix_element_translate (builtin_posix_module_t *posix_module, hio_element_t element, off_t offset,
                                            size_t *size, FILE **fh_out) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  hio_context_t context = hioi_object_context (&element->e_object);
  size_t block_id, block_base, block_bound, block_offset;
  builtin_posix_file_t *file;
  int32_t file_index;
  char *path;
  int rc;

  if (HIO_FILE_MODE_BASIC == posix_dataset->base.ds_fmode) {
    *fh_out = element->e_fh;
    fseek (*fh_out, offset, SEEK_SET);
    return HIO_SUCCESS;
  }

  block_id = offset / posix_dataset->base.ds_bs;
  block_base = block_id * posix_dataset->base.ds_bs;
  block_bound = block_base + posix_dataset->base.ds_bs;
  block_offset = offset - block_base;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin_posix_element_translate: element: %s, offset: %lu, block_id: %lu, "
            "block_offset: %lu, block_size: %lu", element->e_object.identifier, (unsigned long) offset,
            block_id, block_offset, posix_dataset->base.ds_bs);

  if (offset + *size > block_bound) {
    *size = block_bound - offset;
  }

  rc = asprintf (&path, "%s/%s_block.%lu", posix_dataset->base_path, element->e_object.identifier,
                 (unsigned long) block_id);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* use crc as a hash to pick a file index to use */
  file_index = hioi_crc32 ((uint8_t *) path, strlen (path)) % HIO_POSIX_MAX_OPEN_FILES;
  file = posix_dataset->files + file_index;

  if (block_id != file->f_bid || file->f_element != element) {
    if (file->f_fh) {
      fclose (file->f_fh);
      file->f_fh = NULL;
      file->f_bid = -1;
    }

    file->f_element = element;

    rc = builtin_posix_open_file (posix_module, posix_dataset, path, &file->f_fh);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    file->f_bid = block_id;
  }

  fseek (file->f_fh, block_offset, SEEK_SET);
  *fh_out = file->f_fh;

  return HIO_SUCCESS;
}

static ssize_t builtin_posix_module_element_write_strided_internal (struct hio_module_t *module, hio_element_t element,
                                                                    off_t offset, const void *ptr, size_t count, size_t size,
                                                                    size_t stride) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) module;
  size_t bytes_written = 0, ret;
  int rc;
  FILE *fh;

  if (!(count * size)) {
    return 0;
  }

  errno = 0;

  for (size_t i = 0 ; i < count ; ++i) {
    size_t req = size, actual;

    do {
      actual = req;

      rc = builtin_posix_element_translate (posix_module, element, offset, &actual, &fh);
      if (HIO_SUCCESS != rc) {
        return rc;
      }

      ret = fwrite (ptr, 1, actual, fh);
      if (ret > 0) {
        bytes_written += ret;
      }

      if (ret < actual) {
        /* short read */
        return bytes_written;
      }

      req -= actual;
      offset += actual;
      ptr = (void *) ((intptr_t) ptr + actual);
    } while (req);

    ptr = (void *) ((intptr_t) ptr + stride);
  }

  if (0 == bytes_written) {
    return hioi_err_errno (errno);
  }

  return bytes_written;
}

static int builtin_posix_module_element_write_strided_nb (struct hio_module_t *module, hio_element_t element,
                                                          hio_request_t *request, off_t offset, const void *ptr,
                                                          size_t count, size_t size, size_t stride) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  hio_context_t context = hioi_object_context (&element->e_object);
  ssize_t bytes_written;
  uint64_t stop, start;
  hio_request_t new_request;
  long file_offset;
  FILE *fh;

  if (!(posix_dataset->base.ds_flags & HIO_FLAG_WRITE)) {
    return HIO_ERR_PERM;
  }

  if (0 == stride) {
    size *= count;
    count = 1;
  }

  start = hioi_gettime ();

  pthread_mutex_lock (&posix_dataset->lock);
  bytes_written = builtin_posix_module_element_write_strided_internal (module, element, offset, ptr, count, size,
                                                                       stride);
  pthread_mutex_unlock (&posix_dataset->lock);

  stop = hioi_gettime ();
  posix_dataset->base.ds_stat.s_wtime += stop - start;

  if (0 < bytes_written) {
    posix_dataset->base.ds_stat.s_bwritten += bytes_written;
  }

  posix_dataset->base.ds_status = hioi_err_errno (errno);

  if (offset + bytes_written > element->e_size) {
    element->e_size = offset + bytes_written;
  }

  if (request) {
    new_request = hioi_request_alloc (context);
    if (NULL == new_request) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    *request = new_request;
    new_request->req_transferred = bytes_written;
    new_request->req_complete = true;
    new_request->req_status = posix_dataset->base.ds_status;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: finished write. bytes written: %lu, time: %llu usec",
            bytes_written, stop - start);

  return posix_dataset->base.ds_status;
}

static ssize_t builtin_posix_module_element_read_strided_internal (struct hio_module_t *module, hio_element_t element,
                                                                   off_t offset, void *ptr, size_t count, size_t size,
                                                                   size_t stride) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) module;
  size_t bytes_read = 0, ret;
  int rc;
  FILE *fh;

  if (!(count * size)) {
    return 0;
  }

  errno = 0;

  for (size_t i = 0 ; i < count ; ++i) {
    size_t req = size, actual;

    do {
      actual = req;

      rc = builtin_posix_element_translate (posix_module, element, offset, &actual, &fh);
      if (HIO_SUCCESS != rc) {
        return rc;
      }

      ret = fread (ptr, 1, actual, fh);
      if (ret > 0) {
        bytes_read += ret;
      }

      if (ret < actual) {
        /* short read */
        return bytes_read;
      }

      req -= actual;
      offset += actual;
      ptr = (void *) ((intptr_t) ptr + actual);
    } while (req);

    ptr = (void *) ((intptr_t) ptr + stride);
  }

  if (0 == bytes_read) {
    return hioi_err_errno (errno);
  }

  return bytes_read;
}

static int builtin_posix_module_element_read_strided_nb (struct hio_module_t *module, hio_element_t element,
                                                         hio_request_t *request, off_t offset, void *ptr,
                                                         size_t count, size_t size, size_t stride) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  ssize_t bytes_read;
  hio_context_t context = hioi_object_context (&element->e_object);
  size_t remaining_size = size;
  uint64_t start, stop;
  hio_request_t new_request;
  off_t file_offset;
  int rc = HIO_SUCCESS;
  FILE *fh;

  if (!(posix_dataset->base.ds_flags & HIO_FLAG_READ)) {
    return HIO_ERR_PERM;
  }

  if (stride == 0) {
    size *= count;
    count = 1;
  }

  start = hioi_gettime ();

  pthread_mutex_lock (&posix_dataset->lock);
  bytes_read = builtin_posix_module_element_read_strided_internal (module, element, offset, ptr, count, size, stride);
  pthread_mutex_unlock (&posix_dataset->lock);

  stop = hioi_gettime ();
  posix_dataset->base.ds_stat.s_rtime += stop - start;

  if (0 < bytes_read) {
    posix_dataset->base.ds_stat.s_bread += bytes_read;
  } else if (0 > bytes_read) {
    rc = (int) bytes_read;
  }

  /* see if a request was requested */
  if (request) {
    new_request = hioi_request_alloc (context);
    if (NULL == new_request) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    *request = new_request;
    new_request->req_transferred = bytes_read;
    new_request->req_complete = true;
  }

  return rc;
}

static int builtin_posix_module_element_flush (struct hio_module_t *module, hio_element_t element,
                                               hio_flush_mode_t mode) {
  builtin_posix_module_dataset_t *posix_dataset =
    (builtin_posix_module_dataset_t *) hioi_element_dataset (element);

  if (!(posix_dataset->base.ds_flags & HIO_FLAG_WRITE)) {
    return HIO_ERR_PERM;
  }

  if (HIO_FILE_MODE_OPTIMIZED == posix_dataset->base.ds_fmode) {
    for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
      if (element == posix_dataset->files[i].f_element) {
        fflush (posix_dataset->files[i].f_fh);
      }
    }
  } else {
    if (element->e_fh) {
      fflush (element->e_fh);
    }
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_complete (struct hio_module_t *module, hio_element_t element) {
  hio_dataset_t dataset = hioi_element_dataset (element);

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
  .dataset_open     = builtin_posix_module_dataset_open,
  .dataset_close    = builtin_posix_module_dataset_close,
  .dataset_unlink   = builtin_posix_module_dataset_unlink,

  .element_open     = builtin_posix_module_element_open,
  .element_close    = builtin_posix_module_element_close,

  .element_write_strided_nb = builtin_posix_module_element_write_strided_nb,
  .element_read_strided_nb  = builtin_posix_module_element_read_strided_nb,

  .element_flush    = builtin_posix_module_element_flush,
  .element_complete = builtin_posix_module_element_complete,

  .dataset_list     = builtin_posix_module_dataset_list,

  .fini             = builtin_posix_module_fini,
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

  if (0 == strncasecmp("datawarp", data_root, 8)) {
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: will not use module for datawarp root",
	      data_root);
    return HIO_ERR_NOT_AVAILABLE;
  }

  if (0 == strncasecmp("posix:", data_root, 6)) {
    /* skip posix: */
    data_root += 6;
  }

  if (access (data_root, F_OK)) {
    hio_err_push (hioi_err_errno (errno), context, NULL, "posix: data root %s does not exist or can not be accessed",
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
};
