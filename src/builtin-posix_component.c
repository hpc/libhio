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

  rc = asprintf (path, "%s/%s.hio/%s/%lu", module->data_root, context->context_object.identifier, name,
                 (unsigned long) set_id);
  return (0 > rc) ? hioi_err_errno (errno) : HIO_SUCCESS;
}

static int builtin_posix_create_dataset_dirs (builtin_posix_module_t *posix_module, builtin_posix_module_dataset_t *posix_dataset) {
  mode_t access_mode = posix_module->access_mode;
  hio_context_t context = posix_module->base.context;
  int rc;

  if (context->context_rank > 0) {
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
    if (0 != context->context_rank) {
      break;
    }

    rc = asprintf (&path, "%s/%s.hio/%s/", module->data_root, context->context_object.identifier, name);
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

      rc = asprintf (&manifest_path, "%s/%s/manifest.xml", path, dp->d_name);
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
    MPI_Bcast (&num_set_ids, 1, MPI_INT, 0, context->context_comm);
  }
#endif

  if (0 == context->context_rank) {
    closedir (dir);
    free (path);
  }

  if (0 == num_set_ids) {
    free (*headers);
    *headers = NULL;

    return HIO_SUCCESS;
  }

  if (0 != context->context_rank) {
    *headers = (hio_dataset_header_t *) calloc (num_set_ids, sizeof (**headers));
    assert (NULL != *headers);
  }

#if HIO_USE_MPI
  if (hioi_context_using_mpi (context)) {
    MPI_Bcast (*headers, sizeof (**headers) * num_set_ids, MPI_BYTE, 0, context->context_comm);
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
                 context->context_object.identifier, name, (unsigned long) set_id);
  assert (0 < rc);

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
  int rc = HIO_SUCCESS;
  char *path = NULL;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "posix:dataset_open: opening dataset %s:%lu mpi: %d flags: 0x%x mode: 0x%x",
	    name, (unsigned long) set_id, hioi_context_using_mpi (context), flags, mode);

  rc = builtin_posix_module_dataset_alloc (module, (hio_dataset_t *) &posix_dataset, name, set_id, flags, mode);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  fs_attr = &posix_dataset->base.dataset_fs_attr;

  rc = hioi_fs_query (context, module->data_root, fs_attr);
  if (HIO_SUCCESS != rc) {
    hio_err_push (rc, context, NULL, "posix: error querying the filesystem");
    return rc;
  }

  if (fs_attr->fs_flags & HIO_FS_SUPPORTS_STRIPING) {
    hioi_config_add (context, &posix_dataset->base.dataset_object, &fs_attr->fs_scount,
                     "stripe_count", HIO_CONFIG_TYPE_UINT64, NULL, "Stripe count for all dataset "
                     "data files", 0);

    hioi_config_add (context, &posix_dataset->base.dataset_object, &fs_attr->fs_ssize,
                     "stripe_size", HIO_CONFIG_TYPE_UINT64, NULL, "Stripe size for all dataset "
                     "data files", 0);
  }

  do {
    if (0 != context->context_rank) {
      break;
    }

    if (flags & HIO_FLAG_TRUNC) {
      /* blow away the existing dataset */
      (void) builtin_posix_module_dataset_unlink (module, name, set_id);

      /* ensure we take the create path later */
      flags |= HIO_FLAG_CREAT;
    }

    if (!(flags & HIO_FLAG_CREAT)) {
      rc = asprintf (&path, "%s/manifest.xml", posix_dataset->base_path);
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

    posix_dataset->base.dataset_flags = flags;
  } while (0);

  rc = hioi_dataset_scatter (&posix_dataset->base, rc);
  if (HIO_SUCCESS != rc) {
    free (posix_dataset->base_path);

    hioi_dataset_release ((hio_dataset_t *) &posix_dataset);
    return rc;
  }

  posix_dataset->fh = NULL;
  posix_dataset->base.dataset_module = module;

  pthread_mutex_init (&posix_dataset->lock, NULL);

  /* record the open time */
  gettimeofday (&posix_dataset->base.dataset_open_time, NULL);

  *set_out = &posix_dataset->base;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: successfully %s posix dataset %s:%llu on data root %s",
            (flags & HIO_FLAG_CREAT) ? "created" : "opened", name, set_id, module->data_root);

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_close (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  hio_context_t context = module->context;
  hio_element_t element;
  int rc = HIO_SUCCESS;

  if (posix_dataset->fh) {
    fclose (posix_dataset->fh);
    posix_dataset->fh = NULL;
  }

  hioi_list_foreach(element, dataset->dataset_element_list, struct hio_element, element_list) {
    if (element->element_fh) {
      fclose (element->element_fh);
      element->element_fh = NULL;
    }
  }

  if (dataset->dataset_flags & HIO_FLAG_WRITE) {
    if (HIO_FILE_MODE_BASIC != dataset->dataset_file_mode || 0 == context->context_rank) {
      char *path;

      if (HIO_FILE_MODE_BASIC != dataset->dataset_file_mode) {
        rc = asprintf (&path, "%s/manifest%05d.xml", posix_dataset->base_path,
                       context->context_rank);
      } else if (0 == context->context_rank) {
        rc = asprintf (&path, "%s/manifest.xml", posix_dataset->base_path);
      }

      if (0 < rc) {
        rc = hioi_manifest_save (dataset, path);
        free (path);
        if (HIO_SUCCESS != rc) {
          hio_err_push (rc, context, &dataset->dataset_object, "posix: error writing dataset manifest");
        }
      } else {
        rc = HIO_ERR_OUT_OF_RESOURCE;
      }
    }
  }

#if HIO_USE_MPI
  /* ensure all ranks have closed the dataset before continuing */
  if (hioi_context_using_mpi (context)) {
    MPI_Allreduce (MPI_IN_PLACE, &rc, 1, MPI_INT, MPI_MIN, context->context_comm);
  }
#endif

  if (posix_dataset->base_path) {
    free (posix_dataset->base_path);
    posix_dataset->base_path = NULL;
  }

  pthread_mutex_destroy (&posix_dataset->lock);

  return rc;
}

static int builtin_posix_unlink_cb (const char *path, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
  return remove (path);
}

static int builtin_posix_module_dataset_unlink (struct hio_module_t *module, const char *name, int64_t set_id) {
  struct stat statinfo;
  char *path = NULL;
  int rc;

  if (module->context->context_rank) {
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

static int builtin_posix_module_element_open (struct hio_module_t *module, hio_dataset_t dataset,
                                              hio_element_t *element_out, const char *element_name,
                                              int flags) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) module;
  hio_context_t context = hioi_object_context (&dataset->dataset_object);
  hio_fs_attr_t *fs_attr = &posix_dataset->base.dataset_fs_attr;
  hio_element_t element;

  hioi_list_foreach (element, dataset->dataset_element_list, struct hio_element, element_list) {
    if (!strcmp (element->element_object.identifier, element_name)) {
      *element_out = element;
      element->element_is_open = true;
      /* nothing more to do with optimized mode */
      return HIO_SUCCESS;
    }
  }

  element = hioi_element_alloc (dataset, element_name);
  if (NULL == element) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  if (HIO_FILE_MODE_BASIC == dataset->dataset_file_mode) {
    int open_flags;
    char *file_mode;
    char *path;
    int rc, fd;

    if (HIO_SET_ELEMENT_UNIQUE == dataset->dataset_mode) {
      rc = asprintf (&element->element_backing_file, "element_data.%s.%05d", element_name,
                     context->context_rank);
    } else {
      rc = asprintf (&element->element_backing_file, "element_data.%s", element_name);
    }

    if (0 > rc) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    /* determine the fopen file mode to use */
    if (HIO_FLAG_WRITE & dataset->dataset_flags) {
      file_mode = "w";
      open_flags = O_CREAT | O_WRONLY;
    } else {
      file_mode = "r";
      open_flags = O_RDONLY;
    }

    rc = asprintf (&path, "%s/%s", posix_dataset->base_path, element->element_backing_file);
    if (0 > rc) {
      hioi_element_release (element);
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    /* it is not possible to get open with create without truncation using fopen so use a
     * combination of open and fdopen to get the desired effect */
    //hioi_log (context, HIO_VERBOSE_DEBUG_HIGH, "posix: calling open; path: %s open_flags: %i", path, open_flags);
    fd = fs_attr->fs_open (path, fs_attr, open_flags, posix_module->access_mode);
    if (fd < 0) {
      hio_err_push (fd, context, &dataset->dataset_object, "posix: error opening element path %s. "
                    "errno: %d", path, errno);
      free (path);
      hioi_element_release (element);
      return fd;
    }

    //hioi_log (context, HIO_VERBOSE_DEBUG_HIGH, "posix: calling fdopen; fd: %d file_mode: %c", fd, file_mode);
    element->element_fh = fdopen (fd, file_mode);
    if (NULL == element->element_fh) {
      int hrc = hioi_err_errno (errno);
      hio_err_push (hrc, context, &dataset->dataset_object, "posix: error opening element file %s. "
                    "errno: %d", path, errno);
      free (path);
      hioi_element_release (element);
      return hrc;
    }

    fseek (element->element_fh, 0, SEEK_END);
    element->element_size = ftell (element->element_fh);
    fseek (element->element_fh, 0, SEEK_SET);

    free (path);
  }

  hioi_dataset_add_element (dataset, element);

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: %s element %p (identifier %s) for dataset %s",
	    (HIO_FLAG_WRITE & dataset->dataset_flags) ? "created" : "opened", element, element_name,
            dataset->dataset_object.identifier);

  *element_out = element;

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_close (struct hio_module_t *module, hio_element_t element) {
  element->element_is_open = false;
  return HIO_SUCCESS;
}

static int builtin_posix_module_element_write_strided_nb (struct hio_module_t *module, hio_element_t element,
                                                          hio_request_t *request, off_t offset, const void *ptr,
                                                          size_t count, size_t size, size_t stride) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  hio_context_t context = hioi_object_context (&element->element_object);
  uint64_t stop, start;
  hio_request_t new_request;
  size_t items_written;
  long file_offset;
  FILE *fh;

  if (!(posix_dataset->base.dataset_flags & HIO_FLAG_WRITE)) {
    return HIO_ERR_PERM;
  }

  start = hioi_gettime ();

  pthread_mutex_lock (&posix_dataset->lock);

  if (HIO_FILE_MODE_BASIC != posix_dataset->base.dataset_file_mode) {
    file_offset = ftell (posix_dataset->fh);
    fh = posix_dataset->fh;
  } else {
    file_offset = offset;
    fseek (element->element_fh, file_offset, SEEK_SET);
    fh = element->element_fh;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: writing %lu bytes to file offset %lu (%lu)", count * size,
            file_offset, ftell (fh));

  errno = 0;
  if (0 < stride) {
    items_written = 0;

    for (int i = 0 ; i < count ; ++i) {
      size_t ret = fwrite (ptr, size, 1, fh);
      items_written += ret;
      if (ret != size) {
        break;
      }
      ptr = (void *)((intptr_t) ptr + size + stride);
    }
  } else {
    items_written = fwrite (ptr, size, count, fh);
  }

  posix_dataset->base.dataset_status = hioi_err_errno (errno);

  if (HIO_FILE_MODE_BASIC != posix_dataset->base.dataset_file_mode) {
    hioi_element_add_segment (element, file_offset, offset, 0, size * count);
  }

  file_offset = ftell (fh);
  if (element->element_size < file_offset) {
    element->element_size = file_offset;
  }

  pthread_mutex_unlock (&posix_dataset->lock);

  if (request) {
    new_request = hioi_request_alloc (context);
    if (NULL == new_request) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    *request = new_request;
    new_request->request_transferred = items_written * size;
    new_request->request_complete = true;
    new_request->request_status = posix_dataset->base.dataset_status;
  }

  stop = hioi_gettime ();
  posix_dataset->base.dataset_bytes_written += items_written * size;
  posix_dataset->base.dataset_write_time += stop - start;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: finished write. bytes written: %lu, time: %llu usec",
            items_written * size, stop - start);

  return posix_dataset->base.dataset_status;
}

static int builtin_posix_module_element_read_strided_nb (struct hio_module_t *module, hio_element_t element,
                                                         hio_request_t *request, off_t offset, void *ptr,
                                                         size_t count, size_t size, size_t stride) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  size_t bytes_read, bytes_available, bytes_requested = count * size;
  hio_context_t context = hioi_object_context (&element->element_object);
  size_t remaining_size = size;
  uint64_t start, stop;
  hio_request_t new_request;
  off_t file_offset;
  int rc = HIO_SUCCESS;
  FILE *fh;

  if (!(posix_dataset->base.dataset_flags & HIO_FLAG_READ)) {
    return HIO_ERR_PERM;
  }

  start = hioi_gettime ();

  if (HIO_FILE_MODE_BASIC != posix_dataset->base.dataset_file_mode) {
    fh = posix_dataset->fh;
  } else {
    fh = element->element_fh;
  }

  while (bytes_requested) {
    bytes_available = bytes_requested;
    if (HIO_FILE_MODE_BASIC != posix_dataset->base.dataset_file_mode) {
      rc = hioi_element_find_offset (element, offset, 0, &file_offset, &bytes_available);
      if (HIO_SUCCESS != rc) {
        break;
      }
    } else {
      file_offset = offset;
    }

    errno = 0;
    pthread_mutex_lock (&posix_dataset->lock);
    fseek (fh, file_offset, SEEK_SET);
    if (0 < stride) {
      bytes_read = 0;

      for (int i = 0 ; i < count && bytes_available ; ++i) {
        size_t tmp = (bytes_available < remaining_size) ? bytes_available : remaining_size;
        size_t actual_bytes_read;

        actual_bytes_read = fread (ptr, 1, tmp, fh);
        if (!actual_bytes_read) {
          break;
        }

        bytes_read += actual_bytes_read;

        if (tmp < remaining_size) {
          /* partial read */
          ptr = (void *)((intptr_t) ptr + tmp);
          remaining_size = size - tmp;
        } else {
          ptr = (void *)((intptr_t) ptr + remaining_size + stride);
          remaining_size = size;
        }
      }
    } else {
      bytes_read = fread (ptr, 1, bytes_available, fh);
      ptr = (void *)((intptr_t) ptr + bytes_read);
    }

    pthread_mutex_unlock (&posix_dataset->lock);

    posix_dataset->base.dataset_bytes_read += bytes_read;

    if (0 == bytes_read) {
      rc = hioi_err_errno (errno);
      break;
    }

    offset += bytes_read;
    bytes_requested -= bytes_read;
  }

  if (request) {
    new_request = hioi_request_alloc (context);
    if (NULL == new_request) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    *request = new_request;
    new_request->request_transferred = (count * size) - bytes_requested;
    new_request->request_complete = true;
  }

  stop = hioi_gettime ();
  posix_dataset->base.dataset_read_time += stop - start;
  return rc;
}

static int builtin_posix_module_element_flush (struct hio_module_t *module, hio_element_t element,
                                               hio_flush_mode_t mode) {
  hio_dataset_t dataset = hioi_element_dataset (element);

  if (!(dataset->dataset_flags & HIO_FLAG_WRITE)) {
    return HIO_ERR_PERM;
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_complete (struct hio_module_t *module, hio_element_t element) {
  hio_dataset_t dataset = hioi_element_dataset (element);

  if (!(dataset->dataset_flags & HIO_FLAG_READ)) {
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
