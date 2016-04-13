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
static int builtin_posix_module_dataset_close (hio_dataset_t dataset);
static int builtin_posix_module_element_open (hio_dataset_t dataset, hio_element_t element);
static int builtin_posix_module_element_close (hio_element_t element);
static int builtin_posix_module_element_write_strided_nb (hio_element_t element, hio_request_t *request,
                                                          off_t offset, const void *ptr, size_t count,
                                                          size_t size, size_t stride);
static int builtin_posix_module_element_read_strided_nb (hio_element_t element, hio_request_t *request, off_t offset,
                                                         void *ptr, size_t count, size_t size, size_t stride);
static int builtin_posix_module_element_flush (hio_element_t element, hio_flush_mode_t mode);
static int builtin_posix_module_element_complete (hio_element_t element);
static int builtin_posix_module_process_reqs (hio_dataset_t dataset, hio_internal_request_t **reqs, int req_count);

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
  int rc;

  if (context->c_rank > 0) {
    return HIO_SUCCESS;
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "posix: creating dataset directory @ %s", posix_dataset->base_path);

  rc = hio_mkpath (context, posix_dataset->base_path, access_mode);
  if (0 > rc || EEXIST == errno) {
    if (EEXIST != errno) {
      hioi_err_push (hioi_err_errno (errno), &context->c_object, "posix: error creating context directory: %s",
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

    rc = asprintf (&path, "%s/%s.hio/%s", module->data_root, hioi_object_identifier(context), name);
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

      rc = asprintf (&manifest_path, "%s/%s/manifest.json.bz2", path, dp->d_name);
      assert (0 <= rc);

      rc = hioi_manifest_read_header (context, headers[0] + set_id_index, manifest_path);
      if (HIO_SUCCESS == rc) {
        ++set_id_index;
      } else {
        free (manifest_path);
        rc = asprintf (&manifest_path, "%s/%s/manifest.json", path, dp->d_name);
        assert (0 <= rc);

        rc = hioi_manifest_read_header (context, headers[0] + set_id_index, manifest_path);
        if (HIO_SUCCESS == rc) {
          ++set_id_index;
        } else {
          /* skip dataset */
          hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_list: could not read manifest at path: %s. rc: %d",
                    manifest_path, rc);
        }
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

static int builtin_posix_module_dataset_init (struct hio_module_t *module,
                                              builtin_posix_module_dataset_t *posix_dataset) {
  hio_context_t context = hioi_object_context ((hio_object_t) posix_dataset);
  int rc;

  rc = asprintf (&posix_dataset->base_path, "%s/%s.hio/%s/%lu", module->data_root,
                 hioi_object_identifier(context), hioi_object_identifier (posix_dataset),
                 (unsigned long) posix_dataset->base.ds_id);
  assert (0 < rc);

  for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
    posix_dataset->files[i].f_bid = -1;
    posix_dataset->files[i].f_file.f_hndl = NULL;
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_open (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) module;
  hio_context_t context = hioi_object_context ((hio_object_t) dataset);
  unsigned char *manifest = NULL;
  size_t manifest_size = 0;
  hio_fs_attr_t *fs_attr;
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

  fs_attr = &posix_dataset->base.ds_fsattr;

  rc = hioi_fs_query (context, module->data_root, fs_attr);
  if (HIO_SUCCESS != rc) {
    hioi_err_push (rc, &context->c_object, "posix: error querying the filesystem");
    return rc;
  }

  if (fs_attr->fs_flags & HIO_FS_SUPPORTS_STRIPING) {
    hioi_config_add (context, &dataset->ds_object, &fs_attr->fs_scount,
                     "stripe_count", HIO_CONFIG_TYPE_UINT32, NULL, "Stripe count for all dataset "
                     "data files", 0);

    if (fs_attr->fs_scount > fs_attr->fs_smax_count) {
      hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_open: requested stripe count %u exceeds the available resources. "
                "adjusting to maximum %u", fs_attr->fs_scount, fs_attr->fs_smax_count);
      fs_attr->fs_scount = fs_attr->fs_smax_count;
    }

    hioi_config_add (context, &dataset->ds_object, &fs_attr->fs_ssize,
                     "stripe_size", HIO_CONFIG_TYPE_UINT64, NULL, "Stripe size for all dataset "
                     "data files", 0);

    /* ensure the stripe size is a multiple of the stripe unit */
    fs_attr->fs_ssize = fs_attr->fs_sunit * ((fs_attr->fs_ssize + fs_attr->fs_sunit - 1) / fs_attr->fs_sunit);
    if (fs_attr->fs_ssize > fs_attr->fs_smax_size) {
      hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_open: requested stripe size %" PRIu64 " exceeds the maximum %"
                PRIu64 ". ", fs_attr->fs_ssize, fs_attr->fs_smax_size);
      fs_attr->fs_ssize = fs_attr->fs_smax_size;
    }

    hioi_config_add (context, &dataset->ds_object, &fs_attr->fs_raid_level,
                     "raid_level", HIO_CONFIG_TYPE_UINT64, NULL, "RAID level for dataset "
                     "data files. Keep in mind that some filesystems only support 1/2 RAID "
                     "levels", 0);

    if (HIO_FILE_MODE_OPTIMIZED == dataset->ds_fmode) {
      fs_attr->fs_scount = 1;
      fs_attr->fs_ssize = dataset->ds_bs;
      fs_attr->fs_use_group_locking = true;
    }
  }

  do {
    if (0 != context->c_rank) {
      break;
    }

    if (dataset->ds_flags & HIO_FLAG_TRUNC) {
      /* blow away the existing dataset */
      (void) builtin_posix_module_dataset_unlink (module, hioi_object_identifier(dataset),
                                                  dataset->ds_id);

      /* ensure we take the create path later */
      dataset->ds_flags |= HIO_FLAG_CREAT;
    }

    if (!(dataset->ds_flags & HIO_FLAG_CREAT)) {
      /* load manifest. the manifest data will be shared with other processes in hioi_dataset_scatter */
      rc = asprintf (&path, "%s/manifest.json.bz2", posix_dataset->base_path);
      assert (0 < rc);

      if (access (path, F_OK)) {
        free (path);
        rc = asprintf (&path, "%s/manifest.json", posix_dataset->base_path);
        assert (0 < rc);
        if (access (path, F_OK)) {
          hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: could not find top-level manifest");
          rc = HIO_ERR_NOT_FOUND;
          break;
        }
      }

      rc = hioi_manifest_read (path, &manifest, &manifest_size);
      free (path);
    } else {
      rc = builtin_posix_create_dataset_dirs (posix_module, posix_dataset);
      if (HIO_SUCCESS != rc) {
        break;
      }

      rc = hioi_manifest_serialize (dataset, &manifest, &manifest_size, true);
    }
  } while (0);

  /* share dataset information will all processes in the communication domain */
  rc = hioi_dataset_scatter (dataset, manifest, manifest_size, rc);
  if (HIO_SUCCESS != rc) {
    free (posix_dataset->base_path);
    return rc;
  }

  free (manifest);

  if (HIO_FILE_MODE_OPTIMIZED == dataset->ds_fmode) {
    if (HIO_SET_ELEMENT_UNIQUE == dataset->ds_mode || 2 > context->c_size || NULL == dataset->ds_shared_control) {
      posix_dataset->base.ds_fmode = HIO_FILE_MODE_BASIC;
      /* NTH: no optimized mode for N->N yet */
      hioi_log (context, HIO_VERBOSE_WARN, "posix:dataset_open: optimized file mode requested but not supported in this "
                "dataset mode. falling back to basic file mode");
    }
  }

  dataset->ds_module = module;
  dataset->ds_close = builtin_posix_module_dataset_close;
  dataset->ds_element_open = builtin_posix_module_element_open;
  dataset->ds_process_reqs = builtin_posix_module_process_reqs;

  pthread_mutex_init (&posix_dataset->lock, NULL);

  /* record the open time */
  gettimeofday (&dataset->ds_otime, NULL);

  stop = hioi_gettime ();

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: successfully %s posix dataset %s:%llu on data root %s. "
            "open time %lu usec", (dataset->ds_flags & HIO_FLAG_CREAT) ? "created" : "opened", hioi_object_identifier(dataset),
            dataset->ds_id, module->data_root, stop - start);

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_close (hio_dataset_t dataset) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) dataset->ds_module;
  hio_context_t context = hioi_object_context ((hio_object_t) dataset);
  hio_module_t *module = dataset->ds_module;
  unsigned char *manifest;
  uint64_t start, stop;
  int rc = HIO_SUCCESS;
  size_t manifest_size;

  start = hioi_gettime ();

  for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
    if (posix_dataset->files[i].f_file.f_hndl != NULL) {
      fclose (posix_dataset->files[i].f_file.f_hndl);
      posix_dataset->files[i].f_file.f_hndl = NULL;
    }
  }

  if (dataset->ds_flags & HIO_FLAG_WRITE) {
    rc = hioi_dataset_gather_manifest (dataset, &manifest, &manifest_size, dataset->ds_use_bzip);
    if (HIO_SUCCESS != rc) {
      dataset->ds_status = rc;
    }

    if (0 == context->c_rank) {
      char *path;

      rc = asprintf (&path, "%s/manifest.json%s", posix_dataset->base_path,
                     dataset->ds_use_bzip ? ".bz2" : "");
      if (0 < rc) {
        int fd;

        errno = 0;
        fd = open (path, O_CREAT | O_WRONLY, posix_module->access_mode);
        if (0 <= fd) {
          (void) write (fd, manifest, manifest_size);
          close (fd);
        }
        free (manifest);

        rc = hioi_err_errno (errno);

        free (path);
        if (HIO_SUCCESS != rc) {
          hioi_err_push (rc, &dataset->ds_object, "posix: error writing dataset manifest");
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

  free (posix_dataset->base_path);

  pthread_mutex_destroy (&posix_dataset->lock);

  stop = hioi_gettime ();

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix:dataset_open: successfully closed posix dataset %s:%llu on data root %s. "
            "close time %lu usec", hioi_object_identifier(dataset), dataset->ds_id, module->data_root, stop - start);

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
    hioi_err_push (hioi_err_errno (errno), &module->context->c_object, "posix: could not unlink dataset. errno: %d",
                  errno);
    return hioi_err_errno (errno);
  }

  return HIO_SUCCESS;
}

static int builtin_posix_open_file (builtin_posix_module_t *posix_module, builtin_posix_module_dataset_t *posix_dataset,
                                    char *path, hio_file_t *file) {
  hio_object_t hio_object = &posix_dataset->base.ds_object;
  hio_fs_attr_t *fs_attr = &posix_dataset->base.ds_fsattr;
  int open_flags, fd;
  char *file_mode;

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
    hioi_err_push (fd, hio_object, "posix: error opening element path %s. "
                  "errno: %d", path, errno);
    return fd;
  }

  file->f_hndl = fdopen (fd, file_mode);
  if (NULL == file->f_hndl) {
    int hrc = hioi_err_errno (errno);
    hioi_err_push (hrc, hio_object, "posix: error opening element file %s. "
                   "errno: %d", path, errno);
    return hrc;
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_open_basic (builtin_posix_module_t *posix_module, builtin_posix_module_dataset_t *posix_dataset,
                                                    hio_element_t element) {
  const char *element_name = hioi_object_identifier(element);
  char *path;
  int rc;

  if (HIO_SET_ELEMENT_UNIQUE == posix_dataset->base.ds_mode) {
    rc = asprintf (&path, "%s/element_data.%s.%05d", posix_dataset->base_path, element_name,
                   element->e_rank);
  } else {
    rc = asprintf (&path, "%s/element_data.%s", posix_dataset->base_path, element_name);
  }

  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  rc = builtin_posix_open_file (posix_module, posix_dataset, path, &element->e_file);
  free (path);
  if (HIO_SUCCESS != rc) {
    return rc;
  }

  fseek (element->e_file.f_hndl, 0, SEEK_END);
  element->e_size = ftell (element->e_file.f_hndl);
  fseek (element->e_file.f_hndl, 0, SEEK_SET);

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_open (hio_dataset_t dataset, hio_element_t element) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) dataset->ds_module;
  hio_context_t context = hioi_object_context (&dataset->ds_object);
  int rc;

  if (HIO_FILE_MODE_BASIC == dataset->ds_fmode) {
    rc = builtin_posix_module_element_open_basic (posix_module, posix_dataset, element);
    if (HIO_SUCCESS != rc) {
      hioi_object_release (&element->e_object);
      return rc;
    }
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "posix: %s element %p (identifier %s) for dataset %s",
	    (HIO_FLAG_WRITE & dataset->ds_flags) ? "created" : "opened", element,
            hioi_object_identifier(element), hioi_object_identifier(dataset));

  element->e_write_strided_nb = builtin_posix_module_element_write_strided_nb;
  element->e_read_strided_nb = builtin_posix_module_element_read_strided_nb;
  element->e_flush = builtin_posix_module_element_flush;
  element->e_complete = builtin_posix_module_element_complete;
  element->e_close = builtin_posix_module_element_close;

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_close (hio_element_t element) {
  return HIO_SUCCESS;
}

static unsigned long builtin_posix_reserve (builtin_posix_module_dataset_t *posix_dataset, size_t *requested) {
  unsigned long new_offset, to_use, space;

  if (posix_dataset->reserved_remaining) {
    to_use = (*requested > posix_dataset->reserved_remaining) ? posix_dataset->reserved_remaining : *requested;
    new_offset = posix_dataset->reserved_offset;

    /* update cached values */
    posix_dataset->reserved_offset += to_use;
    posix_dataset->reserved_remaining -= to_use;

    *requested = to_use;
    return new_offset;
  }

  pthread_mutex_lock (&posix_dataset->base.ds_shared_control->s_mutex);
  for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
    if (posix_dataset->files[i].f_file.f_hndl) {
      fflush (posix_dataset->files[i].f_file.f_hndl);
      fsync (fileno (posix_dataset->files[i].f_file.f_hndl));
    }
  }
  pthread_mutex_unlock (&posix_dataset->base.ds_shared_control->s_mutex);

  space = *requested;

  if (space % posix_dataset->base.ds_bs) {
    space += posix_dataset->base.ds_bs - (space % posix_dataset->base.ds_bs);
  }

  new_offset = atomic_fetch_add (&posix_dataset->base.ds_shared_control->s_offset, space);

  posix_dataset->reserved_offset = new_offset + *requested;
  posix_dataset->reserved_remaining = space - *requested;

  return new_offset;
}

static int builtin_posix_element_translate_opt_old (builtin_posix_module_t *posix_module, hio_element_t element, off_t offset,
                                                    size_t *size, hio_file_t **file_out) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  hio_context_t context = hioi_object_context (&element->e_object);
  size_t block_id, block_base, block_bound, block_offset;
  builtin_posix_file_t *file;
  int32_t file_index;
  char *path;
  int rc, foo;

  block_id = offset / posix_dataset->base.ds_bs;
  block_base = block_id * posix_dataset->base.ds_bs;
  block_bound = block_base + posix_dataset->base.ds_bs;
  block_offset = offset - block_base;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "builtin_posix_element_translate: element: %s, offset: %lu, block_id: %lu, "
            "block_offset: %lu, block_size: %lu", hioi_object_identifier(element), (unsigned long) offset,
            block_id, block_offset, posix_dataset->base.ds_bs);

  if (offset + *size > block_bound) {
    *size = block_bound - offset;
  }

  rc = asprintf (&path, "%s_block.%lu", hioi_object_identifier(element), (unsigned long) block_id);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  if (HIO_FLAG_WRITE & posix_dataset->base.ds_flags) {
    foo = hioi_dataset_add_file (&posix_dataset->base, path);
  }
  char *tmp = path;
  rc = asprintf (&path, "%s/%s", posix_dataset->base_path, tmp);
  free (tmp);
  if (0 > rc) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  /* use crc as a hash to pick a file index to use */
  file_index = hioi_crc32 ((uint8_t *) path, strlen (path)) % HIO_POSIX_MAX_OPEN_FILES;
  file = posix_dataset->files + file_index;

  if (block_id != file->f_bid || file->f_element != element) {
    if (file->f_file.f_hndl != NULL) {
      fclose (file->f_file.f_hndl);
      file->f_file.f_hndl = NULL;
      file->f_bid = -1;
    }

    file->f_element = element;

    rc = builtin_posix_open_file (posix_module, posix_dataset, path, &file->f_file);
    if (HIO_SUCCESS != rc) {
      return rc;
    }

    file->f_bid = block_id;
  }

  if (block_offset != file->f_file.f_offset) {
    fseek (file->f_file.f_hndl, block_offset, SEEK_SET);
    file->f_file.f_offset = block_offset;
  }

  if (HIO_FLAG_WRITE & posix_dataset->base.ds_flags) {
    hioi_element_add_segment (element, foo, block_offset, offset, *size);
  }

  *file_out = &file->f_file;

  return HIO_SUCCESS;
}

static int builtin_posix_element_translate_opt (builtin_posix_module_t *posix_module, hio_element_t element, off_t offset,
                                                size_t *size, hio_file_t **file_out, bool reading) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  hio_context_t context = hioi_object_context (&element->e_object);
  builtin_posix_file_t *file;
  uint64_t file_offset;
  int file_index;
  char *path;
  int rc;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "translating element %s offset %ld size %lu",
            hioi_object_identifier (&element->e_object), offset, *size);
  rc = hioi_element_translate_offset (element, offset, &file_index, &file_offset, size);
  if (HIO_SUCCESS != rc) {
    if (reading) {
      hioi_log (context, HIO_VERBOSE_DEBUG_MED, "offset not found");
      /* not found */
      return rc;
    }

    if (hioi_context_using_mpi (context)) {
      rc = asprintf (&path, "%s/data.%x", posix_dataset->base_path, posix_dataset->base.ds_shared_control->s_master);
      if (0 > rc) {
        return HIO_ERR_OUT_OF_RESOURCE;
      }
    } else {
      rc = asprintf (&path, "%s/data", posix_dataset->base_path);
      if (0 > rc) {
        return HIO_ERR_OUT_OF_RESOURCE;
      }
    }

    file_offset = builtin_posix_reserve (posix_dataset, size);

    file_index = hioi_dataset_add_file (&posix_dataset->base, strrchr (path, '/') + 1);
    hioi_element_add_segment (element, file_index, file_offset, offset, *size);
  } else {
    hioi_log (context, HIO_VERBOSE_DEBUG_MED, "offset found in file @ index %d, offset %lu, size %lu", file_index,
              file_offset, *size);
    rc = asprintf (&path, "%s/%s", posix_dataset->base_path, posix_dataset->base.ds_flist[file_index].f_name);
    if (0 > rc) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }
  }

  /* use crc as a hash to pick a file index to use */
  int internal_index = file_index % HIO_POSIX_MAX_OPEN_FILES;
  file = posix_dataset->files + internal_index;

  if (internal_index != file->f_bid) {
    if (NULL != file->f_file.f_hndl) {
      fclose (file->f_file.f_hndl);
      file->f_file.f_hndl = NULL;
      file->f_bid = -1;
    }

    rc = builtin_posix_open_file (posix_module, posix_dataset, path, &file->f_file);
    if (HIO_SUCCESS != rc) {
      free (path);
      return rc;
    }

    file->f_bid = file_index;
  }

  free (path);

  if (file_offset != file->f_file.f_offset) {
    fseek (file->f_file.f_hndl, file_offset, SEEK_SET);
    file->f_file.f_offset = file_offset;
  }

  *file_out = &file->f_file;

  return HIO_SUCCESS;
}

static int builtin_posix_element_translate (builtin_posix_module_t *posix_module, hio_element_t element, off_t offset,
                                            size_t *size, hio_file_t **file_out, bool reading) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);

  if (HIO_FILE_MODE_BASIC == posix_dataset->base.ds_fmode) {
    *file_out = &element->e_file;
    if (offset != element->e_file.f_offset) {
      fseek (element->e_file.f_hndl, offset, SEEK_SET);
      element->e_file.f_offset = offset;
    }

    return HIO_SUCCESS;
  }

  if (reading && 0 == element->e_scount) {
    return builtin_posix_element_translate_opt_old (posix_module, element, offset, size, file_out);
  }

  return builtin_posix_element_translate_opt (posix_module, element, offset, size, file_out, reading);
}

static ssize_t builtin_posix_module_element_write_strided_internal (builtin_posix_module_t *posix_module, hio_element_t element,
                                                                    off_t offset, const void *ptr, size_t count, size_t size,
                                                                    size_t stride) {
  hio_dataset_t dataset = hioi_element_dataset (element);
  size_t bytes_written = 0, ret;
  hio_file_t *file;
  uint64_t stop, start;
  int rc;

  assert (dataset->ds_flags & HIO_FLAG_WRITE);

  if (!(count * size)) {
    return 0;
  }

  if (0 == stride) {
    size *= count;
    count = 1;
  }

  start = hioi_gettime ();

  errno = 0;

  for (size_t i = 0 ; i < count ; ++i) {
    size_t req = size, actual;

    do {
      actual = req;

      rc = builtin_posix_element_translate (posix_module, element, offset, &actual,
                                            &file, false);
      assert (file);
      if (HIO_SUCCESS != rc) {
        break;
      }

      ret = fwrite (ptr, 1, actual, file->f_hndl);
      if (ret > 0) {
        bytes_written += ret;
        file->f_offset += ret;
      }

      if (ret < actual) {
        /* short write */
        break;
      }

      req -= actual;
      offset += actual;
      ptr = (void *) ((intptr_t) ptr + actual);
    } while (req);

    if (HIO_SUCCESS != rc || req) {
      break;
    }

    ptr = (void *) ((intptr_t) ptr + stride);
  }

  if (0 == bytes_written || HIO_SUCCESS != rc) {
    if (0 == bytes_written) {
      rc = hioi_err_errno (errno);
    }

    dataset->ds_status = rc;
    return rc;
  }

  if (offset + bytes_written > element->e_size) {
    element->e_size = offset + bytes_written;
  }

  stop = hioi_gettime ();

  dataset->ds_stat.s_wtime += stop - start;

  if (0 < bytes_written) {
    dataset->ds_stat.s_bwritten += bytes_written;
  }

  hioi_log (hioi_object_context (&element->e_object), HIO_VERBOSE_DEBUG_LOW, "posix: finished write. bytes written: "
            "%lu, time: %llu usec", bytes_written, stop - start);

  return bytes_written;
}

static int builtin_posix_module_element_write_strided_nb (hio_element_t element, hio_request_t *request,
                                                          off_t offset, const void *ptr, size_t count,
                                                          size_t size, size_t stride) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) posix_dataset->base.ds_module;
  hio_context_t context = hioi_object_context (&element->e_object);
  ssize_t bytes_written;
  hio_request_t new_request;

  pthread_mutex_lock (&posix_dataset->lock);
  bytes_written = builtin_posix_module_element_write_strided_internal (posix_module, element, offset, ptr, count, size,
                                                                       stride);
  pthread_mutex_unlock (&posix_dataset->lock);

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

  return posix_dataset->base.ds_status;
}

static ssize_t builtin_posix_module_element_read_strided_internal (builtin_posix_module_t *posix_module, hio_element_t element,
                                                                   off_t offset, void *ptr, size_t count, size_t size,
                                                                   size_t stride) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  size_t bytes_read = 0, ret;
  hio_file_t *file;
  uint64_t start, stop;
  int rc;

  if (!(count * size)) {
    return 0;
  }

  errno = 0;

  start = hioi_gettime ();

  for (size_t i = 0 ; i < count ; ++i) {
    size_t req = size, actual;

    do {
      actual = req;

      rc = builtin_posix_element_translate (posix_module, element, offset, &actual,
                                            &file, true);
      if (HIO_SUCCESS != rc) {
        break;
      }

      ret = fread (ptr, 1, actual, file->f_hndl);
      if (ret > 0) {
        bytes_read += ret;
        file->f_offset += ret;
      }

      if (ret < actual) {
        /* short read */
        break;
      }

      req -= actual;
      offset += actual;
      ptr = (void *) ((intptr_t) ptr + actual);
    } while (req);

    if (req || HIO_SUCCESS != rc) {
      break;
    }

    ptr = (void *) ((intptr_t) ptr + stride);
  }

  if (0 == bytes_read || HIO_SUCCESS != rc) {
    if (0 == bytes_read) {
      rc = hioi_err_errno (errno);
    }
    return rc;
  }

  stop = hioi_gettime ();
  posix_dataset->base.ds_stat.s_rtime += stop - start;
  posix_dataset->base.ds_stat.s_bread += bytes_read;

  return bytes_read;
}

static int builtin_posix_module_element_read_strided_nb (hio_element_t element, hio_request_t *request, off_t offset,
                                                         void *ptr, size_t count, size_t size, size_t stride) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) hioi_element_dataset (element);
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) posix_dataset->base.ds_module;
  hio_context_t context = hioi_object_context (&element->e_object);
  ssize_t bytes_read;
  hio_request_t new_request;
  int rc = HIO_SUCCESS;

  if (!(posix_dataset->base.ds_flags & HIO_FLAG_READ)) {
    return HIO_ERR_PERM;
  }

  if (stride == 0) {
    size *= count;
    count = 1;
  }

  hioi_object_lock (&posix_dataset->base.ds_object);
  bytes_read = builtin_posix_module_element_read_strided_internal (posix_module, element, offset, ptr, count, size, stride);
  hioi_object_unlock (&posix_dataset->base.ds_object);

  if (0 > bytes_read) {
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


static int builtin_posix_module_process_reqs (hio_dataset_t dataset, hio_internal_request_t **reqs, int req_count) {
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) dataset->ds_module;

  hioi_object_lock (&dataset->ds_object);
  for (int i = 0 ; i < req_count ; ++i) {
    hio_internal_request_t *req = reqs[i];

    if (HIO_REQUEST_TYPE_READ == req->ir_type) {
      req->ir_status =  builtin_posix_module_element_read_strided_internal (posix_module, req->ir_element, req->ir_offset,
                                                                            req->ir_data.r, req->ir_count, req->ir_size,
                                                                            req->ir_stride);
    } else {
      req->ir_status =  builtin_posix_module_element_write_strided_internal (posix_module, req->ir_element, req->ir_offset,
                                                                             req->ir_data.w, req->ir_count, req->ir_size,
                                                                             req->ir_stride);
    }

    if (req->ir_status < 0) {
      hioi_object_unlock (&dataset->ds_object);
      return (int) req->ir_status;
    }
  }

  hioi_object_unlock (&dataset->ds_object);

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_flush (hio_element_t element, hio_flush_mode_t mode) {
  builtin_posix_module_dataset_t *posix_dataset =
    (builtin_posix_module_dataset_t *) hioi_element_dataset (element);

  if (!(posix_dataset->base.ds_flags & HIO_FLAG_WRITE)) {
    return HIO_ERR_PERM;
  }

  if (HIO_FILE_MODE_OPTIMIZED == posix_dataset->base.ds_fmode) {
    for (int i = 0 ; i < HIO_POSIX_MAX_OPEN_FILES ; ++i) {
      if (posix_dataset->files[i].f_file.f_hndl) {
        fflush (posix_dataset->files[i].f_file.f_hndl);
        if (HIO_FLUSH_MODE_COMPLETE == mode) {
          fsync (fileno (posix_dataset->files[i].f_file.f_hndl));
        }
      }
    }
  } else {
    if (NULL != element->e_file.f_hndl) {
      fflush (element->e_file.f_hndl);
      if (HIO_FLUSH_MODE_COMPLETE == mode) {
        fsync (fileno (element->e_file.f_hndl));
      }
    }
  }

  return HIO_SUCCESS;
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

  .fini           = builtin_posix_module_fini,
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
};
