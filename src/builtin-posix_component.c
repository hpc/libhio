/* -*- Mode: C; c-basic-offset:2 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014      Los Alamos National Security, LLC.  All rights
 *                         reserved. 
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "hio_types.h"
#include "hio_component.h"
#include "hio_module.h"

#include <stdlib.h>
#include <stdio.h>

#include <string.h>
#include <errno.h>

#include <dirent.h>
#include <unistd.h>

#if defined(HAVE_SYS_STAT_H)
#include <sys/stat.h>
#endif

typedef struct builtin_posix_module_t {
  hio_module_t base;
  mode_t access_mode;
} builtin_posix_module_t;

#if !defined(PATH_MAX)
#define PATH_MAX 4096
#endif

typedef struct builtin_posix_module_dataset_t {
  struct hio_dataset_t base;
  pthread_mutex_t      lock;
  FILE                *fh;
} builtin_posix_module_dataset_t;

static void builtin_posix_dataset_path (struct hio_module_t *module, char *path, size_t path_size,
					const char *name, uint64_t set_id) {
  hio_context_t context = module->context;
  snprintf (path, path_size, "%s/%s.hio/%s/%lu", module->data_root,
	    context->context_object.identifier, name, (unsigned long) set_id);
}

static int builtin_posix_create_dataset_dirs (struct hio_module_t *module, const char *name, uint64_t set_id) {
  builtin_posix_module_t *posix_module = (builtin_posix_module_t *) module;
  mode_t access_mode = posix_module->access_mode;
  hio_context_t context = module->context;
  char path[PATH_MAX];
  int rc;

  snprintf (path, PATH_MAX, "%s/%s.hio", module->data_root,
	    context->context_object.identifier);

  rc = mkdir (path, access_mode);
  if (0 > rc && EEXIST != errno) {
    hio_err_push (hioi_err_errno (errno), context, NULL, "Error creating context directory: %s", path);
    return hioi_err_errno (errno);
  }

  snprintf (path, PATH_MAX, "%s/%s.hio/%s", module->data_root,
	    context->context_object.identifier, name);

  rc = mkdir (path, access_mode);
  if (0 > rc && EEXIST != errno) {
    hio_err_push (hioi_err_errno (errno), context, NULL, "Error creating context directory: %s", path);
    return hioi_err_errno (errno);
  }

  builtin_posix_dataset_path (module, path, PATH_MAX, name, set_id);
  rc = mkdir (path, access_mode);
  if (0 > rc) {
    hio_err_push (hioi_err_errno (errno), context, NULL, "Error creating context directory: %s", path);
    return hioi_err_errno (errno);
  }

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Successfully created dataset directories");

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_list (struct hio_module_t *module, const char *name,
                                              int64_t **set_ids, int *set_id_count) {
  hio_context_t context = module->context;
  int num_set_ids = 0, set_id_index = 0;
  struct dirent *dp;
  char path[PATH_MAX];
  DIR *dir;

  *set_ids = NULL;
  *set_id_count = 0;

  snprintf (path, PATH_MAX, "%s/%s.hio/%s/", module->data_root,
            context->context_object.identifier, name);

  dir = opendir (path);
  if (NULL == dir) {
    return HIO_SUCCESS;
  }

  while (NULL != (dp = readdir (dir))) {
    if (dp->d_name[0] != '.') {
      num_set_ids++;
    }
  }

  if (0 == num_set_ids) {
    closedir (dir);
    return HIO_SUCCESS;
  }

  rewinddir (dir);

  *set_ids = calloc (num_set_ids, sizeof (uint64_t));
  if (NULL == *set_ids) {
    closedir (dir);
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  while (NULL != (dp = readdir (dir))) {
    if (dp->d_name[0] != '.') {
      set_ids[0][set_id_index++] = strtol (dp->d_name, NULL, 0);
    }
  }

  *set_id_count = num_set_ids;

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_open (struct hio_module_t *module,
                                              hio_dataset_t *set_out, const char *name,
                                              int64_t set_id, hio_flags_t flags,
                                              hio_dataset_mode_t mode) {
  hio_context_t context = module->context;
  builtin_posix_module_dataset_t *dataset;
  char path[PATH_MAX];
  int rc;

  hioi_log (context, HIO_VERBOSE_DEBUG_MED, "builtin-posix: opening dataset %s:%lu",
	    name, (unsigned long) set_id);

  dataset = (builtin_posix_module_dataset_t *)
    hioi_dataset_alloc (context, name, set_id, flags, mode,
			sizeof (builtin_posix_module_dataset_t));
  if (NULL == dataset) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  if (!(flags & HIO_FLAG_CREAT)) {
    snprintf (path, PATH_MAX, "%s/%s.hio/%s/%lu/manifest%05d.xml", module->data_root,
	      context->context_object.identifier, name, (unsigned long) set_id,
	      context->context_rank);

    rc = hioi_manifest_load (&dataset->base, path);
  } else {
    rc = builtin_posix_create_dataset_dirs (module, name, set_id);

    snprintf (path, PATH_MAX, "%s.hio/%s/%lu/data.%05d",
	      context->context_object.identifier, name, (unsigned long) set_id,
	      context->context_rank);

    dataset->base.dataset_backing_file = strdup (path);
  }

  if (HIO_SUCCESS == rc) {
    const char *file_mode;

    if (flags == HIO_FLAG_RDONLY) {
      file_mode = "r";
    } else {
      file_mode = "a";
    }

    fprintf (stderr, "file mode: %s, flags: %x\n", file_mode, flags);

    snprintf (path, PATH_MAX, "%s/%s", module->data_root, dataset->base.dataset_backing_file);

    dataset->fh = fopen (path, file_mode);
    if (NULL == dataset->fh) {
      rc = HIO_ERR_NOT_FOUND;
    }
  } else {
    dataset->fh = NULL;
  }


#if HIO_USE_MPI
  if (hioi_context_using_mpi (context)) {
    MPI_Allreduce (&rc, 1, MPI_INT, 0, context->context_comm);
  }
#endif

  if (HIO_SUCCESS != rc) {
    if (dataset->fh) {
      fclose (dataset->fh);
    }

    hioi_dataset_release ((hio_dataset_t *) &dataset);
    return rc;
  }

  dataset->base.dataset_module = module;

  pthread_mutex_init (&dataset->lock, NULL);

  *set_out = &dataset->base;

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_close (struct hio_module_t *module, hio_dataset_t dataset) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) dataset;
  hio_context_t context = module->context;
  int rc;

  if (posix_dataset->fh) {
    fclose (posix_dataset->fh);
  }

  if (dataset->dataset_flags & HIO_FLAG_WRONLY) {
    char path[PATH_MAX];

    snprintf (path, PATH_MAX, "%s/%s.hio/%s/%lu/manifest%05d.xml", module->data_root,
	      context->context_object.identifier, dataset->dataset_object.identifier,
	      (unsigned long) dataset->dataset_id, context->context_rank);

    rc = hioi_manifest_save (dataset, path);
    if (HIO_SUCCESS != rc) {
      hio_err_push (rc, context, &dataset->dataset_object, "error writing local manifest");
      return rc;
    }
  }

  pthread_mutex_destroy (&posix_dataset->lock);

  return HIO_SUCCESS;
}

static int builtin_posix_module_dataset_unlink (struct hio_module_t *module, const char *name, int64_t set_id) {
  char path[PATH_MAX];
  struct stat statinfo;
  int rc;

  builtin_posix_dataset_path (module, path, PATH_MAX, name, set_id);
  if (stat (path, &statinfo)) {
    return hioi_err_errno (errno);
  }

  rc = rmdir (path);
  if (0 > rc) {
    return hioi_err_errno (errno);
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_open (struct hio_module_t *module, hio_dataset_t dataset,
                                              hio_element_t *element_out, const char *element_name,
                                              hio_flags_t flags) {
  hio_element_t element;

  hioi_list_foreach (element, dataset->dataset_element_list, struct hio_element_t, element_list) {
    if (!strcmp (element->element_object.identifier, element_name)) {
      *element_out = element;
      element->element_is_open = true;
      return HIO_SUCCESS;
    }
  }

  element = hioi_element_alloc (dataset, element_name);
  if (NULL == element) {
    return HIO_ERR_OUT_OF_RESOURCE;
  }

  hioi_dataset_add_element (dataset, element);

  hioi_log (dataset->dataset_context, HIO_VERBOSE_DEBUG_LOW, "Created new element %p (idenfier %s) for dataset %s",
	    element, element_name, dataset->dataset_object.identifier);

  *element_out = element;

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_close (struct hio_module_t *module, hio_element_t element) {
  element->element_is_open = false;
  return HIO_SUCCESS;
}

static int builtin_posix_module_element_write_nb (struct hio_module_t *module, hio_element_t element,
                                                  hio_request_t *request, off_t offset, void *ptr,
                                                  size_t count, size_t size) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) element->element_dataset;
  hio_context_t context = posix_dataset->base.dataset_context;
  hio_request_t new_request;
  size_t items_written;
  long file_offset;

  pthread_mutex_lock (&posix_dataset->lock);
  file_offset = ftell (posix_dataset->fh);

  items_written = fwrite (ptr, size, count, posix_dataset->fh);

  hioi_element_add_segment (element, file_offset, offset, 0, size * count);

  pthread_mutex_unlock (&posix_dataset->lock);

  if (request) {
    new_request = hioi_request_alloc (context);
    if (NULL == new_request) {
      return HIO_ERR_OUT_OF_RESOURCE;
    }

    *request = new_request;
    new_request->request_transferred = items_written * size;;
    new_request->request_complete = true;
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_read_nb (struct hio_module_t *module, hio_element_t element,
                                                 hio_request_t *request, off_t offset, void *ptr,
                                                 size_t count, size_t size) {
  builtin_posix_module_dataset_t *posix_dataset = (builtin_posix_module_dataset_t *) element->element_dataset;
  size_t bytes_read, bytes_available, bytes_requested = count * size;
  hio_context_t context = posix_dataset->base.dataset_context;
  hio_request_t new_request;
  off_t file_offset;
  int rc = HIO_SUCCESS;

  while (bytes_requested) {
    bytes_available = bytes_requested;
    rc = hioi_element_find_offset (element, offset, 0, &file_offset, &bytes_available);
    if (HIO_SUCCESS != rc) {
      break;
    }

    errno = 0;
    pthread_mutex_lock (&posix_dataset->lock);
    fseek (posix_dataset->fh, file_offset, SEEK_SET);
    bytes_read = fread (ptr, 1, bytes_available, posix_dataset->fh);
    pthread_mutex_unlock (&posix_dataset->lock);

    if (0 == bytes_read) {
      rc = hioi_err_errno (errno);
      break;
    }

    offset += bytes_read;
    ptr = (void *)((intptr_t) ptr + bytes_read);
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

  return rc;
}

static int builtin_posix_module_element_flush (struct hio_module_t *module, hio_element_t element,
                                               hio_flush_mode_t mode) {
  hio_dataset_t dataset = element->element_dataset;

  if (!(dataset->dataset_flags & HIO_FLAG_WRONLY)) {
    return HIO_ERR_PERM;
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_element_complete (struct hio_module_t *module, hio_element_t element) {
  hio_dataset_t dataset = element->element_dataset;

  if (!(dataset->dataset_flags & HIO_FLAG_RDONLY)) {
    return HIO_ERR_PERM;
  }

  return HIO_SUCCESS;
}

static int builtin_posix_module_fini (struct hio_module_t *module) {
  hioi_log (module->context, HIO_VERBOSE_DEBUG_LOW, "Finalizing posix filesystem module for data root %s",
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

  .element_write_nb = builtin_posix_module_element_write_nb,
  .element_read_nb  = builtin_posix_module_element_read_nb,

  .element_flush    = builtin_posix_module_element_flush,
  .element_complete = builtin_posix_module_element_complete,

  .dataset_list     = builtin_posix_module_dataset_list,

  .fini             = builtin_posix_module_fini,
};

static int builtin_posix_component_init (void) {
  /* nothing to do */
  return HIO_SUCCESS;
}

static int builtin_posix_component_fini (void) {
  /* nothing to do */
  return HIO_SUCCESS;
}

static int builtin_posix_component_query (hio_context_t context, const char *data_root,
					  hio_module_t **module) {
  builtin_posix_module_t *new_module;

  if (strncasecmp("posix:", data_root, 1)) {
    hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Module posix does not match for data root %s",
	      data_root);
    return HIO_ERR_NOT_AVAILABLE;
  }

  /* skip posix: */
  data_root += 6;

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
  new_module->access_mode ^= 0777;

  hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Created posix filesystem module for data root %s. access mode %o",
	    data_root, new_module->access_mode);

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
