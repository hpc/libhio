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

#include "hio_internal.h"

#if USE_DYNAMIC_COMPONENTS
#include <dlfcn.h>
#include <dirent.h>
#endif

#include <stdlib.h>
#include <string.h>
#include <errno.h>

extern hio_component_t builtin_posix_component;
#if HIO_USE_DATAWARP
extern hio_component_t builtin_datawarp_component;
#endif

#define MAX_COMPONENTS 128

static hio_component_t *hio_builtin_components[] = {&builtin_posix_component,
#if HIO_USE_DATAWARP
                                                    &builtin_datawarp_component,
#endif
                                                    NULL};

static int hio_component_init_count = 0;

#if USE_DYNAMIC_COMPONENTS
typedef struct hio_dynamic_component_t {
  void *dl_ctx;
  hio_component_t *component;
} hio_dynamic_component_t;

static hio_dynamic_component_t hio_external_components[MAX_COMPONENTS];
static int hio_external_component_count;

static int hioi_dynamic_component_init (hio_context_t context) {
  const char *module_dir = HIO_PREFIX "/lib/hio_" PACKAGE_VERSION "/modules";
  char component_name[128], component_symbol[512];
  hio_component_t *component_ptr;
  int rc = HIO_SUCCESS;
  struct dirent *entry;
  void *dl_ctx;
  char *path;
  DIR *dir;

  hioi_log(context, HIO_VERBOSE_DEBUG_LOW, "Looking for plugins in %s...", module_dir);

  dir = opendir (module_dir);
  if (NULL != dir) {
    hio_dynamic_component_t *external_component = hio_external_components;

    while (NULL != (entry = readdir (dir))) {
      if ('.' == entry->d_name[0]) {
        continue;
      }

      hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Checking file %s", entry->d_name);
      if (strncmp (entry->d_name, "hio_plugin_", 11)) {
        hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "File does not match expected name pattern. Skipping...");
        continue;
      }
      sscanf (entry->d_name, "hio_plugin_%128s", component_name);

      rc = asprintf (&path, "%s/%s", module_dir, entry->d_name);
      if (0 > rc) {
        rc = HIO_ERR_OUT_OF_RESOURCE;
        break;
      }

      dl_ctx = dlopen (path, RTLD_LAZY);
      free (path);
      if (NULL == dl_ctx) {
        rc = hioi_err_errno (errno);
        hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Failed to dlopen() plugin. Reason: %s", strerror (errno));
        continue;
      }

      snprintf (component_symbol, 512, "%s_component", component_name);

      component_ptr = (hio_component_t *) dlsym (dl_ctx, component_symbol);
      if (NULL == component_ptr) {
        hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Could not find component symbol: %s", component_symbol);
        rc = hioi_err_errno (errno);
        dlclose (dl_ctx);
        continue;
      }

      if (NULL == component_ptr->init) {
        hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Component does not define the required init() function");
        dlclose (dl_ctx);
        continue;
      }

      rc = component_ptr->init (context);
      if (HIO_SUCCESS != rc) {
        hioi_log (context, HIO_VERBOSE_DEBUG_LOW, "Component initialize function failed");
        dlclose (dl_ctx);
        continue;
      }

      external_component->dl_ctx = dl_ctx;
      external_component->component = component_ptr;

      ++external_component;
      ++hio_external_component_count;

      rc = HIO_SUCCESS;

      if (MAX_COMPONENTS == hio_external_component_count) {
        break;
      }
    }

    closedir (dir);
  }

  return rc;
}
#endif

int hioi_component_init (hio_context_t context) {
  int rc = HIO_SUCCESS;

  if (hio_component_init_count++ > 0) {
    return HIO_SUCCESS;
  }

  for (int i = 0 ; hio_builtin_components[i] ; ++i) {
    hio_component_t *component = hio_builtin_components[i];

    rc = component->init (context);
    if (HIO_SUCCESS != rc) {
      return rc;
    }
  }

  if (HIO_SUCCESS != rc) {
    return rc;
  }

#if USE_DYNAMIC_COMPONENTS
  return hioi_dynamic_component_init (context);
#endif

  return HIO_SUCCESS;
}

int hioi_component_fini (void) {
  if (0 == hio_component_init_count || --hio_component_init_count) {
    return HIO_SUCCESS;
  }

  for (int i = 0 ; hio_builtin_components[i] ; ++i) {
    hio_component_t *component = hio_builtin_components[i];

    (void) component->fini ();
  }

#if USE_DYNAMIC_COMPONENTS
  for (int i = 0 ; i < hio_external_component_count ; ++i) {
    hio_dynamic_component_t *component = hio_external_components + i;
    (void) component->component->fini ();
    dlclose (component->dl_ctx);
  }

  memset (hio_external_components, 0, sizeof (hio_external_components));
#endif

  return HIO_SUCCESS;
}

int hioi_component_query (hio_context_t context, const char *data_root, const char *next_data_root,
                          hio_module_t **module) {
  int rc;

  for (int i = 0 ; hio_builtin_components[i] ; ++i) {
    hio_component_t *component = hio_builtin_components[i];

    rc = component->query (context, data_root, next_data_root, module);
    if (HIO_SUCCESS == rc) {
      return HIO_SUCCESS;
    }
  }

#if USE_DYNAMIC_COMPONENTS
  for (int i = 0 ; i < hio_external_component_count ; ++i) {
    hio_dynamic_component_t *component = hio_external_components + i;

    rc = component->component->query (context, data_root, next_data_root, module);
    if (HIO_SUCCESS == rc) {
      return HIO_SUCCESS;
    }
  }
#endif

  return HIO_ERR_NOT_FOUND;
}
