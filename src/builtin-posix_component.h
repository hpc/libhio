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

#if !defined(BUILTIN_POSIX_COMPONENT_H)
#define BUILTIN_POSIX_COMPONENT_H

#include "hio_internal.h"
#include "hio_component.h"

#define HIO_POSIX_MAX_OPEN_FILES  32

typedef enum builtin_posix_dataset_fmode {
  /** use basic mode. unique address space results in a single file per element per rank.
   * shared address space results in a single file per element */
  HIO_FILE_MODE_BASIC,
  /** use optimized mode. there is no guarantee about file structure in this mode */
  HIO_FILE_MODE_OPTIMIZED,
  /** write block across multiple files */
  HIO_FILE_MODE_STRIDED,
} builtin_posix_dataset_fmode_t;

/* data types */
typedef struct builtin_posix_module_t {
  hio_module_t base;
  mode_t access_mode;
} builtin_posix_module_t;

typedef struct builtin_posix_module_dataset_t {
  /** base type */
  struct hio_dataset base;

  /** open backing files */
  hio_file_t files[HIO_POSIX_MAX_OPEN_FILES];

  /** base path of this manifest */
  char *base_path;

  /** start offset of reserved file region. for peformance this
   * should be a multiple of the underlying filesystem's stripe
   * size */
  uint64_t reserved_offset;

  /** space left in reserved file region */
  uint64_t reserved_remaining;

  /** stripe this rank should write to */
  int my_stripe;

  /** use bzip2 to compress data manifests */
  bool                ds_use_bzip;

  /** dataset file mode */
  builtin_posix_dataset_fmode_t ds_fmode;

  /** block size to use for optimized and strided file modes */
  uint64_t            ds_bs;

  /** number of files to use with strided mode */
  int                 ds_fcount;

  /** trace file */
  FILE               *ds_trace_fh;

  /** exclusively write to a single stripe in optimized mode */
  bool                ds_stripe_exclusivity;
} builtin_posix_module_dataset_t;

extern hio_component_t builtin_posix_component;

int builtin_posix_module_dataset_list_internal (struct hio_module_t *module, const char *name,
                                                hio_dataset_header_t **headers, int *count);

#endif /* BUILTIN_POSIX_COMPONENT_H */
